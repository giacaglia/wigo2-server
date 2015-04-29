from __future__ import absolute_import

import re
import logging
import redis_lock
import requests

from datetime import datetime, timedelta
from geodis.city import City
from time import time
from pytz import timezone, UTC
from repoze.lru import CacheMaker
from schematics.types import StringType, BooleanType, FloatType

from server.db import redis
from server.models import WigoPersistentModel, DoesNotExist, IntegrityException, skey

logger = logging.getLogger('wigo.model')
cache_maker = CacheMaker(maxsize=1000, timeout=60)


class Group(WigoPersistentModel):
    indexes = (
        ('group:{city_id}:city_id', True),
        ('group:{code}:code', True),
        ('group', False),
        ('group:locked:{locked}', False),
        ('group:verified:{verified}', False),
    )

    code = StringType()
    name = StringType(required=True)

    city_id = StringType()
    country_id = StringType()
    state_id = StringType()
    continent_id = StringType()

    country = StringType()
    continent = StringType()

    timezone = StringType(default='US/Eastern', required=True)
    locked = BooleanType(default=True, required=True)
    verified = BooleanType(default=False, required=True)

    latitude = FloatType()
    longitude = FloatType()

    def get_day_start(self, current=None):
        tz = timezone(self.timezone)
        if not current:
            current = datetime.now(tz)
        elif not current.tzinfo:
            current = current.replace(tzinfo=UTC).astimezone(tz)

        current = current.replace(minute=0, second=0, microsecond=0)

        # if it is < 6am, the date will be 0am, and the expires will be 6am the SAME day
        # if it is > 6am, the date will be 6am, and the expires will be 6am the NEXT day
        if current.hour < 6:
            return current.replace(hour=0).astimezone(UTC).replace(tzinfo=None)
        else:
            return current.replace(hour=6).astimezone(UTC).replace(tzinfo=None)

    def get_day_end(self, current=None):
        tz = timezone(self.timezone)
        if not current:
            current = datetime.now(tz)
        elif not current.tzinfo:
            current = current.replace(tzinfo=UTC).astimezone(tz)

        current = current.replace(minute=0, second=0, microsecond=0)

        # if it is < 6am, the date will be 0am, and the expires will be 6am the SAME day
        # if it is > 6am, the date will be 6am, and the expires will be 6am the NEXT day
        if current.hour < 6:
            return current.replace(hour=6).astimezone(UTC).replace(tzinfo=None)
        else:
            next_day = current + timedelta(days=1)
            return next_day.replace(hour=6).astimezone(UTC).replace(tzinfo=None)

    @classmethod
    def find(cls, *args, **kwargs):
        if 'lat' in kwargs and 'lon' in kwargs:
            from server.db import redis

            city = City.getByLatLon(kwargs['lat'], kwargs['lon'], redis)
            if not city:
                raise DoesNotExist()

            try:
                return get_group_by_city_id(city.cityId)
            except DoesNotExist:
                return cls.create_from_city(city)

        return super(Group, cls).find(*args, **kwargs)

    @classmethod
    def create_from_city(cls, city):
        tz = get_timezone(city.lat, city.lon)
        city_code = city.name.decode('unicode_escape').encode('ascii', 'ignore').lower()
        city_code = re.sub(r'[^\w]+', '_', city_code)

        for i in range(1, 10):
            lock = redis_lock.Lock(redis, 'group_create:{}'.format(city.cityId), 60)
            if lock.acquire(blocking=False):
                try:
                    # look for the city one more time with the lock
                    return get_group_by_city_id(city.cityId)
                except DoesNotExist:
                    # create a new group with the lock acquired
                    try:
                        return Group({
                            'name': city.name,
                            'code': city_code,
                            'latitude': city.lat,
                            'longitude': city.lon,
                            'city_id': city.cityId,
                            'timezone': tz or 'US/Eastern',
                            'verified': True
                        }).save()

                    except IntegrityException:
                        city_code = '{}_{}'.format(city_code, i)
                finally:
                    lock.release()

        raise DoesNotExist()

    def __repr__(self):
        return self.name


@cache_maker.expiring_lrucache(maxsize=5000, timeout=60 * 60)
def get_group_by_city_id(city_id):
    return Group.find(city_id=city_id)


@cache_maker.expiring_lrucache(maxsize=1000, timeout=60)
def get_close_groups_with_events(group):
    from server.db import wigo_db

    # fetch the groups close to this group that have events
    group_ids = wigo_db.sorted_set_range(skey(group, 'close_groups_with_events'))
    groups = Group.find(group_ids)

    # re-sort by distance
    groups.sort(key=lambda other: City.getLatLonDistance(
        (group.latitude, group.longitude),
        (other.latitude, other.longitude),
    ))

    return groups


@cache_maker.expiring_lrucache(maxsize=1000, timeout=60 * 60)
def get_close_groups(lat, lon, radius=50):
    close_groups = []
    cities = get_close_cities(lat, lon, radius)
    for city in cities:
        try:
            close_groups.append(get_group_by_city_id(city.cityId))
        except DoesNotExist:
            pass

    return close_groups


@cache_maker.expiring_lrucache(maxsize=1000, timeout=60 * 60)
def get_close_cities(lat, lon, radius=50):
    # get all the groups in the radius
    cities = City.loadByNamedKey('geoname', redis, lat, lon, radius, '')

    # sort by distance from this group
    if cities:
        cities.sort(lambda x, y: cmp(
            City.getLatLonDistance((lat, lon), (x.lat, x.lon)),
            City.getLatLonDistance((lat, lon), (y.lat, y.lon)),
        ))

    return cities


@cache_maker.expiring_lrucache(maxsize=1000, timeout=60 * 60)
def get_biggest_close_cities(lat, lon):
    return City.getByRadius(lat, lon, 100, redis)


@cache_maker.lrucache(maxsize=100)
def get_timezone(lat, lon):
    resp = requests.get('https://maps.googleapis.com/maps/api/timezone/json?'
                        'location={},{}&timestamp={}&sensor=false&'
                        'key=AIzaSyD5qSwGfZiRLIVkf3Ij7if3FVFGDcZdGi0'.format(lat, lon, int(time())))

    if resp.status_code == 200:
        timezone_id = resp.json().get('timeZoneId')
        try:
            timezone(timezone_id)
            return timezone_id
        except:
            logger.warn('could not parse timezone {}'.format(timezone_id))

    return None
