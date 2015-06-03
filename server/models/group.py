from __future__ import absolute_import

import re
import logging
import requests

from repoze.lru import CacheMaker
from datetime import datetime, timedelta
from time import time
from pytz import timezone, UTC
from schematics.types import StringType, FloatType, IntType

from server.db import redis
from server.models import WigoPersistentModel, DoesNotExist, IntegrityException
from server.models.location import WigoCity

cache_maker = CacheMaker(maxsize=1000, timeout=60)

logger = logging.getLogger('wigo.model')


class Group(WigoPersistentModel):
    indexes = (
        ('group:{city_id}:city_id', True, False),
        ('group:{code}:code', True, False),
        ('group', False, False),
        ('group:locked:{locked}', False, False),
        ('group:verified:{verified}', False, False),
    )

    code = StringType()
    name = StringType(required=True)

    continent = StringType()
    country = StringType()
    state = StringType()

    continent_id = StringType()
    country_id = StringType()
    state_id = StringType()
    city_id = StringType()

    population = IntType()

    timezone = StringType(default='US/Eastern', required=True)

    latitude = FloatType()
    longitude = FloatType()

    status = StringType(default='initializing')

    def get_day_start(self, current=None):
        tz = timezone(self.timezone)
        if not current:
            current = datetime.now(tz)
        elif not current.tzinfo:
            current = current.replace(tzinfo=UTC).astimezone(tz)

        current = current.replace(minute=0, second=0, microsecond=0)

        if current.hour < 6:
            current = current - timedelta(days=1)

        return current.replace(hour=6).astimezone(UTC).replace(tzinfo=None)

    def get_day_end(self, current=None):
        tz = timezone(self.timezone)
        if not current:
            current = datetime.now(tz)
        elif not current.tzinfo:
            current = current.replace(tzinfo=UTC).astimezone(tz)

        current = current.replace(minute=0, second=0, microsecond=0)

        if current.hour < 6:
            current = current - timedelta(days=1)

        next_day = current + timedelta(days=1)
        return next_day.replace(hour=6).astimezone(UTC).replace(tzinfo=None)

    @classmethod
    def find(cls, *args, **kwargs):
        if 'lat' in kwargs and 'lon' in kwargs:
            from server.db import redis

            close_cities = WigoCity.get_by_radius(kwargs['lat'], kwargs['lon'], 15)
            if close_cities:
                close_cities = close_cities[0:5]
                close_cities.sort(lambda x, y: cmp(y.population, x.population))
                city = close_cities[0]
            else:
                city = WigoCity.getByLatLon(kwargs['lat'], kwargs['lon'], redis)
                if city is None:
                    raise DoesNotExist()

            try:
                return get_group_by_city_id(city.city_id)
            except DoesNotExist:
                return cls.create_from_city(city)

        return super(Group, cls).find(*args, **kwargs)

    @classmethod
    def create_from_city(cls, city):
        tz = get_timezone(city.lat, city.lon)
        city_code = city.name.decode('unicode_escape').encode('ascii', 'ignore').lower()
        city_code = re.sub(r'[^\w]+', '_', city_code)

        for i in range(1, 10):
            lock = redis.lock('locks:group_create:{}'.format(city.city_id), timeout=10)
            if lock.acquire(blocking=True, blocking_timeout=.1):
                try:
                    # look for the city one more time with the lock
                    return get_group_by_city_id(city.city_id)
                except DoesNotExist:
                    # create a new group with the lock acquired
                    try:
                        return Group({
                            'name': city.name,
                            'code': city_code,
                            'latitude': city.lat,
                            'longitude': city.lon,
                            'city_id': city.city_id,
                            'timezone': tz or 'US/Eastern',
                            'population': int(city.population),
                            'continent': city.continent,
                            'country': city.country,
                            'state': city.state,
                            'continent_id': city.continent_id,
                            'country_id': city.country_id,
                            'state_id': city.state_id
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


@cache_maker.expiring_lrucache(maxsize=1000, timeout=60 * 60)
def get_close_groups(lat, lon, radius=50):
    close_groups = []
    cities = get_close_cities(lat, lon, radius)
    for city in cities:
        try:
            close_groups.append(get_group_by_city_id(city.city_id))
        except DoesNotExist:
            pass

    return close_groups


@cache_maker.expiring_lrucache(maxsize=10, timeout=60 * 60)
def get_all_groups():
    return list(Group.select())


@cache_maker.expiring_lrucache(maxsize=1000, timeout=60 * 60)
def get_close_cities(lat, lon, radius=50):
    # get all the groups in the radius
    return WigoCity.get_by_radius(lat, lon, radius)


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
