from __future__ import absolute_import

import re
from datetime import datetime, timedelta
from geodis.city import City
from pytz import timezone, UTC
from repoze.lru import CacheMaker
from schematics.types import StringType, BooleanType, FloatType
from server.db import redis
from server.models import WigoPersistentModel, DoesNotExist, IntegrityException, skey

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
                city_code = city.name
                iterations = 0
                while iterations < 10:
                    try:
                        city_code = city_code.decode('unicode_escape').encode('ascii', 'ignore').lower()
                        city_code = re.sub(r'[^\w]+', '_', city_code)

                        return Group({
                            'name': city.name,
                            'code': city_code,
                            'latitude': city.lat,
                            'longitude': city.lon,
                            'city_id': city.cityId,
                            'verified': True
                        }).save()

                    except IntegrityException:
                        iterations += 1
                        city_code = '{}_{}'.format(city_code, iterations)

                raise DoesNotExist()

        return super(Group, cls).find(*args, **kwargs)

    def __repr__(self):
        return self.name


@cache_maker.expiring_lrucache(maxsize=5000, timeout=60 * 60)
def get_group_by_city_id(city_id):
    return Group.find(city_id=city_id)


@cache_maker.expiring_lrucache(maxsize=1000, timeout=60)
def get_close_groups_with_events(lat, lon, radius=50):
    from server.db import wigo_db

    close_groups = []
    cities = get_close_cities(lat, lon, radius)
    for city in cities:
        try:
            close_group = get_group_by_city_id(city.cityId)
            if wigo_db.sorted_set_is_member(skey('groups_with_events'), close_group.id):
                close_groups.append(close_group)
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