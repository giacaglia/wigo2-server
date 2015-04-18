from __future__ import absolute_import

import re
from datetime import datetime, timedelta
from geodis.city import City
from pytz import timezone, UTC
from schematics.types import StringType, BooleanType, FloatType
from server.models import WigoPersistentModel, DoesNotExist


class Group(WigoPersistentModel):
    indexes = (
        ('group:{city_id}:city_id', True),
        ('group:{code}:code', True),
        ('group:{name}:name', True),
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
                return Group.find(city_id=city.cityId)
            except DoesNotExist:
                return Group({
                    'name': city.name,
                    'code': re.sub(r'([^\s\w]|_)+', '_', city.name.lower()),
                    'latitude': city.lat,
                    'longitude': city.lon,
                    'city_id': city.cityId,
                    'verified': True
                }).save()

        return super(Group, cls).find(*args, **kwargs)
