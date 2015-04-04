from __future__ import absolute_import
from geodis.city import City
from schematics.types import StringType, BooleanType, FloatType
from server.models import WigoPersistentModel, skey, DoesNotExist


class Group(WigoPersistentModel):
    unique_indexes = ('city_id', 'code', 'name')
    indexes = (
        ('group:locked', 'locked'),
        ('group:verified', 'verified'),
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

    @classmethod
    def find(cls, *args, **kwargs):
        if 'lat' in kwargs and 'lon' in kwargs:
            from server.db import redis
            city = City.getByLatLon(kwargs['lat'], kwargs['lon'], redis)
            if city:
                return Group.find(city_id=city.cityId)
            raise DoesNotExist()

        return super(Group, cls).find(*args, **kwargs)


