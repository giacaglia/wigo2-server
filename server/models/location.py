from __future__ import absolute_import

from geodis.index import GeoboxIndex
from geodis.location import Location


class WigoGeoboxIndex(GeoboxIndex):
    def delete(self, obj, redis):
        p = redis.pipeline()

        for r in self.resolutions:
            cell = self.getGeocell(obj.lat, obj.lon, self.BIT_RESOLUTIONS[r])
            k = self.getKey(r, cell)
            p.zrem(k, obj.getId())

        p.execute()


class WigoLocation(Location):
    @classmethod
    def get_by_radius(cls, lat, lon, radius):
        from server.db import redis

        ids = cls._keys['geoname'].getIds(redis, lat=lat, lon=lon, radius=radius)
        p = redis.pipeline(False)
        [p.hgetall(id[0]) for id in ids]
        rx = p.execute()

        # filter out null records
        nodes = [cls(**d) for d in filter(None, rx)]

        # filter out records that are too far away
        nodes = [n for n in nodes if Location.getLatLonDistance((lat, lon), (n.lat, n.lon)) <= radius]

        # sort the events by distance
        if nodes:
            nodes.sort(lambda x, y: cmp(
                Location.getLatLonDistance((lat, lon), (x.lat, x.lon)),
                Location.getLatLonDistance((lat, lon), (y.lat, y.lon)),
            ))

        return nodes

    def delete(self, redis):
        redis.delete(self.getId())
        redis.zrem(self.getGeohashIndexKey(), self.getId())
        for k in self._keys.values():
            k.delete(self, redis)


class WigoCity(WigoLocation):
    __countryspec__ = ['continent', 'country', 'continent_id', 'country_id']
    __spec__ = WigoLocation.__spec__ + __countryspec__ + ['state', 'state_id', 'city_id', 'population']

    _keys = {
        'geoname': WigoGeoboxIndex('WigoCity', [GeoboxIndex.RES_128KM])
    }

    def __init__(self, **kwargs):
        super(WigoCity, self).__init__(**kwargs)
        self.city_id = kwargs['city_id']

        self.continent = kwargs.get('continent', '').strip()
        self.country = kwargs.get('country', '').strip()
        self.state = kwargs.get('state', '').strip()

        self.continent_id = kwargs.get('continent_id', 0)
        self.country_id = kwargs.get('country_id', 0)
        self.state_id = kwargs.get('state_id', 0)

        self.population = kwargs.get('population', 0)
