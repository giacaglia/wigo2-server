from __future__ import absolute_import
from contextlib import contextmanager

import ujson
import msgpack
import shortuuid

from datetime import datetime
from urlparse import urlparse
from redis import Redis
from config import Configuration


class WigoDB(object):
    def gen_id(self):
        raise NotImplementedError()

    def get_code(self, code):
        json = self.get('code:{}'.format(code))
        if json:
            return ujson.loads(json)
        return None

    def get_new_code(self, data, ttl=864000):
        size = 12
        while size < 50:
            code = shortuuid.ShortUUID().random(length=size)
            key = 'code:{}'.format(code)
            if self.set_if_missing(key, ujson.dumps(data)):
                self.expire(key, ttl)
                return code
            else:
                size += 1

        raise Exception('couldnt find an available random code in redis, up to 50chars?')

    def set(self, key, value, expires=None):
        raise NotImplementedError()

    def set_if_missing(self, key, value):
        raise NotImplementedError()

    def get(self, key):
        raise NotImplementedError()

    def mget(self, keys):
        raise NotImplementedError()

    def delete(self, key):
        raise NotImplementedError()

    def expire(self, key, expires):
        raise NotImplementedError()

    def set_add(self, key, value):
        raise NotImplementedError()

    def set_is_member(self, key, value):
        raise NotImplementedError()

    def get_set_size(self, key):
        raise NotImplementedError()

    def set_remove(self, key, value):
        raise NotImplementedError()

    def sorted_set_add(self, key, value, score):
        raise NotImplementedError()

    def sorted_set_is_member(self, key, value):
        raise NotImplementedError()

    def sorted_set_iter(self, key):
        raise NotImplementedError()

    def get_sorted_set_size(self, key):
        raise NotImplementedError()

    def sorted_set_remove(self, key, value):
        raise NotImplementedError()

    def sorted_set_remove_by_score(self, key, min, max):
        raise NotImplementedError()

    def sorted_set_range(self, key, start, end):
        raise NotImplementedError()

    def sorted_set_rrange(self, key, start, end):
        raise NotImplementedError()

    def sorted_set_range_by_score(self, key, min, max, start, limit):
        raise NotImplementedError()

    def sorted_set_rrange_by_score(self, key, max, min, start, limit):
        raise NotImplementedError()


class WigoRedisDB(WigoDB):
    def __init__(self, redis):
        super(WigoRedisDB, self).__init__()
        self.redis = redis

        self.gen_id_script = self.redis.register_script("""
            local epoch = 1288834974657
            local seq = tonumber(redis.call('INCR', 'sequence')) % 4096
            local node = tonumber(redis.call('GET', 'node_id')) % 1024
            local time = redis.call('TIME')
            local time41 = ((tonumber(time[1]) * 1000) + (tonumber(time[2]) / 1000)) - epoch
            return (time41 * (2 ^ 22)) + (node * (2 ^ 12)) + seq
        """)

    def gen_id(self):
        return self.gen_id_script()

    def encode(self, value):
        return msgpack.packb(value)

    def decode(self, value):
        if value is None:
            return value
        if hasattr(value, '__iter__'):
            return [self.decode(v) for v in value]
        else:
            return msgpack.unpackb(value)

    def set(self, key, value, expires=None):
        value = self.encode(value)
        if expires:
            return self.redis.setex(key, value, expires)
        else:
            return self.redis.set(key, value)

    def get(self, key):
        value = self.redis.get(key)
        if value:
            value = self.decode(value)
        return value

    def set_if_missing(self, key, value):
        return self.redis.setnx(key, self.encode(value))

    def mget(self, keys):
        values = self.redis.mget(keys)
        return self.decode(values)

    def delete(self, key):
        return self.redis.delete(key)

    def setex(self, key, value, expires):
        return self.redis.set(key, self.encode(value), expires)

    def expire(self, key, expires):
        return self.redis.expire(key, expires)

    def set_add(self, key, value):
        return self.redis.sadd(key, self.encode(value))

    def set_is_member(self, key, value):
        return self.redis.sismember(key, self.encode(value))

    def get_set_size(self, key):
        return self.redis.scard(key)

    def set_remove(self, key, value):
        return self.redis.srem(key, self.encode(value))

    def sorted_set_add(self, key, value, score):
        return self.redis.zadd(key, self.encode(value), score)

    def sorted_set_is_member(self, key, value):
        return self.redis.zscore(key, self.encode(value)) is not None

    def sorted_set_iter(self, key):
        for item, score in self.redis.zscan_iter(key):
            yield self.decode(item)

    def get_sorted_set_size(self, key):
        return self.redis.zcard(key)

    def sorted_set_range(self, key, start, end):
        return self.decode(self.redis.zrange(key, start, end))

    def sorted_set_rrange(self, key, start, end):
        return self.decode(self.redis.zrevrange(key, start, end))

    def sorted_set_range_by_score(self, key, min, max, start, limit):
        return self.decode(self.redis.zrangebyscore(key, min, max))

    def sorted_set_rrange_by_score(self, key, max, min, start, limit):
        return self.decode(self.redis.zrevrangebyscore(key, max, min, start, limit))

    def sorted_set_remove(self, key, value):
        return self.redis.zrem(key, self.encode(value))

    def sorted_set_remove_by_score(self, key, min, max):
        return self.redis.zremrangebyscore(key, min, max)


@contextmanager
def rate_limit(self, key, expires):
    if Configuration.ENVIRONMENT == 'dev':
        yield False
    else:
        if not key.startswith('rate_limit:'):
            key = 'rate_limit:%s:%s' % (self.__class__.__name__.lower(), key)
        if redis.exists(key):
            yield True
        else:
            yield False
            redis.setex(key, True, expires - datetime.datetime.utcnow())


redis_url = urlparse(Configuration.REDIS_URL)
redis = Redis(host=redis_url.hostname, port=redis_url.port, password=redis_url.password)
wigo_db = WigoRedisDB(redis)

