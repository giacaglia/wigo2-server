from __future__ import absolute_import
from contextlib import contextmanager

import ujson
import msgpack
import shortuuid

from peewee import DoesNotExist, SQL
from datetime import datetime, timedelta
from urlparse import urlparse
from redis import Redis
from config import Configuration
from server.rdbms import DataStrings, DataExpires, DataSets, DataSortedSets


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

    def sorted_set_range_by_score(self, key, min, max, start=0, limit=10):
        raise NotImplementedError()

    def sorted_set_rrange_by_score(self, key, max, min, start=0, limit=10):
        raise NotImplementedError()


class WigoRedisDB(WigoDB):
    def __init__(self, redis, queued_db=None):
        super(WigoRedisDB, self).__init__()
        self.redis = redis
        self.queued_db = queued_db

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
        if expires:
            result = self.redis.setex(key, self.encode(value), expires)
        else:
            result = self.redis.set(key, self.encode(value))

        if self.queued_db:
            self.queued_db.set(key, value, expires)

        return result

    def get(self, key):
        value = self.redis.get(key)
        if value:
            value = self.decode(value)
        return value

    def set_if_missing(self, key, value):
        result = self.redis.setnx(key, self.encode(value))
        if self.queued_db:
            self.queued_db.set_if_missing(key, value)
        return result

    def mget(self, keys):
        values = self.redis.mget(keys)
        return self.decode(values)

    def delete(self, key):
        result = self.redis.delete(key)
        if self.queued_db:
            self.queued_db.delete(key)
        return result

    def expire(self, key, expires):
        result = self.redis.expire(key, expires)
        if self.queued_db:
            self.queued_db.expire(key)
        return result

    def set_add(self, key, value):
        result = self.redis.sadd(key, self.encode(value))
        if self.queued_db:
            self.queued_db.set_add(key, value)
        return result

    def set_is_member(self, key, value):
        return self.redis.sismember(key, self.encode(value))

    def get_set_size(self, key):
        return self.redis.scard(key)

    def set_remove(self, key, value):
        result = self.redis.srem(key, self.encode(value))
        if self.queued_db:
            self.queued_db.set_remove(key, value)
        return result

    def sorted_set_add(self, key, value, score):
        result = self.redis.zadd(key, self.encode(value), score)
        if self.queued_db:
            self.queued_db.sorted_set_add(key, value, score)
        return result

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

    def sorted_set_range_by_score(self, key, min, max, start=0, limit=10):
        return self.decode(self.redis.zrangebyscore(key, min, max, start, limit))

    def sorted_set_rrange_by_score(self, key, max, min, start=0, limit=10):
        return self.decode(self.redis.zrevrangebyscore(key, max, min, start, limit))

    def sorted_set_remove(self, key, value):
        result = self.redis.zrem(key, self.encode(value))
        if self.queued_db:
            self.queued_db.sorted_set_remove(key, value)
        return result

    def sorted_set_remove_by_score(self, key, min, max):
        result = self.redis.zremrangebyscore(key, min, max)
        if self.queued_db:
            self.queued_db.sorted_set_remove_by_score(key, min, max)
        return result


# noinspection PyAbstractClass
class WigoQueuedDB(WigoDB):
    def __init__(self, redis):
        super(WigoQueuedDB, self).__init__()
        self.redis = redis

    def queue(self, cmd):
        self.redis.lpush('db:queue:commands', ujson.dumps(cmd))

    def set(self, key, value, expires=None):
        self.queue(('set', key, value, expires))

    def delete(self, key):
        self.queue(('delete', key))

    def sorted_set_remove(self, key, value):
        self.queue(('sorted_set_remove', key, value))

    def set_add(self, key, value):
        self.queue(('set_add', key, value))

    def set_remove(self, key, value):
        self.queue(('set_remove', key, value))

    def sorted_set_add(self, key, value, score):
        self.queue(('sorted_set_add', key, value, score))

    def expire(self, key, expires):
        self.queue(('expire', key, expires))

    def set_if_missing(self, key, value):
        self.queue(('set_if_missing', key, value))

    def sorted_set_remove_by_score(self, key, min, max):
        self.queue(('sorted_set_remove_by_score', key, min, max))


class WigoRdbms(WigoDB):
    def gen_id(self):
        raise NotImplementedError()

    def set(self, key, value, expires=None):
        if not value:
            return

        if isinstance(expires, timedelta):
            expires = datetime.utcnow() + expires

        try:
            ds = DataStrings.get(key=key)
            ds.value = value
            ds.save()
        except DoesNotExist:
            DataStrings.create(key=key, value=value)

        try:
            ds = DataExpires.get(key=key)
            if not expires:
                ds.delete_instance()
            else:
                ds.expires = expires
                ds.save()
        except DoesNotExist:
            if expires:
                DataExpires.create(key=key, expires=expires)

    def get(self, key):
        row = DataStrings.select_non_expired(DataStrings.value).where(
            DataStrings.key == key
        ).tuples().first()
        return row[0] if row else None

    def set_if_missing(self, key, value):
        ds = self.get(key)
        if not ds:
            self.set(key, value)
            return True
        return False

    def mget(self, keys):
        values = []
        for key in keys:
            values.append(self.get(key))
        return values

    def delete(self, key):
        DataStrings.delete().where(key=key).execute()
        DataSets.delete().where(key=key).execute()
        DataSortedSets.delete().where(key=key).execute()
        DataExpires.delete().where(key=key).execute()

    def expire(self, key, expires):
        DataExpires.update(expires=expires).where(DataExpires.key == key).execute()

    def set_add(self, key, value):
        if not self.set_is_member(key, value):
            DataSets.create(key=key, value=value)

    def set_is_member(self, key, value):
        return DataSets.select_non_expired().where(
            DataSets.key == key, DataSets.value == value
        ).exists()

    def get_set_size(self, key):
        return DataSets.select_non_expired().where(DataSets.key == key).count()

    def set_remove(self, key, value):
        return DataSets.delete().where(
            DataSets.key == key, DataSets.value == value
        ).execute()

    def sorted_set_add(self, key, value, score):
        if not self.sorted_set_is_member(key, value):
            DataSortedSets.create(key=key, value=value, score=score)
        else:
            DataSortedSets.update(score=score).where(
                DataSortedSets.key == key, DataSortedSets.value == value
            ).execute()

    def sorted_set_is_member(self, key, value):
        return DataSortedSets.select_non_expired().where(
            DataSortedSets.key == key,
            DataSortedSets.value == value,
        ).exists()

    def sorted_set_iter(self, key):
        query = DataSortedSets.select_non_expired(DataSortedSets.value).where(
            DataSortedSets.key == key
        ).order_by(DataSortedSets.score.asc()).tuples()

        for row in query:
            yield row[0]

    def get_sorted_set_size(self, key):
        return DataSortedSets.select().where(DataSortedSets.key == key).count()

    def sorted_set_range(self, key, start, end):
        return DataSortedSets.select().where(
            DataSortedSets.key == key,
            DataSortedSets.score >= start,
            DataSortedSets.score <= end
        ).order_by(DataSortedSets.score.asc())

    def sorted_set_rrange(self, key, start, end):
        return [v[0] for v in DataSortedSets.select(DataSortedSets.value).where(
            DataSortedSets.key == key,
            DataSortedSets.score >= min,
            DataSortedSets.score <= max
        ).order_by(DataSortedSets.score.asc()).offset(start).limit(end - start)]

    def sorted_set_range_by_score(self, key, min, max, start=0, limit=10):
        min = get_range_val(min)
        max = get_range_val(max)
        return [v[0] for v in DataSortedSets.select(DataSortedSets.value).where(
            DataSortedSets.key == key,
            DataSortedSets.score >= min,
            DataSortedSets.score <= max
        ).order_by(DataSortedSets.score.asc()).offset(start).limit(limit).tuples()]

    def sorted_set_rrange_by_score(self, key, max, min, start=0, limit=10):
        min = get_range_val(min)
        max = get_range_val(max)
        return [v[0] for v in DataSortedSets.select(DataSortedSets.value).where(
            DataSortedSets.key == key,
            DataSortedSets.score >= min,
            DataSortedSets.score <= max
        ).order_by(DataSortedSets.score.desc()).offset(start).limit(limit).tuples()]

    def sorted_set_remove(self, key, value):
        return DataSortedSets.delete().where(
            DataSortedSets.key == key, DataSortedSets.value == value
        ).execute()

    def sorted_set_remove_by_score(self, key, min, max):
        min = get_range_val(min)
        max = get_range_val(max)
        DataSortedSets.delete().where(
            DataSortedSets.key == key,
            DataSortedSets.score >= min,
            DataSortedSets.score <= max
        ).execute()


def get_range_val(val):
    if isinstance(val, basestring):
        return SQL('-Infinity') if val == '-inf' else 'Infinity'
    return val


@contextmanager
def rate_limit(self, key, expires):
    if Configuration.ENVIRONMENT in ('dev', 'test'):
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
wigo_db = WigoRedisDB(redis, WigoQueuedDB(redis) if Configuration.ENVIRONMENT != 'test' else None)
wigo_rdbms = WigoRdbms()
