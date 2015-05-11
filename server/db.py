from __future__ import absolute_import

import logging
import cPickle
import ujson
import msgpack
import shortuuid

from contextlib import contextmanager
from random import randint
from rq_scheduler import Scheduler

from peewee import DoesNotExist, SQL
from time import time
from datetime import datetime, timedelta
from urlparse import urlparse
from redis import Redis
from config import Configuration
from server.rdbms import DataStrings, DataExpires, DataSets, DataSortedSets, DataIntSortedSets, DataIntSets
from redis_shard.shard import RedisShardAPI
from utils import epoch

logger = logging.getLogger('wigo.db')
EXPIRE_KEY = 'expire'

class WigoDB(object):
    def gen_id(self):
        raise NotImplementedError()

    def get_code(self, code):
        json = self.get('code:{}'.format(code))
        if json:
            return ujson.loads(json)
        return None

    def get_new_code(self, data, ttl=2592000):
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

    def set(self, key, value, expires=None, long_term_expires=None):
        raise NotImplementedError()

    def set_if_missing(self, key, value):
        raise NotImplementedError()

    def get(self, key):
        raise NotImplementedError()

    def mget(self, keys):
        raise NotImplementedError()

    def delete(self, key):
        raise NotImplementedError()

    def expire(self, key, expires, long_term_expires=None):
        raise NotImplementedError()

    def set_add(self, key, value, dt=None):
        raise NotImplementedError()

    def set_is_member(self, key, value, dt=None):
        raise NotImplementedError()

    def get_set_size(self, key, dt=None):
        raise NotImplementedError()

    def set_remove(self, key, value, dt=None):
        raise NotImplementedError()

    def set_members(self, key, dt=None):
        raise NotImplementedError()

    def sorted_set_add(self, key, value, score, dt=None):
        raise NotImplementedError()

    def sorted_set_get_score(self, key, value, dt=None):
        raise NotImplementedError()

    def sorted_set_is_member(self, key, value, dt=None):
        raise NotImplementedError()

    def sorted_set_iter(self, key, dt=None):
        raise NotImplementedError()

    def get_sorted_set_size(self, key, min=None, max=None, dt=None):
        raise NotImplementedError()

    def sorted_set_remove(self, key, value, dt=None):
        raise NotImplementedError()

    def sorted_set_remove_by_score(self, key, min, max, dt=None):
        raise NotImplementedError()

    def sorted_set_range(self, key, start=0, end=-1, withscores=False, dt=None):
        raise NotImplementedError()

    def sorted_set_rrange(self, key, start, end, withscores=False, dt=None):
        raise NotImplementedError()

    def sorted_set_range_by_score(self, key, min, max, start=0, limit=10, withscores=False, dt=None):
        raise NotImplementedError()

    def sorted_set_rrange_by_score(self, key, max, min, start=0, limit=10, withscores=False, dt=None):
        raise NotImplementedError()


class WigoRedisDB(WigoDB):
    def __init__(self, redis, queued_db=None):
        super(WigoRedisDB, self).__init__()
        self.redis = redis
        self.queued_db = queued_db

        if self.queued_db:
            logger.info('enabling rdbms redis replication')

        ID_SCRIPT = """
            local epoch = 1288834974657
            local seq = tonumber(redis.call('INCR', 'sequence')) % 4096
            local node = tonumber(redis.call('GET', 'node_id')) % 1024
            local time = redis.call('TIME')
            local time41 = ((tonumber(time[1]) * 1000) + (tonumber(time[2]) / 1000)) - epoch
            return (time41 * (2 ^ 22)) + (node * (2 ^ 12)) + seq
        """

        # if dealing with the shard api, the script needs to be registered on each instance
        if isinstance(redis, RedisShardAPI):
            gen_id_scripts = []
            for name, conn in redis.connections.items():
                name, index = name.split('_')
                conn.setnx('node_id', int(index) + 1)
                gen_id_scripts.append(conn.register_script(ID_SCRIPT))

            def gen_id():
                script = gen_id_scripts[randint(0, len(gen_id_scripts) - 1)]
                return script()

            self.gen_id_script = gen_id
        else:
            self.gen_id_script = self.redis.register_script(ID_SCRIPT)

    def gen_id(self):
        return self.gen_id_script()

    def encode(self, value, dt):
        dt = get_data_type(dt, value)
        if dt == int:
            return int(value)
        else:
            return msgpack.packb(value)

    def decode(self, value, dt):
        if value is None:
            return value

        dt = get_data_type(dt)
        if hasattr(value, '__iter__'):
            return [self.decode(v, dt) for v in value]
        else:
            if dt == int:
                return int(value)
            else:
                return msgpack.unpackb(value)

    def set(self, key, value, expires=None, long_term_expires=None):
        expires = check_expires(expires)

        result = self.redis.set(key, self.encode(value, dict))

        if expires:
            redis = self.redis
            if isinstance(redis, RedisShardAPI):
                redis = redis.get_server(key)

            redis.zadd(EXPIRE_KEY, key, epoch(datetime.utcnow() + expires))

        if self.queued_db and long_term_expires != 0:
            self.queued_db.set(key, value, expires, long_term_expires)

        return result

    def get(self, key):
        value = self.redis.get(key)
        if value:
            value = self.decode(value, dict)
        return value

    def set_if_missing(self, key, value):
        result = self.redis.setnx(key, self.encode(value, dict))
        if self.queued_db:
            self.queued_db.set_if_missing(key, value)
        return result

    def mget(self, keys):
        values = self.redis.mget(keys)
        return self.decode(values, dict)

    def delete(self, key):
        result = self.redis.delete(key)
        if self.queued_db:
            self.queued_db.delete(key)
        return result

    def expire(self, key, expires, long_term_expires=None):
        expires = check_expires(expires)

        redis = self.redis
        if isinstance(redis, RedisShardAPI):
            redis = redis.get_server(key)

        redis.zadd(EXPIRE_KEY, key, epoch(datetime.utcnow() + expires))

        if self.queued_db and long_term_expires:
            self.queued_db.expire(key, expires, long_term_expires)

    def set_add(self, key, value, dt=None, replicate=True):
        result = self.redis.sadd(key, self.encode(value, dt))
        if replicate and self.queued_db:
            self.queued_db.set_add(key, value)
        return result

    def set_is_member(self, key, value, dt=None):
        return self.redis.sismember(key, self.encode(value, dt))

    def set_members(self, key, dt=None):
        return self.decode(self.redis.smembers(key), dt)

    def get_set_size(self, key, dt=None):
        return self.redis.scard(key)

    def set_remove(self, key, value, dt=None, replicate=True):
        result = self.redis.srem(key, self.encode(value, dt))
        if replicate and self.queued_db:
            self.queued_db.set_remove(key, value)
        return result

    def sorted_set_add(self, key, value, score, dt=None, replicate=True):
        result = self.redis.zadd(key, self.encode(value, dt), score)
        if replicate and self.queued_db:
            self.queued_db.sorted_set_add(key, value, score)
        return result

    def sorted_set_get_score(self, key, value, dt=None):
        return self.redis.zscore(key, self.encode(value, dt))

    def sorted_set_is_member(self, key, value, dt=None):
        return self.sorted_set_get_score(key, value, dt) is not None

    def sorted_set_iter(self, key, count=20, dt=None):
        for item, score in self.redis.zscan_iter(key):
            yield self.decode(item, dt), score

    def get_sorted_set_size(self, key, min=None, max=None, dt=None):
        if min is None and max is None:
            return self.redis.zcard(key)
        else:
            return self.redis.zcount(key, min, max)

    def sorted_set_range(self, key, start=0, end=-1, withscores=False, dt=None):
        results = self.redis.zrange(key, start, end, withscores=withscores)
        if withscores:
            return [(self.decode(v, dt), score) for v, score in results]
        else:
            return self.decode(results, dt)

    def sorted_set_rrange(self, key, start, end, withscores=False, dt=None):
        results = self.redis.zrevrange(key, start, end, withscores=withscores)
        if withscores:
            return [(self.decode(v, dt), score) for v, score in results]
        else:
            return self.decode(results, dt)

    def sorted_set_range_by_score(self, key, min, max, start=0, limit=10, withscores=False, dt=None):
        results = self.redis.zrangebyscore(key, min, max, start, limit, withscores=withscores)
        if withscores:
            return [(self.decode(v, dt), score) for v, score in results]
        else:
            return self.decode(results, dt)

    def sorted_set_rrange_by_score(self, key, max, min, start=0, limit=10, withscores=False, dt=None):
        results = self.redis.zrevrangebyscore(key, max, min, start, limit, withscores=withscores)
        if withscores:
            return [(self.decode(v, dt), score) for v, score in results]
        else:
            return self.decode(results, dt)

    def sorted_set_rank(self, key, value, dt=None):
        return self.redis.zrank(key, self.encode(value, dt))

    def sorted_set_rrank(self, key, value, dt=None):
        return self.redis.zrevrank(key, self.encode(value, dt))

    def sorted_set_remove(self, key, value, dt=None, replicate=True):
        result = self.redis.zrem(key, self.encode(value, dt))
        if replicate and self.queued_db:
            self.queued_db.sorted_set_remove(key, value)
        return result

    def sorted_set_remove_by_score(self, key, min, max, dt=None):
        # don't replicate remove by score to long term storage
        return self.redis.zremrangebyscore(key, min, max)

    def sorted_set_incr_score(self, key, value, amount=1):
        return self.redis.zincrby(key, value, amount)

    def sorted_set_remove_by_rank(self, key, start, stop):
        return self.redis.zremrangebyrank(key, start, stop)

    def process_expired(self):
        logger.info('expiring redis keys')

        num_expired = 0
        for redis in self.redis.connections.values():
            with redis.lock('locks:expire_keys'):
                while True:
                    keys = redis.zrangebyscore(EXPIRE_KEY, 0, time(), 0, 100)
                    if keys:
                        for key in keys:
                            redis.delete(key)
                            redis.zrem(EXPIRE_KEY, key)
                            num_expired += 1
                    else:
                        break

        logger.info('expired {} keys'.format(num_expired))


class WigoQueuedDB(WigoDB):
    def __init__(self, redis):
        super(WigoQueuedDB, self).__init__()
        self.redis = redis

    def queue(self, cmd):
        self.redis.lpush('db:queue:commands', cPickle.dumps(cmd))

    def set(self, key, value, expires=None, long_term_expires=None):
        self.queue(('set', key, value, expires, long_term_expires))

    def expire(self, key, expires, long_term_expires=None):
        self.queue(('expire', key, expires, long_term_expires))

    def delete(self, key):
        self.queue(('delete', key))

    def set_add(self, key, value, dt=None):
        self.queue(('set_add', key, value, 'int' if dt == 'int' else None))

    def set_remove(self, key, value, dt=None):
        self.queue(('set_remove', key, value, 'int' if dt == 'int' else None))

    def sorted_set_add(self, key, value, score, dt=None):
        self.queue(('sorted_set_add', key, value, score, 'int' if dt == int else None))

    def sorted_set_remove(self, key, value, dt=None):
        self.queue(('sorted_set_remove', key, value, 'int' if dt == int else None))

    def set_if_missing(self, key, value):
        self.queue(('set_if_missing', key, value))

    def sorted_set_remove_by_score(self, key, min, max, dt=None):
        self.queue(('sorted_set_remove_by_score', key, min, max, 'int' if dt == int else None))


class WigoRdbms(WigoDB):
    def gen_id(self):
        raise NotImplementedError()

    def set(self, key, value, expires=None, long_term_expires=None):
        if not value:
            return

        long_term_expires = check_expires(long_term_expires)
        if long_term_expires:
            long_term_expires = datetime.utcnow() + long_term_expires

        try:
            ds = DataStrings.get(key=key)
            ds.value = value
            ds.modified = datetime.utcnow()
            ds.save()
        except DoesNotExist:
            DataStrings.create(key=key, value=value)

        try:
            ds = DataExpires.get(key=key)
            if not long_term_expires:
                ds.delete_instance()
            else:
                ds.expires = long_term_expires
                ds.save()
        except DoesNotExist:
            if long_term_expires:
                DataExpires.create(key=key, expires=long_term_expires)

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
        DataStrings.delete().where(DataStrings.key == key).execute()
        DataSets.delete().where(DataSets.key == key).execute()
        DataIntSets.delete().where(DataIntSets.key == key).execute()
        DataSortedSets.delete().where(DataSortedSets.key == key).execute()
        DataIntSortedSets.delete().where(DataIntSortedSets.key == key).execute()
        DataExpires.delete().where(DataExpires.key == key).execute()

    def expire(self, key, expires, long_term_expires=None):
        long_term_expires = check_expires(long_term_expires)
        long_term_expires = datetime.utcnow() + long_term_expires

        try:
            existing = DataExpires.get(key=key)
            existing.expires = long_term_expires
            existing.modified = datetime.utcnow()
            existing.save()
        except DoesNotExist:
            DataExpires.create(key=key, expires=long_term_expires, modified=datetime.utcnow())

    def get_set_type(self, dt, value=None):
        dt = get_data_type(dt, value)
        return DataIntSets if dt == int else DataSets

    def set_add(self, key, value, dt=None):
        stype = self.get_set_type(dt, value)
        if not self.set_is_member(key, value, dt):
            stype.create(key=key, value=value)

    def set_is_member(self, key, value, dt=None):
        stype = self.get_set_type(dt, value)
        return stype.select_non_expired().where(
            stype.key == key, stype.value == value
        ).exists()

    def set_members(self, key, dt=None):
        stype = self.get_sorted_set_type(dt)
        return [r[0] for r in stype.select(stype.value).where(stype.key == key).tuples()]

    def get_set_size(self, key, dt=None):
        stype = self.get_set_type(dt)
        return stype.select_non_expired().where(stype.key == key).count()

    def set_remove(self, key, value, dt=None):
        stype = self.get_set_type(dt)
        return stype.delete().where(
            stype.key == key, stype.value == value
        ).execute()

    def get_sorted_set_type(self, dt, value=None):
        dt = get_data_type(dt, value)
        return DataIntSortedSets if dt == int else DataSortedSets

    def sorted_set_add(self, key, value, score, dt=None):
        if score == 'inf':
            score = 10000000.0

        stype = self.get_sorted_set_type(dt, value)
        if not self.sorted_set_is_member(key, value, dt):
            stype.create(key=key, value=value, score=score)
        else:
            stype.update(score=score).where(
                stype.key == key,
                stype.value == value,
                stype.modified == datetime.utcnow()
            ).execute()

    def sorted_set_get_score(self, key, value, dt=None):
        stype = self.get_sorted_set_type(dt, value)
        return stype.select_non_expired(stype.score).where(
            stype.key == key,
            stype.value == value,
        ).scalar()

    def sorted_set_is_member(self, key, value, dt=None):
        stype = self.get_sorted_set_type(dt, value)
        return stype.select_non_expired().where(
            stype.key == key,
            stype.value == value,
        ).exists()

    def sorted_set_iter(self, key, dt=None):
        stype = self.get_sorted_set_type(dt)

        query = stype.select_non_expired(stype.value, stype.score).where(
            stype.key == key
        ).order_by(stype.score.asc()).tuples()

        for row in query:
            yield row[0], row[1]

    def get_sorted_set_size(self, key, min=None, max=None, dt=None):
        stype = self.get_sorted_set_type(dt)
        # todo implement min/max filters
        return stype.select().where(stype.key == key).count()

    def sorted_set_range(self, key, start=0, end=-1, withscores=False, dt=None):
        stype = self.get_sorted_set_type(dt)
        return [r[0] for r in stype.select(stype.value).where(
            stype.key == key,
            stype.score >= start,
            stype.score <= end
        ).order_by(stype.score.asc()).tuples()]

    def sorted_set_rrange(self, key, start, end, withscores=False, dt=None):
        stype = self.get_sorted_set_type(dt)
        return [v[0] for v in stype.select(stype.value).where(
            stype.key == key,
            stype.score >= min,
            stype.score <= max
        ).order_by(stype.score.asc()).offset(start).limit(end - start)]

    def sorted_set_range_by_score(self, key, min, max, start=0, limit=10, withscores=False, dt=None):
        stype = self.get_sorted_set_type(dt)
        min = get_range_val(min)
        max = get_range_val(max)
        return [v[0] for v in stype.select(stype.value).where(
            stype.key == key,
            stype.score >= min,
            stype.score <= max
        ).order_by(stype.score.asc()).offset(start).limit(limit).tuples()]

    def sorted_set_rrange_by_score(self, key, max, min, start=0, limit=10, withscores=False, dt=None):
        stype = self.get_sorted_set_type(dt)
        min = get_range_val(min)
        max = get_range_val(max)
        return [v[0] for v in stype.select(stype.value).where(
            stype.key == key,
            stype.score >= min,
            stype.score <= max
        ).order_by(stype.score.desc()).offset(start).limit(limit).tuples()]

    def sorted_set_remove(self, key, value, dt=None):
        stype = self.get_sorted_set_type(dt, value)
        return stype.delete().where(
            stype.key == key, stype.value == value
        ).execute()

    def sorted_set_remove_by_score(self, key, min, max, dt=None):
        stype = self.get_sorted_set_type(dt)
        min = get_range_val(min)
        max = get_range_val(max)
        stype.delete().where(
            stype.key == key,
            stype.score >= min,
            stype.score <= max
        ).execute()


def get_data_type(dt, value=None):
    if dt is not None:
        if dt == 'int' or isinstance(dt, type):
            return dt
        else:
            raise ValueError('Invalid data type')
    if value is not None:
        return int if isinstance(value, (int, long)) else None
    return int


def check_expires(expires):
    if isinstance(expires, datetime):
        expires = expires - datetime.utcnow()
    if isinstance(expires, timedelta) and expires.total_seconds() < 0:
        expires = timedelta(days=8)
    return expires


def get_range_val(val):
    if isinstance(val, basestring):
        return SQL('-Infinity') if val == '-inf' else 'Infinity'
    return val


@contextmanager
def rate_limit(key, expires):
    if Configuration.ENVIRONMENT in ('dev', 'test'):
        yield False
    else:
        if isinstance(expires, datetime):
            expires = expires - datetime.utcnow()

        if not key.startswith('rate_limit:'):
            key = 'rate_limit:{}'.format(key)
        if redis.exists(key):
            yield True
        else:
            yield False
            redis.setex(key, True, expires)


redis_url = urlparse(Configuration.REDIS_URL)
redis = Redis(host=redis_url.hostname, port=redis_url.port, password=redis_url.password)
scheduler = Scheduler('scheduled', interval=60, connection=redis)

wigo_queued_db = WigoQueuedDB(redis) if Configuration.RDBMS_REPLICATE else None
wigo_rdbms = WigoRdbms()

if Configuration.ENVIRONMENT != 'test':
    servers = []
    for index, redis_url in enumerate(Configuration.REDIS_URLS):
        parsed = urlparse(redis_url)
        servers.append({
            'name': 'redis_{}'.format(index),
            'host': parsed.hostname,
            'port': parsed.port,
            'password': parsed.password
        })

    from redis_shard import shard

    shard.SHARD_METHODS = set(shard.SHARD_METHODS)
    shard.SHARD_METHODS.add('zscan')
    shard.SHARD_METHODS.add('zscan_iter')

    sharded_redis = RedisShardAPI(servers, hash_method='md5')
    wigo_db = WigoRedisDB(sharded_redis, wigo_queued_db)
else:
    wigo_db = WigoRedisDB(redis, wigo_queued_db)
