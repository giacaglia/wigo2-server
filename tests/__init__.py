from __future__ import absolute_import

import logging
import ujson

from datetime import datetime, timedelta
from contextlib import contextmanager
from mock import patch
from mockredis import mock_redis_client
from config import Configuration
from server.models.user import User


Configuration.ENVIRONMENT = 'test'
Configuration.PUSH_ENABLED = False
Configuration.CELERY_ALWAYS_EAGER = True

NEXT_ID = 1


@contextmanager
def client():
    logging.getLogger().setLevel(level=logging.FATAL)
    logging.getLogger('wigo').setLevel(level=logging.FATAL)

    with patch('redis.Redis', mock_redis_client):
        from web import app
        from server.db import wigo_db
        from server.models.group import Group
        from geodis.city import City

        wigo_db.redis.flushdb()

        def zscan_iter(name, match=None, count=None):
            cursor = '0'
            while cursor != 0:
                cursor, data = wigo_db.redis.zscan(name, cursor=cursor, match=match, count=count)
                for item in data:
                    yield item

        wigo_db.redis.zscan_iter = zscan_iter

        def new_id():
            global NEXT_ID
            next_id = NEXT_ID
            NEXT_ID += 1
            return next_id

        wigo_db.gen_id = new_id

        city = City(cityId=4930956, name='Boston', lat=42.3584, lon=-71.0598)
        city.save(wigo_db.redis)

        city = City(cityId=5391811, name='San Diego', lat=32.7153, lon=-117.157)
        city.save(wigo_db.redis)

        boston = Group({
            'name': 'Boston', 'code': 'boston', 'city_id': 4930956,
            'latitude': 42.3584, 'longitude': -71.0598
        }).save()

        san_diego = Group({
            'name': 'San Diego', 'code': 'san_diego', 'city_id': 5391811,
            'latitude': 32.7153, 'longitude': -117.157
        }).save()

        u = User({
            'username': 'test',
            'group_id': boston.id,
            'facebook_id': 'xxx1',
            'facebook_token': 'xxx1',
            'facebook_token_expires': datetime.utcnow() + timedelta(days=7),
            'email': 'test@test.com',
            'key': 'test'
        }).save()

        u = User({
            'username': 'test2',
            'group_id': boston.id,
            'facebook_id': 'xxx2',
            'facebook_token': 'xxx2',
            'facebook_token_expires': datetime.utcnow() + timedelta(days=7),
            'email': 'test2@test.com',
            'key': 'test2'
        }).save()

        with app.test_client() as client:
            yield client


def get_headers(user, api_version='2.0.0', lat=None, lon=None):
    from config import Configuration

    headers = {'X-Wigo-API-Key': Configuration.API_KEY,
               'X-Wigo-API-Version': api_version,
               'Content-Type': 'application/json'}
    if user:
        headers['X-Wigo-User-Key'] = user.key
    if lat and lon:
        headers['Geolocation'] = 'geo:{},{}'.format(lat, lon)
    return headers


def make_friends(c, user1, user2):
    resp = api_post(c, user1, '/api/users/me/friends', {'friend_id': user2.id})
    assert resp.status_code == 200, 'oops {}'.format(resp.data)
    resp = api_post(c, user2, '/api/users/me/friends', {'friend_id': user1.id})
    assert resp.status_code == 200, 'oops {}'.format(resp.data)


def create_event(c, user, title, privacy='public'):
    return api_post(c, user, '/api/events/', {'name': title, 'privacy': privacy})


def api_get(client, user, url, api_version='2.0.0', lat=None, lon=None):
    headers = get_headers(user, api_version, lat, lon)
    return client.get(url, headers=headers)


def api_post(client, user, url, data, api_version='2.0.0', lat=None, lon=None):
    headers = get_headers(user, api_version, lat, lon)
    return client.post(url, data=ujson.dumps(data), headers=headers)


def api_delete(client, user, url, api_version='2.0.0', lat=None, lon=None):
    headers = get_headers(user, api_version, lat, lon)
    return client.delete(url, headers=headers)
