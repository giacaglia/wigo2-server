from __future__ import absolute_import

import ujson
from tests import client, api_post


def test_register():
    with client() as c:
        resp = api_post(c, None, '/api/register', None)
        assert 400 == resp.status_code

        resp = api_post(c, None, '/api/register', {
            'timezone': 'US/Eastern',
            'facebook_id': 'xxx123',
            'facebook_access_token': '123'
        })

        assert 200 == resp.status_code, 'oops {}'.format(resp.data)
        resp = ujson.loads(resp.data)
        assert resp['objects'][0]['id'] > 0

        # try again, should fail now with a dupe
        resp = api_post(c, None, '/api/register', {
            'timezone': 'US/Eastern',
            'facebook_id': 'xxx123',
            'facebook_access_token': '123'
        })

        assert 400 == resp.status_code, 'oops {}'.format(resp.data)

        # try to login as this user now
        resp = api_post(c, None, '/api/login', {
            'facebook_id': 'xxx123',
            'facebook_access_token': '123'
        })

        assert 200 == resp.status_code, 'oops {}'.format(resp.data)
        resp = ujson.loads(resp.data)
        assert resp['objects'][0]['facebook_id'] == 'xxx123'
