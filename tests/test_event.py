from __future__ import absolute_import
import ujson
from server.models.event import Event
from server.models.group import Group

from server.models.user import User
from tests import client, api_post, api_get


def test_create_event():
    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')

        resp = api_post(c, user1, '/api/events/', {
            'name': 'test event'
        })

        assert resp.status_code == 200, 'oops {}'.format(resp.data)

        resp = api_get(c, user1, '/api/events/')
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        data = ujson.loads(resp.data)
        assert data['objects'][0]['name'] == 'test event'
        assert data['objects'][0]['group'] == {'$ref': 'Group:1'}

        event_id = data['objects'][0]['id']
        event = Event.find(event_id)
        assert user1.is_attending(event), 'user is attending the new event'
        assert 1 == Event.select().count()

        # post again with the same name
        resp = api_post(c, user1, '/api/events/', {
            'name': 'test event'
        })

        data = ujson.loads(resp.data)
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        assert data['objects'][0]['id'] == event_id, 'posting with same name should return same event'
        assert 1 == Event.select().count()
        count, results = Event.select().group(user1.group).execute()
        assert 1 == count

        # post again with a different name
        resp = api_post(c, user1, '/api/events/', {
            'name': 'test event 2'
        })

        data = ujson.loads(resp.data)
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        event_id = data['objects'][0]['id']
        user1 = User.find(key='test')
        assert user1.attending == event_id
        assert 2 == Event.select().count(), '2 events total now'
        count, results = Event.select().group(user1.group).execute()
        assert 1 == count, 'only 1 in the group though, since the other has no attendees now'
        assert results[0].name == 'test event 2'


def test_user_events():
    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')

        api_post(c, user1, '/api/events/', {
            'name': 'test event 1'
        })

        api_post(c, user2, '/api/events/', {
            'name': 'test event 2'
        })

        assert 2 == Event.select().count(), '2 events total now'
        count, results = Event.select().group(user1.group).execute()
        assert 2 == count, '2 in the group'

        count, results = Event.select().user(user1).execute()
        assert 1 == count
        assert results[0].name == 'test event 1'

        count, results = Event.select().user(user2).execute()
        assert 1 == count
        assert results[0].name == 'test event 2'


def test_events_with_friends():
    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')

        api_post(c, user1, '/api/user/me/friends', {
            'friend_id': user2.id
        })



        api_post(c, user2, '/api/user/me/friends', {
            'friend_id': user1.id
        })
