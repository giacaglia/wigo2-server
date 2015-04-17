from __future__ import absolute_import

import ujson
from server.models.event import Event, EventAttendee
from server.models.group import Group
from server.models.user import User
from tests import client, api_post, api_get, api_delete, make_friends, create_event


def test_create_event():
    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')

        resp = create_event(c, user1, 'test event')

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
        resp = create_event(c, user1, 'test event')
        data = ujson.loads(resp.data)
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        assert data['objects'][0]['id'] == event_id, 'posting with same name should return same event'
        assert 1 == Event.select().count()
        count, results = Event.select().group(user1.group).execute()
        assert 1 == count

        # post again with a different name
        resp = create_event(c, user1, 'test event 2')
        data = ujson.loads(resp.data)
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        event_id = data['objects'][0]['id']
        user1 = User.find(key='test')
        assert user1.get_attending_id() == event_id
        assert 2 == Event.select().count(), '2 events total now'
        count, results = Event.select().group(user1.group).execute()
        assert 1 == count, 'only 1 in the group though, since the other has no attendees now'
        assert results[0].name == 'test event 2'


def test_private_event():
    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')
        make_friends(c, user1, user2)

        resp = create_event(c, user1, 'test event 1', privacy='private')
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        event_id = ujson.loads(resp.data)['objects'][0]['id']

        assert 1 == Event.select().count()
        assert 0 == Event.select().group(user1.group).count()
        assert 1 == Event.select().user(user1).count()
        assert 0 == Event.select().user(user2).count()

        event = Event.find(event_id)

        resp = api_post(c, user1, '/api/events/{}/invites'.format(event_id), {
            'invited_id': user2.id
        })

        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        assert 1 == Event.select().user(user2).count()
        count, results = EventAttendee.select().event(event).user(user2).execute()
        assert count == 1
        assert results[0] == user1


def test_user_events():
    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')

        resp = create_event(c, user1, 'test event 1')
        assert resp.status_code == 200, 'oops {}'.format(resp.data)

        resp = create_event(c, user2, 'test event 2')
        assert resp.status_code == 200, 'oops {}'.format(resp.data)

        assert 2 == Event.select().count(), '2 events total now'
        assert 2 == Event.select().group(user1.group).count()

        count, results = Event.select().user(user1).execute()
        assert 1 == count
        assert results[0].name == 'test event 1'

        count, results = Event.select().user(user2).execute()
        assert 1 == count
        assert results[0].name == 'test event 2'


def test_attending():
    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')

        resp = create_event(c, user1, 'test event 1')
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        event = Event.find(name='test event 1', group=user1.group)

        resp = api_post(c, user2, '/api/events/{}/attendees'.format(event.id), {})
        assert resp.status_code == 200, 'oops {}'.format(resp.data)

        assert 2 == EventAttendee.select().event(event).count()
        assert 1 == EventAttendee.select().event(event).user(user1).count()
        assert 1 == EventAttendee.select().event(event).user(user2).count()

        make_friends(c, user1, user2)

        assert 2 == EventAttendee.select().event(event).user(user1).count()


def test_events_with_friends():
    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')

        create_event(c, user1, 'test event 1')

        assert 1 == Event.select().user(user1).count()
        assert 0 == Event.select().user(user2).count()

        make_friends(c, user1, user2)

        assert 1 == Event.select().user(user1).count()
        assert 1 == Event.select().user(user2).count()

        # remove friends, event should disappear from user2
        resp = api_delete(c, user2, '/api/users/me/friends/{}'.format(user1.id))
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        assert 1 == Event.select().user(user1).count()
        assert 0 == Event.select().user(user2).count()
