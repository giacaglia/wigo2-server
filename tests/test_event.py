from __future__ import absolute_import

import ujson
from server.models import skey, user_votes_key
from server.models.event import EventMessage
from tests import client, api_post, api_get, api_delete, make_friends, create_event, create_event_message, \
    create_event_message_vote


def test_create_event():
    from server.models.event import Event
    from server.models.user import User

    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')

        create_event(c, user1, 'test event')

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
        new_event_id = create_event(c, user1, 'test event')
        assert new_event_id == event_id, 'posting with same name should return same event'
        assert 1 == Event.select().count()
        count, page, results = Event.select().group(user1.group).execute()
        assert 1 == count

        # post again with a different name
        event_id = create_event(c, user1, 'test event 2')
        user1 = User.find(key='test')
        assert user1.get_attending_id() == event_id
        assert 2 == Event.select().count(), '2 events total now'
        count, page, results = Event.select().group(user1.group).execute()
        assert 1 == count, 'only 1 in the group though, since the other has no attendees now'
        assert results[0].name == 'test event 2'


def test_private_event():
    from server.models.event import Event, EventAttendee
    from server.models.user import User

    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')
        make_friends(c, user1, user2)

        event_id = create_event(c, user1, 'test event 1', privacy='private')

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
        count, page, results = EventAttendee.select().event(event).user(user2).execute()
        assert count == 1
        assert results[0] == user1


def test_user_events():
    from server.models.event import Event
    from server.models.user import User

    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')

        create_event(c, user1, 'test event 1')
        create_event(c, user2, 'test event 2')

        assert 2 == Event.select().count(), '2 events total now'
        assert 2 == Event.select().group(user1.group).count()

        count, page, results = Event.select().user(user1).execute()
        assert 1 == count
        assert results[0].name == 'test event 1'

        count, page, results = Event.select().user(user2).execute()
        assert 1 == count
        assert results[0].name == 'test event 2'


def test_attending():
    from server.models.event import Event, EventAttendee
    from server.models.user import User

    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')

        create_event(c, user1, 'test event 1')
        event = Event.find(name='test event 1', group=user1.group)

        resp = api_post(c, user2, '/api/events/{}/attendees'.format(event.id), {})
        assert resp.status_code == 200, 'oops {}'.format(resp.data)

        assert 2 == EventAttendee.select().event(event).count()
        assert 1 == EventAttendee.select().event(event).user(user1).count()
        assert 1 == EventAttendee.select().event(event).user(user2).count()

        make_friends(c, user1, user2)

        assert 2 == EventAttendee.select().event(event).user(user1).count()


def test_events_with_friends():
    from server.models.event import Event
    from server.models.user import User

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


def test_event_messages():
    from server.models.event import Event
    from server.models.user import User

    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')
        event_id = create_event(c, user1, 'e1')
        event = Event.find(event_id)

        create_event_message(c, user1, event, 'test.jpg')
        assert 1 == EventMessage.select().event(event).count()
        assert 1 == EventMessage.select().event(event).user(user1).count()
        assert 0 == EventMessage.select().event(event).user(user2).count()

        make_friends(c, user1, user2)

        assert 1 == EventMessage.select().event(event).user(user2).count()


def test_event_message_votes():
    from server.models.event import Event
    from server.models.user import User
    from server.db import wigo_db

    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')
        user3 = User.find(key='test3')

        make_friends(c, user2, user3)

        event_id = create_event(c, user1, 'e1')
        event = Event.find(event_id)

        message_id_1 = create_event_message(c, user1, event, 'test.jpg')
        message_id_2 = create_event_message(c, user1, event, 'test.jpg')

        message_1 = EventMessage.find(message_id_1)
        message_2 = EventMessage.find(message_id_2)

        resp = create_event_message_vote(c, user1, event, message_1)
        resp = create_event_message_vote(c, user2, event, message_2)
        resp = create_event_message_vote(c, user3, event, message_2)

        assert wigo_db.get_sorted_set_size(skey(message_1, 'votes')) == 1
        assert wigo_db.get_sorted_set_size(skey(message_2, 'votes')) == 2

        assert wigo_db.get_set_size(user_votes_key(user1, message_1)) == 1
        assert wigo_db.get_set_size(user_votes_key(user2, message_1)) == 0
        assert wigo_db.get_set_size(user_votes_key(user1, message_2)) == 0
        assert wigo_db.get_set_size(user_votes_key(user2, message_2)) == 2

        assert EventMessage.select().event(event).by_votes().get() == message_2
        assert EventMessage.select().event(event).user(user1).by_votes().get() == message_1
        assert EventMessage.select().event(event).user(user2).by_votes().get() == message_2

        resp = api_get(c, user1, '/api/events/{}/messages/meta'.format(event_id))
        data = ujson.loads(resp.data)
        assert 1 == data[str(message_id_1)]['num_votes']

        make_friends(c, user1, user2)

        assert wigo_db.get_set_size(user_votes_key(user1, message_1)) == 1
        assert wigo_db.get_set_size(user_votes_key(user2, message_1)) == 1

        assert wigo_db.get_set_size(user_votes_key(user1, message_2)) == 1
        assert wigo_db.get_set_size(user_votes_key(user2, message_2)) == 2

        assert EventMessage.select().event(event).user(user1).by_votes().get() == message_2
        assert EventMessage.select().event(event).user(user2).by_votes().get() == message_2

        make_friends(c, user1, user3)

        resp = create_event_message_vote(c, user2, event, message_1)
        resp = create_event_message_vote(c, user3, event, message_1)

        assert EventMessage.select().event(event).by_votes().get() == message_1
        assert EventMessage.select().event(event).user(user1).by_votes().get() == message_1
