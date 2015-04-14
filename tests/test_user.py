from __future__ import absolute_import
import ujson
from server.models.group import Group

from server.models.user import User
from tests import client, api_post, api_delete, make_friends, api_get


def test_update_user():
    with client() as c:
        user = User.find(key='test')

        resp = api_post(c, user, '/api/users/me', {
            'key': '123',
            'bio': '321'
        })

        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        user = User.find(key='test')
        assert user.bio == '321'
        assert user.key == 'test', 'key should not change, blacklisted'


def test_update_user_group():
    with client() as c:
        user = User.find(key='test')
        g = user.group

        user.group_id = Group.find(code='san_diego').id
        user.save()

        assert User.find(key='test').group.name == 'San Diego'

        # posting with geo should change the users group
        resp = api_post(c, user, '/api/users/me', {
            'bio': '321'
        }, lat=42.3584, lon=-71.0598)

        assert User.find(key='test').group.name == 'Boston'

def test_friends():
    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')

        resp = api_post(c, user1, '/api/users/me/friends', {
            'friend_id': user2.id
        })

        assert resp.status_code == 200, 'oops {}'.format(resp.data)

        assert 0 == User.select().user(user2).friends().count()
        assert 1 == User.select().user(user2).friend_requests().count()

        assert 0 == User.select().user(user1).friends().count()
        assert 0 == User.select().user(user1).friend_requests().count()

        resp = api_post(c, user2, '/api/users/me/friends', {
            'friend_id': user1.id
        })
        assert resp.status_code == 200, 'oops {}'.format(resp.data)

        assert 1 == User.select().user(user2).friends().count()
        assert 0 == User.select().user(user2).friend_requests().count()

        assert 1 == User.select().user(user1).friends().count()
        assert 0 == User.select().user(user1).friend_requests().count()

        # remove friendship
        resp = api_delete(c, user2, '/api/users/me/friends/{}'.format(user1.id))
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        assert 0 == User.select().user(user1).friends().count()
        assert 0 == User.select().user(user2).friends().count()


def test_messages():
    with client() as c:
        user1 = User.find(key='test')
        user2 = User.find(key='test2')

        make_friends(c, user1, user2)

        resp = api_post(c, user1, '/api/messages', {'to_user_id': user2.id, 'message': 't1'})
        assert resp.status_code == 200, 'oops {}'.format(resp.data)

        resp = api_get(c, user1, '/api/conversations')
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        data = ujson.loads(resp.data)
        assert len(data['objects']) == 1
        assert data['objects'][0]['message'] == 't1'

        # post another message
        resp = api_post(c, user1, '/api/messages', {'to_user_id': user2.id, 'message': 't2'})
        assert resp.status_code == 200, 'oops {}'.format(resp.data)

        resp = api_get(c, user1, '/api/conversations')
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        data = ujson.loads(resp.data)
        assert len(data['objects']) == 1
        assert data['objects'][0]['message'] == 't2'

        resp = api_get(c, user1, '/api/conversations/{}'.format(user2.id))
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        data = ujson.loads(resp.data)
        assert len(data['objects']) == 2
        assert data['objects'][0]['message'] == 't2'
        assert data['objects'][1]['message'] == 't1'

        resp = api_get(c, user2, '/api/conversations')
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        data = ujson.loads(resp.data)
        assert len(data['objects']) == 1
        assert data['objects'][0]['message'] == 't2'

        resp = api_get(c, user2, '/api/conversations/{}'.format(user1.id))
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        data = ujson.loads(resp.data)
        assert len(data['objects']) == 2
        assert data['objects'][0]['message'] == 't2'
        assert data['objects'][1]['message'] == 't1'

        resp = api_delete(c, user1, '/api/messages/{}'.format(data['objects'][0]['id']))
        assert resp.status_code == 501, 'oops {}'.format(resp.data)

        # delete the conversation with user2
        resp = api_delete(c, user1, '/api/conversations/{}'.format(user2.id))

        # user 1 shouldn't see it anymore
        resp = api_get(c, user1, '/api/conversations')
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        data = ujson.loads(resp.data)
        assert len(data['objects']) == 0

        resp = api_get(c, user1, '/api/conversations/{}'.format(user2.id))
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        data = ujson.loads(resp.data)
        assert len(data['objects']) == 0

        # but user 2 still should
        resp = api_get(c, user2, '/api/conversations')
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        data = ujson.loads(resp.data)
        assert len(data['objects']) == 1

        resp = api_get(c, user2, '/api/conversations/{}'.format(user1.id))
        assert resp.status_code == 200, 'oops {}'.format(resp.data)
        data = ujson.loads(resp.data)
        assert len(data['objects']) == 2
        assert data['objects'][0]['message'] == 't2'
        assert data['objects'][1]['message'] == 't1'
