from __future__ import absolute_import
from server.models.group import Group

from server.models.user import User
from tests import client, api_post


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
