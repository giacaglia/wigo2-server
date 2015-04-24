from __future__ import absolute_import
from datetime import datetime

import re

from flask import request, g
from flask.ext.restful import abort
from flask.ext.restplus import fields
from server.db import wigo_db
from server.models import skey
from server.models.event import Event

from server.models.user import User, Friend, Tap, Block, Invite, Message, Notification
from server.rdbms import db
from server.rest import WigoResource, WigoDbResource, WigoDbListResource, api
from server.security import user_token_required


# noinspection PyUnresolvedReferences
@api.route('/users/<model_id>')
class UserResource(WigoDbResource):
    model = User

    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    def get(self, model_id):
        return super(UserResource, self).get(model_id)

    @api.expect(User.to_doc_list_model(api))
    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    @api.response(403, 'If trying to edit another user')
    def post(self, model_id):
        return super(UserResource, self).post(model_id)

    def check_edit(self, user):
        super(UserResource, self).check_edit(user)
        if not user.id == g.user.id:
            abort(403, message='Forbidden')

    @api.response(501, 'Not implemented')
    def delete(self, model_id):
        abort(501, message='Not implemented')


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/meta')
class UserMetaResource(WigoResource):
    model = User

    @user_token_required
    @api.response(200, 'Success')
    def get(self, user_id):
        user_id = self.get_id(user_id)

        meta = {}
        if user_id == g.user.id:
            user_meta = wigo_db.redis.hgetall(skey('user', user_id, 'meta'))
            if user_meta:
                meta.update({k: datetime.utcfromtimestamp(float(v)).isoformat() for k, v in user_meta.items()})
        else:
            meta['is_tapped'] = g.user.is_tapped(user_id)
            meta['is_friend'] = g.user.is_friend(user_id)
            meta['is_blocked'] = g.user.is_blocked(user_id)
            if g.user.is_friend_request_sent(user_id):
                meta['friend_request'] = 'sent'
            elif g.user.is_friend_request_received(user_id):
                meta['friend_request'] = 'received'

        meta['num_friends'] = wigo_db.get_sorted_set_size(skey('user', user_id, 'friends'))
        return meta


@api.route('/users')
class UserListResource(WigoResource):
    model = User

    @user_token_required
    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    def get(self):
        text = request.args.get('text')
        if text:
            sql = """
              SELECT value->'id' FROM data_strings WHERE
              CAST(value->>'group_id' AS int8) = %s AND value->>'$type' = 'User'
            """
            params = [g.group.id]

            split = [('{}%%'.format(part)) for part in re.split(r'\s+', text.strip().lower())]
            for s in split:
                sql += "AND ((LOWER(value->>'first_name') LIKE %s) or (LOWER(value->>'last_name') LIKE %s))"
                params.append(s)
                params.append(s)

            with db.execution_context(False) as ctx:
                results = list(db.execute_sql('{} limit 20'.format(sql), params))

            users = User.find([id[0] for id in results])
            return self.serialize_list(self.model, users)

        else:
            count, instances = self.setup_query(self.model.select().group(g.group)).execute()
            return self.serialize_list(self.model, instances, count)


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/friends')
class FriendsListResource(WigoResource):
    model = Friend

    @user_token_required
    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    def get(self, user_id):
        user = User.find(self.get_id(user_id))
        count, friends = self.setup_query(self.select(User).user(user).friends()).execute()
        return self.serialize_list(User, friends, count)

    @user_token_required
    @api.expect(api.model('NewFriend', {
        'friend_id': fields.Integer(description='User to connect with', required=True)
    }))
    @api.response(200, 'Success')
    def post(self, user_id):
        friend = Friend()
        friend.user_id = g.user.id
        friend.friend_id = self.get_id_field('friend_id')
        friend.save()
        return {'success': True}

    @user_token_required
    @api.expect(api.model('DeleteFriend', {
        'friend_id': fields.Integer(description='User to removing connection with', required=True)
    }))
    @api.response(200, 'Success')
    def delete(self, user_id):
        user = g.user

        friend = Friend()
        friend.user_id = g.user.id
        friend.friend_id = self.get_id_field('friend_id')
        friend.delete()

        return {'success': True}


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/friends/requested')
class FriendRequestedListResource(WigoResource):
    @user_token_required
    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    def get(self, user_id):
        user = User.find(self.get_id(user_id))
        count, friends = self.setup_query(self.select(User).user(user).friend_requested()).execute()
        return self.serialize_list(User, friends, count)


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/friends/requests')
class FriendRequestsListResource(WigoResource):
    @user_token_required
    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    def get(self, user_id):
        user = User.find(self.get_id(user_id))
        count, friends = self.setup_query(self.select(User).user(user).friend_requests()).execute()
        return self.serialize_list(User, friends, count)


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/friends/<int:friend_id>')
class DeleteFriendResource(WigoResource):
    model = Friend

    @user_token_required
    @api.response(200, 'Success')
    def delete(self, user_id, friend_id):
        friend = Friend()
        friend.user_id = g.user.id
        friend.friend_id = friend_id
        friend.delete()

        return {'success': True}


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/friends/common/<int:with_user_id>/count')
class FriendsInCommonCountResource(WigoResource):
    model = Friend

    @user_token_required
    def get(self, user_id, with_user_id):
        ids = g.user.get_friend_ids_in_common(with_user_id)
        return {'count': len(ids)}


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/friends/common/<int:with_user_id>')
@api.response(200, 'Success', model=User.to_doc_list_model(api))
class FriendsInCommonResource(WigoResource):
    model = Friend

    @user_token_required
    def get(self, user_id, with_user_id):
        ids = g.user.get_friend_ids_in_common(with_user_id)
        users = User.find(ids)
        return self.serialize_list(User, users)


# noinspection PyUnresolvedReferences
@api.route('/events/<int:event_id>/invites')
class InviteListResource(WigoResource):
    model = Invite

    @user_token_required
    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    def get(self, event_id):
        event = Event.find(event_id)

        page = self.get_page()
        limit = self.get_limit()
        start = (page - 1) * limit

        friends_key = skey(g.user, 'friends')
        num_friends = wigo_db.get_sorted_set_size(friends_key)

        # TODO make sure only to recommend friends in the same city

        # find the users top 5 friends. this is users with > 3 interactions
        top_5 = wigo_db.sorted_set_rrange_by_score(friends_key, 'inf', 3, limit=5)

        friend_ids = wigo_db.sorted_set_range(skey(g.user, 'friends', 'alpha'), start, start + (limit - 1))
        for top_friend_id in top_5:
            if top_friend_id in friend_ids:
                friend_ids.remove(top_friend_id)

        if page == 1 and top_5:
            friend_ids = top_5 + friend_ids

        users = User.find(friend_ids)

        for user in users:
            user.invited = user.is_directly_invited(event)

        return self.serialize_list(self.model, users, num_friends)

    @user_token_required
    @api.expect(api.model('NewInvite', {
        'invited_id': fields.Integer(description='User to invite', required=True),
    }))
    @api.response(200, 'Success')
    @api.response(403, 'Not friends or not attending')
    def post(self, event_id):
        invite = Invite()
        invite.user_id = g.user.id
        invite.invited_id = self.get_id_field('invited_id')
        invite.event_id = event_id
        invite.save()
        return {'success': True}


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/taps')
class TapListResource(WigoResource):
    model = Tap

    @user_token_required
    @api.response(200, 'Success')
    def get(self, user_id):
        return g.user.get_tapped_ids()

    @user_token_required
    @api.expect(api.model('NewTap', {
        'tapped_id': fields.Integer(description='User to tap', required=True)
    }))
    @api.response(200, 'Success')
    @api.response(403, 'Not friends')
    def post(self, user_id):
        tap = Tap()
        tap.user_id = g.user.id
        tap.tapped_id = self.get_id_field('tapped_id')
        tap.save()
        return {'success': True}


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/blocks')
class BlockListResource(WigoResource):
    model = Block

    @user_token_required
    @api.response(200, 'Success')
    def get(self, user_id):
        return g.user.get_blocked_ids()

    @user_token_required
    @api.expect(api.model('NewBlock', {
        'blocked_id': fields.Integer(description='User to block', required=True)
    }))
    @api.response(200, 'Success')
    def post(self, user_id):
        block = Block()
        block.user_id = g.user.id
        block.blocked_id = self.get_id_field('blocked_id')
        block.save()

        return {'success': True}


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/taps/<int:tapped_id>')
class DeleteTapResource(WigoResource):
    model = Friend

    @user_token_required
    @api.response(200, 'Success')
    def delete(self, user_id, tapped_id):
        tap = Tap()
        tap.user_id = g.user.id
        tap.tapped_id = tapped_id
        tap.delete()

        return {'success': True}


@api.route('/messages/')
class MessageListResource(WigoDbListResource):
    model = Message

    def get(self):
        raise NotImplementedError()

    @api.expect(Message.to_doc_list_model(api))
    @api.response(200, 'Success', model=Message.to_doc_list_model(api))
    def post(self):
        return super(MessageListResource, self).post()


# noinspection PyUnresolvedReferences
@api.route('/messages/<int:model_id>')
class MessageResource(WigoDbResource):
    model = Message

    @api.response(200, 'Success', model=Message.to_doc_list_model(api))
    def get(self, model_id):
        return super(MessageResource, self).get(model_id)

    def check_get(self, message):
        super(MessageResource, self).check_get(message)
        if message.user_id != g.user.id:
            abort(403, message='Can only view your own messages')

    @api.expect(Message.to_doc_list_model(api))
    @api.response(200, 'Success', model=Message.to_doc_list_model(api))
    def post(self, model_id):
        return super(MessageResource, self).post(model_id)

    def delete(self, model_id):
        abort(501, message='Not implemented')


@api.route('/conversations/')
class ConversationsResource(WigoResource):
    model = Message

    @user_token_required
    @api.response(200, 'Success', model=Message.to_doc_list_model(api))
    def get(self):
        count, instances = self.select().user(g.user).execute()
        return self.serialize_list(self.model, instances, count)


# noinspection PyUnresolvedReferences
@api.route('/conversations/<int:with_user_id>')
class ConversationWithUserResource(WigoResource):
    model = Message

    @user_token_required
    @api.response(200, 'Success', model=Message.to_doc_list_model(api))
    def get(self, with_user_id):
        with_user = User.find(with_user_id)
        count, instances = self.select().user(g.user).to_user(with_user).execute()
        return self.serialize_list(self.model, instances, count)

    @user_token_required
    @api.response(200, 'Success', model=Message.to_doc_list_model(api))
    def delete(self, with_user_id):
        Message.delete_conversation(g.user, User.find(with_user_id))
        return {'success': True}


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/notifications')
class NotificationsResource(WigoResource):
    model = Notification

    @user_token_required
    @api.response(200, 'Success', model=Message.to_doc_list_model(api))
    def get(self, user_id):
        count, instances = self.select().user(g.user).execute()
        return self.serialize_list(self.model, instances, count)

