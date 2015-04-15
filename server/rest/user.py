from __future__ import absolute_import

from flask import request, g
from flask.ext.restful import abort
from flask.ext.restplus import fields

from server.models.user import User, Friend, Tap, Invite, Message
from server.rest import WigoResource, WigoDbResource, WigoDbListResource
from server.security import user_token_required


# noinspection PyUnresolvedReferences
def setup_user_resources(api):
    @api.route('/api/users/<model_id>')
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

        def edit(self, model_id, data):
            if User.username.name in data:
                del data[User.username.name]
            return super(UserResource, self).edit(model_id, data)

        def check_edit(self, user):
            super(UserResource, self).check_edit(user)
            if not user.id == g.user.id:
                abort(403, message='Forbidden')

        @api.response(501, 'Not implemented')
        def delete(self, model_id):
            abort(501, message='Not implemented')


    @api.route('/api/users')
    class UserListResource(WigoDbListResource):
        model = User

        @api.response(200, 'Success', model=User.to_doc_list_model(api))
        def get(self):
            return super(UserListResource, self).get()

        @api.response(501, 'Not implemented')
        def post(self):
            abort(501, message='Not implemented')


    @api.route('/api/users/<user_id>/friends')
    class FriendsListResource(WigoResource):
        model = Friend

        def get_friends_query(self, user_id):
            user = User.find(self.get_id(user_id))
            return self.setup_query(self.select(User).user(user).friends())

        @user_token_required
        @api.response(200, 'Success', model=User.to_doc_list_model(api))
        def get(self, user_id):
            count, friends = self.get_friends_query(user_id).execute()
            return self.serialize_list(User, friends, count)

        @user_token_required
        @api.expect(api.model('NewFriend', {
            'friend_id': fields.Integer(description='User to connect with', required=True)
        }))
        @api.response(200, 'Success')
        def post(self, user_id):
            friend = Friend()
            friend.user_id = g.user.id
            friend.friend_id = self.get_id(request.json.get('friend_id'))
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
            friend.friend_id = self.get_id(request.json.get('friend_id'))
            friend.delete()

            return {'success': True}


    @api.route('/api/users/<user_id>/friends/<int:friend_id>')
    class DeleteFriendResource(WigoResource):
        model = Friend

        @user_token_required
        def delete(self, user_id, friend_id):
            friend = Friend()
            friend.user_id = g.user.id
            friend.friend_id = friend_id
            friend.delete()

            return {'success': True}


    @api.route('/api/users/<user_id>/friend_requests')
    class FriendRequestsListResource(FriendsListResource):
        def get_friends_query(self, user_id):
            user = User.find(self.get_id(user_id))
            return self.setup_query(self.select(User).user(user).friend_requests())


    @api.route('/api/events/<int:event_id>/invites')
    class InviteListResource(WigoResource):
        model = Invite

        @user_token_required
        @api.expect(api.model('NewInvite', {
            'invited_id': fields.Integer(description='User to invite', required=True),
        }))
        @api.response(200, 'Success')
        @api.response(403, 'Not friends or not attending')
        def post(self, event_id):
            invite = Invite()
            invite.user_id = g.user.id
            invite.invited_id = self.get_id(request.json.get('invited_id'))
            invite.event_id = event_id
            invite.save()
            return {'success': True}


    @api.route('/api/users/<user_id>/taps')
    class TapListResource(WigoResource):
        model = Tap

        @user_token_required
        @api.expect(api.model('NewTap', {
            'tapped_id': fields.Integer(description='User to tap', required=True)
        }))
        @api.response(200, 'Success')
        @api.response(403, 'Not friends')
        def post(self, user_id):
            tap = Tap()
            tap.user_id = g.user.id
            tap.tapped_id = self.get_id(request.json.get('tapped_id'))
            tap.save()
            return {'success': True}

    @api.route('/api/users/<user_id>/taps/<int:tapped_id>')
    class DeleteTapResource(WigoResource):
        model = Friend

        @user_token_required
        def delete(self, user_id, friend_id):
            tap = Tap()
            tap.user_id = g.user.id
            tap.tapped_id = self.get_id(request.json.get('tapped_id'))
            tap.delete()

            return {'success': True}


    @api.route('/api/messages/')
    class MessageListResource(WigoDbListResource):
        model = Message

        def get(self):
            raise NotImplementedError()

        @api.expect(Message.to_doc_list_model(api))
        @api.response(200, 'Success', model=Message.to_doc_list_model(api))
        def post(self):
            return super(MessageListResource, self).post()


    @api.route('/api/messages/<int:model_id>')
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


    @api.route('/api/conversations/')
    class ConversationsResource(WigoResource):
        model = Message

        @user_token_required
        @api.response(200, 'Success', model=Message.to_doc_list_model(api))
        def get(self):
            count, instances = self.select().user(g.user).execute()
            return self.serialize_list(self.model, instances, count)

    @api.route('/api/conversations/<int:with_user_id>')
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
