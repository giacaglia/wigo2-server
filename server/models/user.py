from __future__ import absolute_import

from uuid import uuid4
from datetime import datetime, timedelta

from flask.ext.security.core import _security
from flask.ext.security.utils import md5
from schematics.transforms import blacklist
from schematics.types import StringType, BooleanType, DateTimeType, EmailType, LongType, FloatType
from server.models import WigoPersistentModel, JsonType, WigoModel, skey, user_attendees_key
from utils import epoch, ValidationException, memoize


class Role(object):
    def __init__(self, name):
        super(Role, self).__init__()
        self.name = name


class User(WigoPersistentModel):
    unique_indexes = ('facebook_id', 'email', 'username', 'key')
    indexes = (('group', 'group_id'),)

    class Options:
        roles = {'www': blacklist('facebook_token', 'key', 'status', 'role',
                                  'exchange_token', 'email_validated_status', 'password')}
        serialize_when_none = False

    group_id = LongType()

    username = StringType(required=True)
    password = StringType()
    timezone = StringType(required=True, default='US/Eastern')

    first_name = StringType()
    last_name = StringType()
    bio = StringType()
    birthdate = DateTimeType()
    gender = StringType()
    phone = StringType()
    enterprise = BooleanType(default=False, required=True)

    email = EmailType()
    email_validated = BooleanType(default=False, required=True)
    email_validated_date = DateTimeType()
    email_validated_status = StringType()

    key = StringType(required=True, default=lambda: uuid4().hex)
    role = StringType(required=True, default='user')
    status = StringType(required=True, default='active')
    privacy = StringType(choices=('public', 'private'), required=True, default='public')

    facebook_id = StringType(required=True)
    facebook_token = StringType(required=True)
    facebook_token_expires = DateTimeType(required=True)
    exchange_token = StringType()
    exchange_token_expires = DateTimeType()

    latitude = FloatType()
    longitude = FloatType()

    properties = JsonType(default=lambda: {})

    @property
    def roles(self):
        return [Role(self.role)]

    @property
    def images(self):
        return self.get_custom_property('images', [])

    @property
    def full_name(self):
        """ Combines first and last name. """
        if self.first_name and self.last_name:
            return '%s %s' % (self.first_name, self.last_name)
        elif self.first_name:
            return self.first_name
        else:
            return self.username

    def get_auth_token(self):
        data = [str(self.id), md5(self.password)]
        return _security.remember_token_serializer.dumps(data)

    def is_anonymous(self):
        return False

    def is_authenticated(self):
        return True

    def get_id(self):
        return self.id

    def is_active(self):
        return self.status == 'active'

    @property
    def attending(self):
        event_id = self.db.get(skey(self, 'current_attending'))
        return int(event_id) if event_id else None

    @attending.setter
    def attending(self, event):
        self.db.set(skey(self, 'current_attending'), event.id, event.expires - datetime.utcnow())

    def is_attending(self, event):
        return self.db.sorted_set_is_member(user_attendees_key(self, event), self.id) is not None

    def is_friend(self, friend):
        return self.db.sorted_set_is_member(skey(self, 'friends'), friend.id)

    def is_friend_requested(self, friend):
        return self.db.sorted_set_is_member(skey(friend, 'friend_requests'), self.id)

    def is_invited(self, event):
        if event.privacy == 'public':
            return True
        if self.is_attending(event):
            return True
        return self.db.set_is_member(skey(event, 'invited'), self.id)

    def save(self):
        super(User, self).save()

        from server.tasks.images import needs_images_saved, save_images

        if needs_images_saved(self):
            save_images.delay(user_id=self.id)


class Friend(WigoModel):
    user_id = LongType(required=True)
    friend_id = LongType(required=True)
    accepted = BooleanType(required=True, default=False)

    @property
    @memoize('friend_id')
    def friend(self):
        return User.find(self.friend_id)

    def save(self):
        if self.friend.is_friend_requested(self.user):
            self.accepted = True

        return super(Friend, self).save()

    def index(self):
        super(Friend, self).index()

        if self.accepted:
            self.db.sorted_set_add(skey('user', self.user_id, 'friends'), self.friend_id, self.friend_id)
            self.db.sorted_set_add(skey('user', self.friend_id, 'friends'), self.user_id, self.user_id)
            self.db.sorted_set_remove(skey('user', self.user_id, 'friend_requests'), self.friend_id)
            self.db.sorted_set_remove(skey('user', self.friend_id, 'friend_requests'), self.user_id)
        else:
            self.db.sorted_set_remove(skey('user', self.user_id, 'friends'), self.friend_id)
            self.db.sorted_set_remove(skey('user', self.friend_id, 'friends'), self.user_id)
            self.db.sorted_set_add(skey('user', self.friend_id, 'friend_requests'), self.user_id, epoch(self.created))

    def remove_index(self):
        super(Friend, self).remove_index()
        self.db.sorted_set_remove(skey('user', self.user_id, 'friends'), self.friend_id)
        self.db.sorted_set_remove(skey('user', self.friend_id, 'friends'), self.user_id)
        self.db.sorted_set_remove(skey('user', self.user_id, 'friend_requests'), self.friend_id)
        self.db.sorted_set_remove(skey('user', self.friend_id, 'friend_requests'), self.user_id)


class Tap(WigoModel):
    user_id = LongType(required=True)
    tapped_id = LongType(required=True)

    def ttl(self):
        return timedelta(days=8)

    @property
    @memoize('tapped_id')
    def tapped(self):
        return User.find(self.tapped_id)

    def save(self):
        if not self.user.is_friend(self.tapped):
            raise ValidationException('Not friends')
        return super(Tap, self).save()

    def index(self):
        super(Tap, self).index()
        self.db.sorted_set_add(skey('user', self.user_id, 'tapped'), self.tapped_id, epoch(self.expires))
        self.clean_old(skey('user', self.user_id, 'tapped'))

    def remove_index(self):
        super(Tap, self).remove_index()
        self.db.sorted_set_remove(skey('user', self.user_id, 'tapped'), self.tapped_id)


class Invite(WigoModel):
    event_id = LongType(required=True)
    user_id = LongType(required=True)
    invited_id = LongType(required=True)

    @property
    @memoize('invited_id')
    def invited(self):
        return User.find(self.invited_id)

    def save(self):
        inviter = self.user
        invited = self.invited
        event = self.event

        if not inviter.is_friend(invited):
            raise ValidationException('Not friend')

        if not inviter.is_attending(event):
            raise ValidationException('Must be attending the event')

        return super(Invite, self).save()

    def index(self):
        super(Invite, self).index()

        event = self.event
        inviter = self.user
        invited = self.invited

        invited_key = skey(event, 'invited')
        self.db.set_add(invited_key, invited.id)
        self.db.expire(invited_key, timedelta(days=8))

        # make sure i am seeing all my friends attending now
        attendees_key = user_attendees_key(invited, event)
        for friend_id in self.db.sorted_set_iter(skey(invited, 'friends')):
            friend = User.find(friend_id)
            if friend.is_attending(event):
                self.db.sorted_set_add(attendees_key, friend.id, 1)

        self.db.expire(attendees_key, timedelta(days=8))

        # mark this as an event the user can see
        self.add_to_user_events(invited, event)

    def delete(self):
        raise NotImplementedError()


class Notification(WigoModel):
    user_id = LongType(required=True)
    type = StringType(required=True)
    from_user_id = LongType()
    properties = JsonType()

    def ttl(self):
        return timedelta(days=8)

    @property
    @memoize('from_user_id')
    def from_user(self):
        return User.find(self.from_user_id)

    def index(self):
        self.db.sorted_set_add(skey(self.user, 'notifications'), self.to_json(), epoch(self.created))
        self.clean_old(skey(self.user, 'notifications'))

    def delete(self):
        raise NotImplementedError()


class Message(WigoPersistentModel):
    user_id = LongType(required=True)
    to_user_id = LongType(required=True)
    message = LongType(required=True)

    @property
    @memoize('to_user_id')
    def to_user(self):
        return User.find(self.to_user_id)

    def index(self):
        super(Message, self).index()

        self.db.sorted_set_add(skey(self.user, 'conversations'), self.to_user.id, epoch(self.created))
        self.db.sorted_set_add(skey(self.to_user, 'conversations'), self.user.id, epoch(self.created))

        self.db.set(skey(self.user, 'conversation', self.to_user, 'last_message'), self.id)
        self.db.set(skey(self.to_user, 'conversation', self.user, 'last_message'), self.id)

        self.db.sorted_set_add(skey(self.user, 'conversation', self.to_user), self.id, epoch(self.created))
        self.db.sorted_set_add(skey(self.to_user, 'conversation', self.user), self.id, epoch(self.created))

    def remove_index(self):
        super(Message, self).remove_index()
        self.db.sorted_set_remove(skey(self.user, 'conversation', self.to_user), self.id)
        self.db.sorted_set_remove(skey(self.to_user, 'conversation', self.user), self.id)

    @classmethod
    def delete_conversation(cls, user, to_user):
        from server.db import wigo_db

        wigo_db.sorted_set_remove(skey(user, 'conversations'), to_user.id)
        wigo_db.delete(skey(user, 'conversation', to_user))
