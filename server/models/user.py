from __future__ import absolute_import

from uuid import uuid4
from datetime import timedelta

from time import time
from schematics.transforms import blacklist
from schematics.types import StringType, BooleanType, DateTimeType, EmailType, LongType, FloatType
from schematics.types.serializable import serializable
from server.models import WigoPersistentModel, JsonType, WigoModel, skey, user_attendees_key, DEFAULT_EXPIRING_TTL
from utils import epoch, ValidationException, memoize


class Role(object):
    def __init__(self, name):
        super(Role, self).__init__()
        self.name = name


class User(WigoPersistentModel):
    indexes = (
        ('user:{facebook_id}:facebook_id', True),
        ('user:{email}:email', True),
        ('user:{username}:username', True),
        ('user:{key}:key', True),
        ('user', False),
        ('group:{group_id}:users', False),
    )

    class Options:
        roles = {'www': blacklist('facebook_token', 'key', 'status', 'role', 'group_id', 'user_id',
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

    facebook_id = StringType()
    facebook_token = StringType()
    facebook_token_expires = DateTimeType()
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

    def get_id(self):
        return self.id

    def is_active(self):
        return self.status == 'active'

    def get_attending_id(self):
        event_id = self.db.get(skey(self, 'current_attending'))
        return int(event_id) if event_id else None

    def set_attending(self, event):
        self.db.set(skey(self, 'current_attending'), event.id, event.expires, event.expires)

    def is_attending(self, event):
        return self.db.sorted_set_is_member(user_attendees_key(self, event), self.id)

    def is_friend(self, friend):
        return self.db.sorted_set_is_member(skey(self, 'friends'), friend.id)

    def is_tapped(self, tapped):
        score = self.db.sorted_set_get_score(skey(self, 'tapped'), tapped.id)
        return score and score > time()

    def is_blocked(self, user):
        return False

    def is_friend_requested(self, friend):
        return self.db.sorted_set_is_member(skey(friend, 'friend_requests'), self.id)

    def is_invited(self, event):
        if event.privacy == 'public':
            return True
        if self.id == event.owner_id:
            return True
        if self.is_attending(event):
            return True
        return self.db.set_is_member(skey(event, 'invited'), self.id)

    def get_friend_ids_in_common(self, with_user_id):
        from server.db import wigo_db

        friend_ids = set(wigo_db.sorted_set_rrange(skey(self, 'friends'), 0, -1))
        with_friend_ids = set(wigo_db.sorted_set_rrange(skey('user', with_user_id, 'friends'), 0, -1))
        return friend_ids & with_friend_ids

    def get_tapped_ids(self):
        from server.db import wigo_db
        return wigo_db.sorted_set_range_by_score(skey(self, 'tapped'), time(), 'inf')

    def track_friend_interaction(self, user):
        from server.db import wigo_db
        # increment the score for the user in the friends table
        wigo_db.sorted_set_incr_score(skey(self, 'friends'), user.id)


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
        from server.models.event import Event

        if self.accepted:
            self.db.sorted_set_add(skey('user', self.user_id, 'friends'), self.friend_id, 1)
            self.db.sorted_set_add(skey('user', self.friend_id, 'friends'), self.user_id, 1)

            for type in ('friend_requests', 'friend_requested'):
                self.db.sorted_set_remove(skey('user', self.user_id, type), self.friend_id)
                self.db.sorted_set_remove(skey('user', self.friend_id, type), self.user_id)

            user_event_id = self.user.get_attending_id()
            if user_event_id:
                user_event = Event.find(user_event_id)
                if self.friend.is_invited(user_event):
                    user_event.add_to_user_attending(self.friend, self.user)

            friend_event_id = self.friend.get_attending_id()
            if friend_event_id:
                friend_event = Event.find(friend_event_id)
                if self.user.is_invited(friend_event):
                    friend_event.add_to_user_attending(self.user, self.friend)

        else:
            self.db.sorted_set_remove(skey('user', self.user_id, 'friends'), self.friend_id)
            self.db.sorted_set_remove(skey('user', self.friend_id, 'friends'), self.user_id)

            friend_requested_key = skey('user', self.user_id, 'friend_requested')
            self.db.sorted_set_add(friend_requested_key, self.friend_id, epoch(self.created))

            friend_requests_key = skey('user', self.friend_id, 'friend_requests')
            self.db.sorted_set_add(friend_requests_key, self.user_id, epoch(self.created))

            # clean out old friend requests
            self.clean_old(friend_requested_key, timedelta(days=30))
            self.clean_old(friend_requests_key, timedelta(days=30))

    def remove_index(self):
        super(Friend, self).remove_index()
        from server.models.event import Event

        for type in ('friends', 'friend_requests', 'friend_requested'):
            self.db.sorted_set_remove(skey('user', self.user_id, type), self.friend_id)
            self.db.sorted_set_remove(skey('user', self.friend_id, type), self.user_id)

        user_event_id = self.user.get_attending_id()
        if user_event_id:
            user_event = Event.find(user_event_id)
            user_event.remove_from_user_attending(self.friend, self.user)

        friend_event_id = self.friend.get_attending_id()
        if friend_event_id:
            friend_event = Event.find(friend_event_id)
            friend_event.remove_from_user_attending(self.user, self.friend)


class Tap(WigoModel):
    indexes = (
        ('user:{user_id}:tapped={tapped_id}', False),
    )

    user_id = LongType(required=True)
    tapped_id = LongType(required=True)

    def ttl(self):
        return DEFAULT_EXPIRING_TTL

    def get_index_score(self):
        return epoch(self.tapped.group.get_day_end(self.created))

    @property
    @memoize('tapped_id')
    def tapped(self):
        return User.find(self.tapped_id)

    def save(self):
        if not self.user.is_friend(self.tapped):
            raise ValidationException('Not friends')
        if self.user.is_tapped(self.tapped):
            raise ValidationException('Already tapped')

        super(Tap, self).save()

        self.user.track_friend_interaction(self.tapped)

        return self


class Invite(WigoModel):
    indexes = (
        ('event:{event_id}:invited={invited_id}', False),
    )

    event_id = LongType(required=True)
    user_id = LongType(required=True)
    invited_id = LongType(required=True)

    def ttl(self):
        return DEFAULT_EXPIRING_TTL

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

        super(Invite, self).save()

        self.user.track_friend_interaction(self.invited)

        return self

    def index(self):
        super(Invite, self).index()

        event = self.event
        inviter = self.user
        invited = self.invited

        # make sure i am seeing all my friends attending now
        for friend_id, score in self.db.sorted_set_iter(skey(invited, 'friends')):
            friend = User.find(friend_id)
            if friend.is_attending(event):
                event.add_to_user_attending(invited, friend, score)

    def delete(self):
        pass


class Notification(WigoPersistentModel):
    indexes = (
        ('user:{user_id}:notifications', False),
    )

    user_id = LongType(required=True)
    type = StringType(required=True)
    from_user_id = LongType()
    navigate = StringType(required=True)
    message = StringType(required=True)

    properties = JsonType()

    def ttl(self):
        return DEFAULT_EXPIRING_TTL

    @property
    @memoize('from_user_id')
    def from_user(self):
        return User.find(self.from_user_id)

    @serializable(serialized_name='from_user', serialize_when_none=False)
    def user_ref(self):
        return self.ref_field(User, 'from_user_id')


class Message(WigoPersistentModel):
    indexes = (
        ('user:{user_id}:conversations={to_user_id}', False),
        ('user:{user_id}:conversation:{to_user_id}', False),
        ('user:{to_user_id}:conversations={user_id}', False),
        ('user:{to_user_id}:conversation:{user_id}', False),
    )

    user_id = LongType(required=True)
    to_user_id = LongType(required=True)
    message = StringType(required=True)

    @property
    @memoize('to_user_id')
    def to_user(self):
        return User.find(self.to_user_id)

    def save(self):
        if not self.user.is_friend(self.to_user):
            raise ValidationException('Not friends')

        super(Message, self).save()

        self.user.track_friend_interaction(self.to_user)

        return self

    def index(self):
        super(Message, self).index()
        self.db.set(skey(self.user, 'conversation', self.to_user.id, 'last_message'), self.id)
        self.db.set(skey(self.to_user, 'conversation', self.user.id, 'last_message'), self.id)

    @classmethod
    def delete_conversation(cls, user, to_user):
        from server.db import wigo_db

        wigo_db.sorted_set_remove(skey(user, 'conversations'), to_user.id)
        wigo_db.delete(skey(user, 'conversation', to_user.id))
