from __future__ import absolute_import

from uuid import uuid4
from datetime import timedelta, datetime

from time import time
from schematics.transforms import blacklist
from schematics.types import StringType, BooleanType, DateTimeType, EmailType, LongType, FloatType, DateType
from schematics.types.compound import ListType
from schematics.types.serializable import serializable
from config import Configuration
from server.models import WigoPersistentModel, JsonType, WigoModel, skey, user_attendees_key, DEFAULT_EXPIRING_TTL, \
    user_privacy_change, field_memoize
from utils import epoch, ValidationException, prefix_score, memoize


class User(WigoPersistentModel):
    indexes = (
        ('user:{facebook_id}:facebook_id', True, False),
        ('user:{email}:email', True, False),
        ('user:{username}:username', True, False),
        ('user:{key}:key', True, False),
        ('user', False, False),
        ('group:{group_id}:users', False, False),
    )

    class Options:
        roles = {
            'www': blacklist('facebook_token', 'role', 'exchange_token', 'email_validated_status', 'password'),
            'www-edit': blacklist('id', 'facebook_token', 'key', 'role',
                                  'group_id', 'user_id', 'exchange_token',
                                  'email_validated_status', 'password')
        }
        serialize_when_none = False

    group_id = LongType()

    username = StringType(required=True)
    password = StringType()
    timezone = StringType(required=True, default='US/Eastern')

    first_name = StringType()
    last_name = StringType()
    bio = StringType()
    birthdate = DateType()
    education = StringType()
    work = StringType()
    gender = StringType()
    phone = StringType()

    enterprise = BooleanType(default=False, required=True)

    email = EmailType()
    email_validated = BooleanType(default=False, required=True)
    email_validated_date = DateTimeType()
    email_validated_status = StringType()

    key = StringType(required=True, default=lambda: uuid4().hex)
    role = StringType(required=True, default='user')
    status = StringType(required=True, default='waiting')
    privacy = StringType(choices=('public', 'private'), required=True, default='public')

    facebook_id = StringType()
    facebook_token = StringType()
    facebook_token_expires = DateTimeType()
    exchange_token = StringType()
    exchange_token_expires = DateTimeType()

    latitude = FloatType()
    longitude = FloatType()

    location_locked = BooleanType(default=False, required=True)

    tags = ListType(StringType)

    properties = JsonType(default=lambda: {})

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
            return self.username or ''

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
        friend_id = friend.id if isinstance(friend, User) else friend
        return self.db.sorted_set_is_member(skey(self, 'friends'), friend_id)

    def is_tapped(self, tapped):
        tapped_id = tapped.id if isinstance(tapped, User) else tapped
        score = self.db.sorted_set_get_score(skey(self, 'tapped'), tapped_id)
        return score is not None and score > time()

    def is_blocked(self, user):
        user_id = user.id if isinstance(user, User) else user
        return self.db.sorted_set_is_member(skey(self, 'blocked'), user_id)

    def is_friend_request_sent(self, friend):
        friend_id = friend.id if isinstance(friend, User) else friend
        return self.db.sorted_set_is_member(skey(self, 'friend_requested'), friend_id)

    def is_friend_request_received(self, friend):
        friend_id = friend.id if isinstance(friend, User) else friend
        return self.db.sorted_set_is_member(skey(self, 'friend_requests'), friend_id)

    def can_see_event(self, event):
        # everyone can see a public event
        if event.privacy == 'public':
            return True
        # if you own the event, your can see it!
        if self.id == event.owner_id:
            return True
        # if you are going already, you can see it
        if self.is_attending(event):
            return True
        # if you were invited you can see it
        return self.is_directly_invited(event)

    def is_directly_invited(self, event):
        return self.db.sorted_set_is_member(skey(event, 'invited'), self.id)

    def get_friend_ids_in_common(self, with_user_id):
        from server.db import wigo_db

        friend_ids = set(wigo_db.sorted_set_rrange(skey(self, 'friends'), 0, -1))
        with_friend_ids = set(wigo_db.sorted_set_rrange(skey('user', with_user_id, 'friends'), 0, -1))
        return friend_ids & with_friend_ids

    def get_friend_ids(self):
        from server.db import wigo_db

        return wigo_db.sorted_set_rrange(skey(self, 'friends'), 0, -1)

    @memoize
    def get_private_friend_ids(self):
        from server.db import wigo_db

        return wigo_db.set_members(skey(self, 'friends', 'private'))

    def get_tapped_ids(self):
        from server.db import wigo_db

        return wigo_db.sorted_set_range_by_score(skey(self, 'tapped'), time(), 'inf', limit=5000)

    @memoize
    def get_blocked_ids(self):
        from server.db import wigo_db

        return wigo_db.sorted_set_range(skey(self, 'blocked'), 0, -1)

    def track_friend_interaction(self, user):
        from server.db import wigo_db
        # increment the score for the user in the friends table
        wigo_db.sorted_set_incr_score(skey(self, 'friends'), user.id)

    def save(self):
        privacy_changed = self.is_changed(User.privacy.name)
        saved = super(User, self).save()
        user_privacy_change.send(self, instance=self)
        return saved


class Friend(WigoModel):
    user_id = LongType(required=True)
    friend_id = LongType(required=True)
    accepted = BooleanType(required=True, default=False)

    @property
    @field_memoize('friend_id')
    def friend(self):
        return User.find(self.friend_id)

    def validate(self, partial=False, strict=False):
        super(Friend, self).validate(partial, strict)

        if Configuration.ENVIRONMENT != 'dev' and self.user.is_friend(self.friend):
            raise ValidationException('Already friends')

        if self.friend.is_blocked(self.user):
            raise ValidationException('Blocked')

        if not self.accepted and self.friend.is_friend_request_sent(self.user_id):
            self.accepted = True

    def index(self):
        super(Friend, self).index()

        if self.accepted:
            self.db.sorted_set_add(skey('user', self.user_id, 'friends'), self.friend_id, 1)
            self.db.sorted_set_add(skey('user', self.friend_id, 'friends'), self.user_id, 1)

            self.db.sorted_set_add(skey('user', self.user_id, 'friends', 'alpha'),
                                   self.friend_id, prefix_score(self.friend.full_name.lower()), replicate=False)

            self.db.sorted_set_add(skey('user', self.friend_id, 'friends', 'alpha'),
                                   self.user_id, prefix_score(self.user.full_name.lower()), replicate=False)

            if self.user.privacy == 'private':
                self.db.set_add(skey('user', self.friend_id, 'friends', 'private'), self.user_id, replicate=False)

            if self.friend.privacy == 'private':
                self.db.set_add(skey('user', self.user_id, 'friends', 'private'), self.friend_id, replicate=False)

            for type in ('friend_requests', 'friend_requested'):
                self.db.sorted_set_remove(skey('user', self.user_id, type), self.friend_id)
                self.db.sorted_set_remove(skey('user', self.friend_id, type), self.user_id)

        else:
            self.db.sorted_set_remove(skey('user', self.user_id, 'friends'), self.friend_id)
            self.db.sorted_set_remove(skey('user', self.friend_id, 'friends'), self.user_id)

            self.db.sorted_set_remove(skey('user', self.user_id, 'friends', 'alpha'), self.friend_id, replicate=False)
            self.db.sorted_set_remove(skey('user', self.friend_id, 'friends', 'alpha'), self.user_id, replicate=False)

            self.db.set_remove(skey('user', self.user_id, 'friends', 'private'), self.friend_id, replicate=False)
            self.db.set_remove(skey('user', self.friend_id, 'friends', 'private'), self.user_id, replicate=False)

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

        self.db.sorted_set_remove(skey('user', self.user_id, 'friends'), self.friend_id)
        self.db.sorted_set_remove(skey('user', self.friend_id, 'friends'), self.user_id)

        self.db.sorted_set_remove(skey('user', self.user_id, 'friends', 'alpha'), self.friend_id, replicate=False)
        self.db.sorted_set_remove(skey('user', self.friend_id, 'friends', 'alpha'), self.user_id, replicate=False)

        self.db.set_remove(skey('user', self.user_id, 'friends', 'private'), self.friend_id, replicate=False)
        self.db.set_remove(skey('user', self.friend_id, 'friends', 'private'), self.user_id, replicate=False)

        # clean it out of the current users friend_requests and friend_requested but
        # leave the request on the other side of the relationship so it still seems to be pending
        self.db.sorted_set_remove(skey('user', self.user_id, 'friend_requests'), self.friend_id)
        self.db.sorted_set_remove(skey('user', self.user_id, 'friend_requested'), self.friend_id)

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
        ('user:{user_id}:tapped={tapped_id}', False, False),
    )

    user_id = LongType(required=True)
    tapped_id = LongType(required=True)

    def ttl(self):
        return DEFAULT_EXPIRING_TTL

    def get_index_score(self):
        return epoch(self.tapped.group.get_day_end(self.created))

    @property
    @field_memoize('tapped_id')
    def tapped(self):
        return User.find(self.tapped_id)

    def validate(self, partial=False, strict=False):
        super(Tap, self).validate(partial, strict)

        # if not self.user.is_friend(self.tapped_id):
        #     raise ValidationException('Not friends')

        if Configuration.ENVIRONMENT != 'dev' and self.user.is_tapped(self.tapped_id):
            raise ValidationException('Already tapped')

    def save(self):
        super(Tap, self).save()
        self.user.track_friend_interaction(self.tapped)
        return self


class Block(WigoModel):
    indexes = (
        ('user:{user_id}:blocked={blocked_id}', False, False),
    )

    user_id = LongType(required=True)
    blocked_id = LongType(required=True)

    @property
    @field_memoize('tapped_id')
    def blocked(self):
        return User.find(self.blocked_id)

    def save(self):
        super(Block, self).save()

        Friend({
            'user_id': self.user_id,
            'blocked_id': self.blocked_id
        }).delete()

        return self


class Invite(WigoModel):
    indexes = (
        ('event:{event_id}:invited={invited_id}', False, True),
    )

    event_id = LongType(required=True)
    user_id = LongType(required=True)
    invited_id = LongType(required=True)

    def ttl(self):
        return DEFAULT_EXPIRING_TTL

    @property
    @field_memoize('invited_id')
    def invited(self):
        return User.find(self.invited_id)

    def validate(self, partial=False, strict=False):
        super(Invite, self).validate(partial, strict)

        inviter = self.user
        invited = self.invited
        event = self.event

        if not inviter.is_friend(invited):
            raise ValidationException('Not friend')

        if not inviter.is_attending(event):
            raise ValidationException('Must be attending the event')

    def save(self):
        super(Invite, self).save()
        self.user.track_friend_interaction(self.invited)
        return self

    def delete(self):
        pass


class Notification(WigoPersistentModel):
    indexes = (
        ('user:{user_id}:notifications', False, False),
    )

    user_id = LongType(required=True)
    type = StringType(required=True)
    from_user_id = LongType()
    navigate = StringType(required=True)
    message = StringType(required=True)
    badge = StringType()

    properties = JsonType()

    def ttl(self):
        return timedelta(days=30)

    @property
    @field_memoize('from_user_id')
    def from_user(self):
        return User.find(self.from_user_id)

    @serializable(serialized_name='from_user', serialize_when_none=False)
    def from_user_ref(self):
        return self.ref_field(User, 'from_user_id')


class Message(WigoPersistentModel):
    indexes = (
        ('user:{user_id}:conversations={to_user_id}', False, False),
        ('user:{user_id}:conversation:{to_user_id}', False, False),
        ('user:{to_user_id}:conversations={user_id}', False, False),
        ('user:{to_user_id}:conversation:{user_id}', False, False),
    )

    user_id = LongType(required=True)
    to_user_id = LongType(required=True)
    message = StringType(required=True)

    @property
    @field_memoize('to_user_id')
    def to_user(self):
        return User.find(self.to_user_id)

    @serializable(serialized_name='to_user', serialize_when_none=False)
    def to_user_ref(self):
        return self.ref_field(User, 'to_user_id')

    def validate(self, partial=False, strict=False):
        super(Message, self).validate(partial, strict)
        if not self.user.is_friend(self.to_user):
            raise ValidationException('Not friends')

    def save(self):
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
        user.track_meta('last_message')
        to_user.track_meta('last_message')

