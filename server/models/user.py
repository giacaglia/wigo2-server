from __future__ import absolute_import
from contextlib import contextmanager

import logging

from uuid import uuid4
from time import time
from datetime import timedelta, datetime
from redis.exceptions import LockError

from schematics.transforms import blacklist
from schematics.types import StringType, BooleanType, DateTimeType, EmailType, LongType, FloatType, DateType
from schematics.types.compound import ListType
from schematics.types.serializable import serializable
from config import Configuration
from server.models import WigoPersistentModel, JsonType, WigoModel, skey, user_attendees_key, \
    user_privacy_change, field_memoize, DoesNotExist, cache_maker
from utils import epoch, ValidationException, prefix_score, memoize

logger = logging.getLogger('wigo.model')


class User(WigoPersistentModel):
    indexes = (
        ('user:{facebook_id}:facebook_id', True, False),
        ('user:{email}:email', False, False),
        ('user:{username}:username', True, False),
        ('user:{key}:key', True, False),
        ('user', False, False),
        ('group:{group_id}:users', False, False),
    )

    class Options:
        roles = {
            'www': blacklist('facebook_token', 'role', 'exchange_token',
                             'email_validated', 'email_validated_date',
                             'email_validated_status', 'enterprise', 'location_locked',
                             'password', 'latitude', 'longitude', 'timezone'),
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

    email = StringType()
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

    def is_waiting(self):
        return self.status == 'waiting'

    def get_platforms(self):
        platforms = self.get_custom_property('platforms')
        if not platforms:
            platforms = ['iphone']
        return platforms

    def is_ios_push_enabled(self):
        platforms = self.get_platforms()
        return 'iphone' in platforms or 'ipad' in platforms

    def is_android_push_enabled(self):
        platforms = self.get_platforms()
        return 'android' in platforms

    def get_attending_id(self, event=None):
        date = event.date if event else self.group.get_day_start()
        date = date.date().isoformat()
        event_id = self.db.get(skey(self, 'current_attending_{}'.format(date)))
        return int(event_id) if event_id else None

    def set_attending(self, event):
        date = event.date.date().isoformat()
        self.db.set(skey(self, 'current_attending_{}'.format(date)), event.id, event.expires, event.expires)

    def remove_attending(self, event=None):
        date = event.date if event else self.group.get_day_start()
        date = date.date().isoformat()
        self.db.delete(skey(self, 'current_attending_{}'.format(date)))

    def is_attending(self, event):
        return self.db.sorted_set_is_member(user_attendees_key(self, event), self.id)

    def is_friend(self, friend):
        friend_id = friend.id if isinstance(friend, User) else friend
        return self.db.sorted_set_is_member(skey(self, 'friends'), friend_id)

    def are_friends(self, users):
        from server.db import wigo_db

        mapping = {}
        p = wigo_db.redis.pipeline()
        for user in users:
            p.zscore(skey(self, 'friends'), user.id)
        results = p.execute()
        for index, user in enumerate(users):
            mapping[user] = results[index] is not None
        return mapping

    def friends_iter(self):
        from server.db import wigo_db

        cursor = '0'
        while cursor != 0:
            cursor, data = wigo_db.get_redis().zscan(skey(self, 'friends'), cursor=cursor, count=15)
            friend_ids = [int(friend_id) for friend_id, score in data]
            friends = User.find(friend_ids)
            for friend in friends:
                if friend:
                    yield friend

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

    def get_num_friends_in_common(self, with_user_id):
        return len(self.get_friend_ids_in_common(with_user_id))

    def get_friend_ids_in_common(self, with_user_id):
        from server.db import wigo_db

        friend_ids = set(self.get_friend_ids())
        with_friend_ids = set(wigo_db.sorted_set_range(skey('user', with_user_id, 'friends'), 0, -1))
        return friend_ids & with_friend_ids

    def get_friend_ids(self):
        from server.db import wigo_db

        return wigo_db.sorted_set_rrange(skey(self, 'friends'), 0, -1)

    def get_friend_request_ids(self):
        from server.db import wigo_db

        return wigo_db.sorted_set_rrange(skey(self, 'friend_requests'), 0, -1)

    def get_friend_requested_ids(self):
        from server.db import wigo_db

        return wigo_db.sorted_set_rrange(skey(self, 'friend_requested'), 0, -1)

    @memoize
    def get_private_friend_ids(self):
        from server.db import wigo_db

        return wigo_db.set_members(skey(self, 'friends', 'private'))

    def get_tapped_ids(self):
        from server.db import wigo_db

        return wigo_db.sorted_set_range_by_score(skey(self, 'tapped'),
                                                 epoch(self.group.get_day_start()), 'inf', limit=5000)

    @memoize
    def get_blocked_ids(self):
        from server.db import wigo_db

        return wigo_db.sorted_set_range(skey(self, 'blocked'), 0, -1)

    def save(self):
        is_new = self.is_new
        privacy_changed = self.is_changed(User.privacy.name)
        saved = super(User, self).save()
        if not is_new and privacy_changed:
            user_privacy_change.send(self, instance=self)
        return saved


class Friend(WigoModel):
    user_id = LongType(required=True)
    friend_id = LongType(required=True)
    accepted = BooleanType(required=True, default=False)

    @property
    @field_memoize('friend_id')
    def friend(self):
        try:
            return User.find(self.friend_id)
        except DoesNotExist:
            logger.warn('user {} not found'.format(self.friend_id))
        return None

    def validate(self, partial=False, strict=False):
        super(Friend, self).validate(partial, strict)

        if self.user_id == self.friend_id:
            raise ValidationException('Cannot friend yourself')

        if Configuration.ENVIRONMENT != 'dev' and self.user.is_friend(self.friend):
            raise ValidationException('Already friends')

        if self.friend.is_blocked(self.user):
            raise ValidationException('Blocked')

        if not self.accepted and self.friend.is_friend_request_sent(self.user_id):
            self.accepted = True

    def index(self):
        super(Friend, self).index()

        with self.db.transaction(commit_on_select=False):
            if self.accepted:
                def setup(u1, u2):
                    self.db.sorted_set_add(skey(u1, 'friends'), u2.id, epoch(self.created))
                    self.db.sorted_set_add(skey(u1, 'friends', 'top'), u2.id, 10000)
                    self.db.sorted_set_add(skey(u1, 'friends', 'alpha'), u2.id,
                                           prefix_score(u2.full_name.lower()), replicate=False)
                    if u2.privacy == 'private':
                        self.db.set_add(skey(u1, 'friends', 'private'), u2.id, replicate=False)

                setup(self.user, self.friend)
                setup(self.friend, self.user)

                for type in ('friend_requests', 'friend_requested'):
                    self.db.sorted_set_remove(skey('user', self.user_id, type), self.friend_id)
                    self.db.sorted_set_remove(skey('user', self.friend_id, type), self.user_id)

            else:
                def teardown(u1, u2):
                    self.db.sorted_set_remove(skey(u1, 'friends'), u2.id)
                    self.db.sorted_set_remove(skey(u1, 'friends', 'top'), u2.id)
                    self.db.sorted_set_remove(skey(u1, 'friends', 'alpha'), u2.id, replicate=False)
                    self.db.set_remove(skey(u1, 'friends', 'private'), u2.id, replicate=False)

                teardown(self.user, self.friend)
                teardown(self.friend, self.user)

                f_reqed_key = skey('user', self.user_id, 'friend_requested')
                self.db.sorted_set_add(f_reqed_key, self.friend_id, epoch(self.created))

                f_req_key = skey('user', self.friend_id, 'friend_requests')
                self.db.sorted_set_add(f_req_key, self.user_id, epoch(self.created))

                f_req_in_common_key = skey('user', self.friend_id, 'friend_requests', 'common')
                f_in_common = self.user.get_num_friends_in_common(self.friend_id)
                self.db.sorted_set_add(f_req_in_common_key, self.user_id, f_in_common)

                # clean out old friend requests
                self.db.clean_old(f_reqed_key, timedelta(days=30))
                self.db.clean_old(f_req_key, timedelta(days=30))
                self.db.clean_old(f_req_in_common_key, timedelta(days=30))

    def remove_index(self):
        super(Friend, self).remove_index()

        with self.db.transaction(commit_on_select=False):
            def cleanup(u1, u2):
                self.db.sorted_set_remove(skey(u1, 'friends'), u2.id)
                self.db.sorted_set_remove(skey(u1, 'friends', 'top'), u2.id)
                self.db.sorted_set_remove(skey(u1, 'friends', 'alpha'), u2.id, replicate=False)
                self.db.set_remove(skey(u1, 'friends', 'private'), u2.id, replicate=False)

            cleanup(self.user, self.friend)
            cleanup(self.friend, self.user)

            # clean it out of the current users friend_requests and friend_requested but
            # leave the request on the other side of the relationship so it still seems to be pending
            self.db.sorted_set_remove(skey('user', self.user_id, 'friend_requested'), self.friend_id)
            self.db.sorted_set_remove(skey('user', self.user_id, 'friend_requests'), self.friend_id)
            self.db.sorted_set_remove(skey('user', self.user_id, 'friend_requests', 'common'), self.friend_id)


class Tap(WigoModel):
    TTL = timedelta(days=2)

    indexes = (
        ('user:{user_id}:tapped={tapped_id}', False, True),
    )

    user_id = LongType(required=True)
    tapped_id = LongType(required=True)

    def ttl(self):
        return self.tapped.group.get_day_end(self.created) - datetime.utcnow()

    @property
    @field_memoize('tapped_id')
    def tapped(self):
        return User.find(self.tapped_id)

    def validate(self, partial=False, strict=False):
        super(Tap, self).validate(partial, strict)

        if not self.user.is_friend(self.tapped_id):
            raise ValidationException('Not friends')

        if Configuration.ENVIRONMENT != 'dev' and self.user.is_tapped(self.tapped_id):
            raise ValidationException('Already tapped')


class Block(WigoModel):
    indexes = (
        ('user:{user_id}:blocked={blocked_id}', False, False),
    )

    user_id = LongType(required=True)
    blocked_id = LongType(required=True)
    type = StringType()

    @property
    @field_memoize('tapped_id')
    def blocked(self):
        return User.find(self.blocked_id)

    def validate(self, partial=False, strict=False):
        super(Block, self).validate(partial, strict)
        if self.user.is_blocked(self.blocked):
            raise ValidationException('Already blocked')

    def save(self):
        super(Block, self).save()

        Friend({
            'user_id': self.user_id,
            'blocked_id': self.blocked_id
        }).delete()

        if self.type:
            self.db.sorted_set_incr_score(skey('blocked', self.type), self.blocked_id)

        return self


class Invite(WigoModel):
    TTL = timedelta(days=2)

    indexes = (
        ('event:{event_id}:invited={invited_id}', False, True),
        ('event:{event_id}:user:{user_id}:invited={invited_id}', False, True),
    )

    event_id = LongType(required=True)
    user_id = LongType(required=True)
    invited_id = LongType(required=True)

    def ttl(self):
        if self.event and self.event.expires:
            return (self.event.expires + timedelta(days=2)) - datetime.utcnow()
        else:
            return super(Invite, self).ttl()

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

    def delete(self):
        pass


class Notification(WigoModel):
    TTL = timedelta(days=15)

    id = LongType()
    user_id = LongType(required=True)
    type = StringType(required=True)
    from_user_id = LongType()
    navigate = StringType()
    message = StringType(required=True)
    badge = StringType()

    properties = JsonType()

    @property
    @field_memoize('from_user_id')
    def from_user(self):
        try:
            return User.find(self.from_user_id)
        except DoesNotExist:
            logger.warn('user {} not found'.format(self.from_user_id))
        return None

    @serializable(serialized_name='from_user', serialize_when_none=False)
    def from_user_ref(self):
        return self.ref_field(User, 'from_user_id')

    def index(self):
        super(Notification, self).index()
        key = skey('user', self.user_id, 'notifs')
        primitive = self.to_primitive()
        self.db.sorted_set_add(key, primitive, epoch(self.created), dt=dict, replicate=False)
        self.db.clean_old(key, self.TTL)


class Message(WigoPersistentModel):
    indexes = (
        ('user:{user_id}:messages', False, False),
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
        try:
            return User.find(self.to_user_id)
        except DoesNotExist:
            logger.warn('user {} not found'.format(self.to_user_id))
        return None

    @serializable(serialized_name='to_user', serialize_when_none=False)
    def to_user_ref(self):
        return self.ref_field(User, 'to_user_id')

    def validate(self, partial=False, strict=False):
        super(Message, self).validate(partial, strict)
        if not self.user.is_friend(self.to_user_id):
            raise ValidationException('Not friends')

    def index(self):
        super(Message, self).index()
        with self.db.transaction(commit_on_select=False):
            self.db.set(skey(self.user, 'conversation', self.to_user.id, 'last_message'), self.id)
            self.db.set(skey(self.to_user, 'conversation', self.user.id, 'last_message'), self.id)

    @classmethod
    def delete_conversation(cls, user, to_user):
        from server.db import wigo_db

        with wigo_db.transaction(commit_on_select=False):
            wigo_db.sorted_set_remove(skey(user, 'conversations'), to_user.id)
            wigo_db.delete(skey(user, 'conversation', to_user.id))
            user.track_meta('last_message_change')
            to_user.track_meta('last_message_change')


@cache_maker.expiring_lrucache(maxsize=10000, timeout=60 * 60)
def get_user_id_for_key(key):
    from server.db import wigo_db

    model_ids = wigo_db.sorted_set_range(skey('user', key, 'key'))
    if model_ids:
        return model_ids[0]
    else:
        raise DoesNotExist()


@contextmanager
def user_lock(user_id, timeout=30, yield_on_blocking_timeout=True):
    if Configuration.ENVIRONMENT != 'test':
        from server.db import redis

        lock = redis.lock('locks:user:{}'.format(user_id), timeout=timeout)
        if lock.acquire(blocking=True, blocking_timeout=5):
            try:
                yield
            finally:
                try:
                    lock.release()
                except LockError:
                    pass
        elif yield_on_blocking_timeout:
            yield
    else:
        yield


