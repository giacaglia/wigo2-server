from __future__ import absolute_import

import ujson

from itertools import chain
from datetime import datetime, timedelta
from generic.multidispatch import multimethod, has_multimethods
from schematics.models import Model

from server.models import Friend, Tap, Invite, Notification, Message, \
    Event, EventMessage, EventMessageVote, EventAttendee, Dated, User, Group, Config
from utils import epoch


@has_multimethods
class WigoDB(object):
    def __init__(self, storage):
        self.storage = storage

    def gen_id(self):
        raise NotImplementedError()

    def count(self, model_class):
        return self.storage.zcard(self.key(model_class))

    def all(self, model_class, page=0, limit=10):
        start = page * limit
        model_ids = self.storage.zrevrange(self.key(model_class), start, start+limit)
        return self.mget(model_class, model_ids)

    def get(self, model_class, object_id):
        return self.mget(model_class, [object_id])[0]

    def mget(self, model_class, object_ids):
        if object_ids:
            results = self.storage.mget([self.key(model_class, int(object_id)) for object_id in object_ids])
            return [model_class(ujson.loads(result)) if result is not None else None for result in results]
        else:
            return []

    def save(self, obj):
        if hasattr(obj, 'id'):
            self.check_id(obj)
            self.persist(obj)
            self.storage.zadd(self.key(obj.__class__), obj.id, obj.id)
        self.process_save(obj)

    def delete(self, obj):
        if hasattr(obj, 'id'):
            self.remove(obj)
            self.storage.zrem(self.key(obj.__class__), obj.id)
        self.process_delete(obj)

    @multimethod(User)
    def process_save(self, user):
        pass

    @multimethod(User)
    def process_delete(self, user):
        pass

    @process_save.when(Group)
    def process_save(self, loc):
        pass

    @process_delete.when(Group)
    def process_delete(self, loc):
        pass

    @process_save.when(Friend)
    def process_save(self, friend):
        self.storage.zadd(self.key('user', friend.user_id, 'friends'), friend.follow_id, friend.follow_id)
        self.storage.zadd(self.key('user', friend.friend_id, 'friends'), friend.user_id, friend.user_id)

    @process_delete.when(Friend)
    def process_delete(self, friend):
        self.storage.zrem(self.key('user', friend.user_id, 'friends'), friend.follow_id)
        self.storage.zrem(self.key('user', friend.friend_id, 'friends'), friend.user_id)

    @process_save.when(Tap)
    def process_save(self, tap):
        self.storage.zadd(self.key('user', tap.user_id, 'tapped'), tap.tapped_id, epoch(tap.expires))
        self.storage.zadd(self.key('user', tap.tapped_id, 'tapped_by'), tap.user_id, epoch(tap.expires))
        self.clean_old(self.key('user', tap.user_id, 'tapped'))
        self.clean_old(self.key('user', tap.tapped_id, 'tapped_by'))

    @process_delete.when(Tap)
    def process_delete(self, tap):
        self.storage.zrem(self.key('user', tap.user_id, 'tapped'), tap.tapped_id)
        self.storage.zrem(self.key('user', tap.tapped_id, 'tapped_by'), tap.user_id)

    @process_save.when(Invite)
    def process_save(self, invite):
        event = self.get(Event, invite.event_id)
        invited = self.get(User, invite.invited_id)
        inviter = self.get(User, invite.inviter_id)

        invited_key = self.key(event, 'invited')
        self.storage.sadd(invited_key, invited.id)
        self.storage.expires(invited_key, timedelta(days=8))

        # make sure i am seeing all my friends attending now
        attendees_key = self.user_attendees_key(invited, event)
        for following_id in self.storage.zscan_iter(self.key(invited, 'following')):
            following = self.get(User, following_id)
            if self.is_attending(following, event):
                self.storage.zadd(attendees_key, following.id, 1)

        self.storage.expires(attendees_key, timedelta(days=8))

        # mark this as an event the user can see
        self.record_event_for_user(invited, event)

    @process_delete.when(Invite)
    def process_delete(self, invite):
        event = self.get(Event, invite.event_id)
        invited = self.get(User, invite.invited_id)
        inviter = self.get(User, invite.inviter_id)

        if not self.is_attending(invited, event):
            invited_key = self.key(event, 'invited')
            self.storage.srem(invited_key, 'user', invited.id)
            self.storage.srem(self.key(invited, 'events'), event.id)

    @process_save.when(Notification)
    def process_save(self, notification):
        self.storage.zadd(self.key(notification.user, 'notifications'), self.serialize(notification),
                          epoch(notification.created))
        self.clean_old(self.key(notification.user, 'notifications'))

    @process_delete.when(Notification)
    def process_delete(self, notification):
        self.storage.zrem(self.key(notification.user, 'notifications'), self.serialize(notification))
        self.uncache(notification)

    @process_save.when(Message)
    def process_save(self, message):
        self.storage.zadd(self.key(message.user, 'conversations'), message.to_user.id, epoch(message.created))
        self.storage.zadd(self.key(message.to_user, 'conversations'), message.user.id, epoch(message.created))

        serialized = self.serialize(message)
        self.storage.zadd(self.key(message.user, 'conversation', message.to_user), serialized, epoch(message.created))
        self.storage.zadd(self.key(message.to_user, 'conversation', message.user), serialized, epoch(message.created))

    @process_delete.when(Message)
    def process_delete(self, message):
        serialized = self.serialize(message)
        self.storage.zrem(self.key(message.user, 'conversation', message.to_user), serialized)
        self.storage.zrem(self.key(message.to_user, 'conversation', message.user), serialized)

    def delete_conversation(self, user, to_user):
        self.storage.zrem(self.key(user, 'conversations'), to_user.id)
        self.storage.delete(self.key(user, 'conversation', to_user))

    @process_save.when(Event)
    def process_save(self, event):
        self.record_global_event(event)
        self.clean_old(self.key('group', event.group_id, 'events'))

    def record_global_event(self, event):
        if event.privacy == 'public':
            events_key = self.key('group', event.group_id, 'events')
            attendees_key = self.key(event, 'attendees')
            num_attending = self.storage.zcard(attendees_key)
            if num_attending > 0:
                self.storage.zadd(events_key, event.id, get_score_key(event.expires, num_attending))
            else:
                self.storage.zrem(events_key, event.id)

    @process_delete.when(Event)
    def process_delete(self, event):
        self.storage.zrem(self.key('group', event.group_id, 'events'), event.id)
        self.storage.zrem(self.key(event, 'attendees'))

    @process_save.when(EventAttendee)
    def process_save(self, attendee):
        user_id = attendee.user_id
        event_id = attendee.event_id

        user = self.get(User, user_id)
        event = self.get(Event, event_id)
        location = self.get(Group, event.location_id)

        # check if the user is switching events for today
        current_event_id = self.get_current_attending(user)
        if current_event_id and current_event_id != event_id:
            self.delete(EventAttendee({'event_id': event_id, 'user_id': user.id}))

        # first update the global state of the event
        if user.privacy != 'private':
            attendees_key = self.key('group', event.group_id, event, 'attendees')
            self.storage.zadd(attendees_key, user.id, epoch(attendee.created))
            self.storage.expires(attendees_key, timedelta(days=8))
            self.record_global_event(event)

        # now update the users view of the events
        # record the exact event the user is currently attending
        self.set_current_attending(user, event)

        # record the event into the events the user can see, as the most important one
        self.storage.zadd(self.key(user, 'events'), event.id, 'inf')

        # record current user as an attendee
        attendees_key = self.user_attendees_key(user, event)
        self.storage.zadd(attendees_key, user.id, 'inf')
        self.storage.expires(attendees_key, timedelta(days=8))

        for follower_id in self.storage.zscan_iter(self.key(user, 'followers')):
            follower = self.get(User, follower_id)
            if event.privacy == 'public' or self.is_invited(event, follower):
                # add to the users view of who is attending
                attendees_key = self.user_attendees_key(follower, event)
                self.storage.zadd(attendees_key, user.id, epoch(attendee.created))
                self.storage.expires(attendees_key, timedelta(days=8))
                # add to the users current events list
                self.record_event_for_user(follower, event)

    @process_delete.when(EventAttendee)
    def process_delete(self, attendee):
        user = self.get(User, attendee.user_id)
        event = self.get(Event, attendee.event_id)

        # first update the global state of the event
        self.storage.zrem(self.key(event, 'attendees'), user.id)
        self.record_global_event(event)

        # now update the users view of the events
        self.storage.delete(self.key(user, 'current_attending'))
        self.storage.zrem(self.user_attendees_key(user, event), user.id)
        self.record_event_for_user(user, event)

        for follower_id in self.storage.zscan_iter(self.key(user, 'followers')):
            follower = self.get(User, follower_id)
            # add to the users view of who is attending
            self.storage.zrem(self.user_attendees_key(follower, event), user.id)
            # add to the users current events list
            self.record_event_for_user(follower, event)

    @process_save.when(EventMessage)
    def process_save(self, message):
        event = self.get(Event, message.event_id)
        messages_key = self.key(event, 'messages')
        self.storage.zadd(messages_key, message.id, epoch(message.created))
        self.storage.expires(messages_key, timedelta(days=8))

    @process_delete.when(EventMessage)
    def process_delete(self, message):
        event = self.get(Event, message.event_id)
        messages_key = self.key(event, 'messages')
        self.storage.zrem(messages_key, message.id)

    @process_save.when(EventMessageVote)
    def process_save(self, vote):
        message = self.get(EventMessage, vote.message_id)
        votes_key = self.key(message, 'votes')
        self.storage.sadd(votes_key, vote.user_id)
        self.storage.expires(votes_key, timedelta(days=8))

    @process_delete.when(EventMessageVote)
    def process_delete(self, vote):
        message = self.get(EventMessage, vote.message_id)
        self.storage.srem(self.key(message, 'votes'), vote.user_id)

    @process_save.when(Config)
    def process_save(self, config):
        pass

    @process_delete.when(Config)
    def process_delete(self, config):
        pass

    def set_current_attending(self, user, event):
        self.storage.setex(self.key(user, 'current_attending'), event.id)

    def get_current_attending(self, user):
        event_id = self.storage.get(self.key(user, 'current_attending'))
        return int(event_id) if event_id else None

    def record_event_for_user(self, user, event):
        events_key = self.key(user, 'events')
        current_attending = self.get_current_attending(user)
        if current_attending and current_attending == event.id:
            self.storage.zadd(events_key, event.id, 'inf')
        else:
            attendees_key = self.user_attendees_key(user, event)
            num_attending = self.storage.zcard(attendees_key)
            if num_attending > 0:
                self.storage.zadd(events_key, event.id, get_score_key(event.expires, num_attending))
            else:
                self.storage.zrem(events_key, event.id)

    def get_event_ids(self, group, start=None, end=None):
        if start is None:
            start = datetime.utcnow() - timedelta(days=7)
        if end is None:
            end = Dated.get_expires(group.timezone)
        return [int(event_id) for event_id in self.storage.zrangebyscore(
            self.key(group, 'events'), epoch(start), epoch(end) + 1)]

    def get_events(self, group, start=None, end=None):
        event_ids = self.get_event_ids(group, start, end)
        return self.mget(Event, event_ids)

    def get_user_event_ids(self, user, start=None, end=None):
        if start is None:
            start = datetime.utcnow() - timedelta(days=7)
        if end is None:
            end = Dated.get_expires(user.timezone)
        return [int(event_id) for event_id in self.storage.zrangebyscore(
            self.key(user, 'events'), epoch(start), 'inf')]

    def get_user_events(self, user, start=None, end=None):
        event_ids = self.get_user_event_ids(user, start, end)
        return self.mget(Event, event_ids)

    def get_attendee_ids(self, *events):
        attendees_keys = [self.key('group', event.group_id, event, 'attendees') for event in events]
        p = self.storage.pipeline()
        for attendees_key in attendees_keys:
            p.zrange(attendees_key, 0, -1)
        attendee_ids_by_event = p.execute()
        attendees = []
        for attendee_ids in attendee_ids_by_event:
            attendees.append([int(aid) for aid in attendee_ids])
        return attendees

    def get_attendees(self, *events):
        attendee_ids_by_event = self.get_attendee_ids(*events)
        users = self.mget(User, list(chain.from_iterable(attendee_ids_by_event)))
        users_by_id = {user.id: user for user in users}
        attendees = []
        for attendee_ids in attendee_ids_by_event:
            attendees.append([users_by_id.get(aid) for aid in attendee_ids])
        return attendees

    def get_user_attendee_ids(self, user, *events):
        attendees_keys = [self.user_attendees_key(user, event) for event in events]
        p = self.storage.pipeline()
        for attendees_key in attendees_keys:
            p.zrange(attendees_key, 0, -1)
        attendee_ids_by_event = p.execute()
        attendees = []
        for attendee_ids in attendee_ids_by_event:
            attendees.append([int(aid) for aid in attendee_ids])
        return attendees

    def get_user_attendees(self, user, *events):
        attendee_ids_by_event = self.get_user_attendee_ids(user, *events)
        users = self.mget(User, list(chain.from_iterable(attendee_ids_by_event)))
        users_by_id = {user.id: user for user in users}
        attendees = []
        for attendee_ids in attendee_ids_by_event:
            attendees.append([users_by_id.get(aid for aid in attendee_ids)])
        return attendees

    def is_attending(self, user, event):
        return self.storage.zscore(self.user_attendees_key(user, event), user.id)

    def is_invited(self, event, user):
        return self.storage.sismember(self.key(event, 'invited'), user.id)

    def clean_old(self, key):
        up_to = datetime.utcnow() - timedelta(days=8)
        self.storage.zremrangebyscore(key, 0, epoch(up_to))

    def check_id(self, object):
        if object.id is None:
            object.id = self.gen_id()
        return object

    def serialize(self, obj):
        return ujson.dumps(obj.to_primitive())

    def persist(self, obj):
        key = self.key(obj)
        self.storage.set(key, self.serialize(obj))

    def remove(self, obj):
        self.storage.delete(self.key(obj))

    def user_attendees_key(self, user, event):
        return self.key(user, event, 'attendees')

    def key(self, *keys):
        key_list = []
        for key in keys:
            if isinstance(key, Model):
                key = '{}:{}'.format(key.__class__.__name__.lower(), str(key.id))
            elif isinstance(key, type):
                key = key.__name__.lower()
            else:
                key = str(key)
            key_list.append(key)
        return ':'.join(key_list)


class WigoRedisDB(WigoDB):
    def __init__(self, storage):
        super(WigoRedisDB, self).__init__(storage)
        self.gen_id_script = self.storage.register_script("""
            local epoch = 1288834974657
            local seq = tonumber(redis.call('INCR', 'sequence')) % 4096
            local node = tonumber(redis.call('GET', 'node_id')) % 1024
            local time = redis.call('TIME')
            local time41 = ((tonumber(time[1]) * 1000) + (tonumber(time[2]) / 1000)) - epoch
            return (time41 * (2 ^ 22)) + (node * (2 ^ 12)) + seq
        """)

    def gen_id(self):
        return self.gen_id_script()


def get_score_key(d, score):
    return int(epoch(d)) + (score / 10000.0)
