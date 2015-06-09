from __future__ import absolute_import

import logging
from decimal import Decimal
from time import time
from datetime import datetime, timedelta
from geodis.location import Location
from schematics.transforms import blacklist
from schematics.types import LongType, StringType, IntType, DateTimeType
from schematics.types.compound import ListType
from schematics.types.serializable import serializable
from server.models import WigoModel, WigoPersistentModel, skey, DoesNotExist, JsonType
from server.models import AlreadyExistsException, user_attendees_key, user_eventmessages_key, \
    DEFAULT_EXPIRING_TTL, field_memoize, user_votes_key
from server.models import cache_maker
from utils import strip_unicode, strip_punctuation, epoch, ValidationException

EVENT_LEADING_STOP_WORDS = {'a', 'the'}
logger = logging.getLogger('wigo.model')


class Event(WigoPersistentModel):
    # Must be enough days to cover 7 days of history, 7 days of future (padding a bit)
    TTL = DEFAULT_EXPIRING_TTL

    indexes = (
        ('event', False, True),
    )

    group_id = LongType(required=True)
    owner_id = LongType()
    name = StringType(required=True)
    privacy = StringType(choices=('public', 'private'), required=True, default='public')

    date = DateTimeType(required=True)
    expires = DateTimeType(required=True)

    properties = JsonType(default=lambda: {})
    tags = ListType(StringType)

    @serializable
    def is_expired(self):
        return datetime.utcnow() > self.expires

    @property
    def is_global(self):
        return self.tags and 'global' in self.tags

    @property
    def is_verified(self):
        return self.tags and 'verified' in self.tags

    def ttl(self):
        if self.expires:
            # expire the event 10 days after self.expires
            return (self.expires + self.TTL) - datetime.utcnow()
        else:
            return super(Event, self).ttl()

    def validate(self, partial=False, strict=False):
        if self.id is None and self.privacy == 'public':
            # for new events make sure there is an existing event with the same name
            try:
                existing_event = self.find(group=self.group, name=self.name)
                if existing_event.id != self.id:
                    raise AlreadyExistsException(existing_event)
            except DoesNotExist:
                pass

        return super(Event, self).validate(partial, strict)

    @classmethod
    def find(cls, *args, **kwargs):
        if 'name' in kwargs:
            from server.db import wigo_db

            name = kwargs.get('name')
            group = kwargs.get('group')
            if not group:
                raise TypeError('Missing group argument')

            uniqe_name_key = skey(group, Event, Event.event_key(name, group))
            event_id = wigo_db.get(uniqe_name_key)
            if event_id:
                try:
                    return Event.find(int(event_id))
                except DoesNotExist:
                    wigo_db.delete(uniqe_name_key)
            raise DoesNotExist()

        return super(Event, cls).find(*args, **kwargs)

    @classmethod
    def event_key(cls, event_name, group):
        event_key = event_name
        if event_key:
            event_key = strip_unicode(strip_punctuation(event_key.strip().lower())).strip()
            words = event_key.split()
            if len(words) > 0 and words[0] in EVENT_LEADING_STOP_WORDS:
                event_key = ' '.join(words[1:])
        if not event_key:
            event_key = event_name
        return '{}:{}'.format(event_key.encode('utf-8'), group.get_day_end().date().isoformat())

    def save(self):
        created = self.id is None

        if not self.date and not self.expires and self.group:
            self.date = self.group.get_day_start()
            self.expires = self.group.get_day_end()

        super(Event, self).save()

        if created and self.owner_id:
            EventAttendee({
                'user_id': self.owner_id,
                'event_id': self.id
            }).save()

    def index(self):
        super(Event, self).index()

        with self.db.pipeline(commit_on_select=False):
            self.update_global_events()

    def update_global_events(self, group=None):
        if group is None:
            group = self.group

        events_key = skey('group', group.id, 'events')
        attendees_key = skey(self, 'attendees')

        if group.id == self.group_id:
            distance = 0
            event_name_key = skey(group, Event, Event.event_key(self.name, group))
        else:
            event_name_key = None
            # get the distance to this event
            distance = Location.getLatLonDistance((self.group.latitude, self.group.longitude),
                                                  (group.latitude, group.longitude))

        with self.db.pipeline(commit_on_select=False):
            if self.privacy == 'public':
                if event_name_key:
                    self.db.set(event_name_key, self.id, self.expires, self.expires)

                num_attending = self.db.get_sorted_set_size(attendees_key)
                num_messages = get_cached_num_messages(self.id) if self.is_expired else 10

                if self.is_new is False and self.owner_id is not None and (num_attending == 0 or num_messages == 0):
                    self.db.sorted_set_remove(events_key, self.id)

                    if group.id == self.group_id and self.is_global:
                        self.db.sorted_set_remove(skey('global', 'events'), self.id)

                else:
                    # special scoring of verified, and verified global events
                    if self.is_verified:
                        score = get_score_key(self.expires, 0 if self.is_global else distance, 500 + num_attending)
                    else:
                        score = get_score_key(self.expires, distance, num_attending)

                    self.db.sorted_set_add(events_key, self.id, score)
                    if group.id == self.group_id and self.is_global:
                        self.db.sorted_set_add(skey('global', 'events'), self.id, score)

            else:
                # if the event is being made private, make sure it hasn't taken the name
                if event_name_key:
                    try:
                        existing_event = self.find(group=group, name=self.name)
                        if existing_event.id == self.id:
                            self.db.delete(event_name_key)
                    except DoesNotExist:
                        pass

                self.db.sorted_set_remove(events_key, self.id)

    def update_user_events(self, user):
        events_key = skey(user, 'events')

        with self.db.pipeline(commit_on_select=False):
            current_attending = user.get_attending_id(self)
            if current_attending and current_attending == self.id:
                self.db.sorted_set_add(events_key, self.id, get_score_key(self.expires, 0, 1000))
                self.db.clean_old(events_key, self.TTL)
            else:
                num_attending = self.db.get_sorted_set_size(user_attendees_key(user, self))
                num_messages = get_cached_num_messages(self.id, user.id) if self.is_expired else 10

                if self.is_new is False and (num_attending == 0 or num_messages == 0):
                    self.db.sorted_set_remove(events_key, self.id)
                else:
                    distance = Location.getLatLonDistance((self.group.latitude, self.group.longitude),
                                                          (user.group.latitude, user.group.longitude))

                    self.db.sorted_set_add(events_key, self.id, get_score_key(self.expires, distance, num_attending))
                    self.db.clean_old(events_key, self.TTL)

            user.track_meta('last_event_change')

    def add_to_user_attending(self, user, attendee):
        # add to the users view of who is attending
        with self.db.pipeline(commit_on_select=False):
            attendees_key = user_attendees_key(user, self)
            self.db.sorted_set_add(attendees_key, attendee.id, time())
            self.db.expire(attendees_key, self.ttl())

        # add to the users current events list
        self.update_user_events(user)

    def remove_from_user_attending(self, user, attendee):
        # remove from the users view of who is attending
        self.db.sorted_set_remove(user_attendees_key(user, self), attendee.id)
        # update the users event list for this event, removing if now empty
        self.update_user_events(user)

    def remove_index(self, group=None):
        super(Event, self).remove_index()

        if group is None:
            group = self.group

        with self.db.pipeline(commit_on_select=False):
            self.db.delete(skey(self, 'attendees'))
            self.db.delete(skey(self, 'messages'))
            self.db.sorted_set_remove(skey(group, 'events'), self.id)

            if group.id == self.group_id:
                try:
                    existing_event = self.find(group=group, name=self.name)
                    if existing_event.id == self.id:
                        self.db.delete(skey(group, Event, Event.event_key(self.name, group)))
                except:
                    pass


class EventAttendee(WigoModel):
    user_id = LongType(required=True)
    event_id = LongType(required=True)

    def validate(self, partial=False, strict=False):
        super(EventAttendee, self).validate(partial, strict)
        if not self.user.can_see_event(self.event):
            raise ValidationException('Not invited')

    def index(self):
        super(EventAttendee, self).index()

        user = self.user
        event = self.event

        # check if the user is switching events for the date the event is on
        current_event_id = user.get_attending_id(event)
        if current_event_id and current_event_id != event.id:
            EventAttendee({'event_id': current_event_id, 'user_id': user.id}).delete()

        with self.db.pipeline(commit_on_select=False):
            # first update the global state of the event
            attendees_key = skey(event, 'attendees')
            self.db.sorted_set_add(attendees_key, user.id, epoch(self.created))
            self.db.expire(attendees_key, event.ttl())

            # now update the users view of the events
            # record the exact event the user is currently attending
            user.set_attending(event)

            # record current user as an attendee
            attendees_key = user_attendees_key(user, event)
            self.db.sorted_set_add(attendees_key, user.id, 'inf')
            self.db.expire(attendees_key, event.ttl())

        # record the event into the events the user can see
        with self.db.pipeline(commit_on_select=False):
            event.update_global_events()
            event.update_user_events(user)

    def remove_index(self):
        super(EventAttendee, self).remove_index()
        user = self.user
        event = self.event

        # the event may have been deleted
        if event:
            with self.db.pipeline(commit_on_select=True):
                user.remove_attending(event)
                self.db.sorted_set_remove(skey(event, 'attendees'), user.id)
                self.db.sorted_set_remove(user_attendees_key(user, event), user.id)

            with self.db.pipeline(commit_on_select=True):
                event.update_global_events()
                event.update_user_events(user)
        else:
            user.remove_attending(event)


class EventMessage(WigoPersistentModel):
    TTL = DEFAULT_EXPIRING_TTL

    indexes = (
        ('event:{event_id}:messages', False, True),
        ('user:{user_id}:event_messages', False, True),
    )

    class Options:
        roles = {
            'www': blacklist('vote_boost'),
            'www-edit': blacklist('id', 'vote_boost', 'user_id'),
        }
        serialize_when_none = False

    event_id = LongType(required=True)
    user_id = LongType(required=True)
    message = StringType()
    media_mime_type = StringType()
    media = StringType()
    image = StringType()
    thumbnail = StringType()
    vote_boost = IntType()

    properties = JsonType(default=lambda: {})

    tags = ListType(StringType)

    def ttl(self):
        if self.event:
            return self.event.ttl()
        else:
            return super(EventMessage, self).ttl()

    def validate(self, partial=False, strict=False):
        super(EventMessage, self).validate(partial, strict)
        if self.is_new and not self.user.is_attending(self.event):
            raise ValidationException('Not attending event')

    def index(self):
        super(EventMessage, self).index()

        event = self.event

        # record to the global by_votes sort
        with self.db.pipeline(commit_on_select=False):
            num_votes = self.db.get_sorted_set_size(skey(self, 'votes'))
            sub_sort = epoch(self.created) / epoch(event.expires + timedelta(days=365))
            by_votes_key = skey(event, 'messages', 'by_votes')
            self.db.sorted_set_add(by_votes_key, self.id, num_votes + sub_sort)
            self.db.expire(by_votes_key, self.ttl())

            self.record_for_user(self.user)

    def record_for_user(self, user):
        event = self.event

        # record into users list of messages by time
        with self.db.pipeline(commit_on_select=False):
            user_emessages_key = user_eventmessages_key(user, event)
            self.db.sorted_set_add(user_emessages_key, self.id, epoch(self.created))
            self.db.expire(user_emessages_key, self.ttl())

            # record into users list by vote count
            num_votes = self.db.get_sorted_set_size(skey(self, 'votes'))
            sub_sort = epoch() / epoch(event.expires + timedelta(days=365))
            by_votes_key = user_eventmessages_key(user, event, True)
            self.db.sorted_set_add(by_votes_key, self.id, num_votes + sub_sort, replicate=False)
            self.db.expire(by_votes_key, self.ttl())

            user.track_meta('last_event_change')

    def remove_index(self):
        super(EventMessage, self).remove_index()
        with self.db.pipeline(commit_on_select=False):
            self.db.sorted_set_remove(skey(self.event, 'messages', 'by_votes'), self.id)
            self.remove_for_user(self.user)

    def remove_for_user(self, user):
        event = self.event
        with self.db.pipeline(commit_on_select=False):
            self.db.sorted_set_remove(user_eventmessages_key(user, event), self.id)
            self.db.sorted_set_remove(user_eventmessages_key(user, event, True), self.id)
            user.track_meta('last_event_change')


class EventMessageVote(WigoModel):
    TTL = DEFAULT_EXPIRING_TTL

    indexes = (
        ('eventmessage:{message_id}:votes={user_id}', False, True),
        ('user:{user_id}:votes={message_id}', False, True)
    )

    message_id = LongType(required=True)
    user_id = LongType(required=True)

    def ttl(self):
        if self.message:
            return self.message.ttl()
        else:
            return super(EventMessageVote, self).ttl()

    @property
    @field_memoize('message_id')
    def message(self):
        try:
            return EventMessage.find(self.message_id)
        except DoesNotExist:
            logger.warn('event message {} not found'.format(self.message_id))
        return None

    def index(self):
        super(EventMessageVote, self).index()

        user = self.user
        message = self.message
        event = message.event

        # record the vote into the global "by_votes" sort order
        with self.db.pipeline(commit_on_select=False):
            num_votes = self.db.get_sorted_set_size(skey(message, 'votes'))
            sub_sort = epoch() / epoch(event.expires + timedelta(days=365))
            by_votes = skey(event, 'messages', 'by_votes')
            self.db.sorted_set_add(by_votes, self.message_id, num_votes + sub_sort, replicate=False)
            self.db.expire(by_votes, self.ttl())

        # record the vote into the users view of votes
        # a job will take care of recording the vote for friends
        # self.record_for_user(user)

    def record_for_user(self, user):
        message = self.message
        event = message.event

        with self.db.pipeline(commit_on_select=False):
            user_votes = user_votes_key(user, self.message)
            self.db.sorted_set_add(user_votes, self.user.id, epoch(self.created), replicate=False)
            self.db.expire(user_votes, self.ttl())

        # this forces the message to update its indexes for the user
        message.record_for_user(user)

    def remove_index(self):
        super(EventMessageVote, self).remove_index()

        message = self.message
        event = message.event

        self.db.sorted_set_remove(skey(event, 'messages', 'by_votes'), self.message_id, replicate=False)


@cache_maker.expiring_lrucache(maxsize=5000, timeout=60 * 60 * 12)
def get_cached_num_messages(event_id, user_id=None):
    """ Should only be called on expired events. """
    from server.db import wigo_db

    key = (skey('user', user_id, 'event', event_id, 'messages')
           if user_id is not None else skey('event', event_id, 'messages'))

    return wigo_db.get_sorted_set_size(key)


@cache_maker.expiring_lrucache(maxsize=5000, timeout=60 * 60 * 12)
def get_cached_num_attending(event_id, user_id=None):
    """ Should only be called on expired events. """
    return get_num_attending(event_id, user_id)


def get_num_attending(event_id, user_id=None):
    """ Should only be called on expired events. """
    from server.db import wigo_db

    key = (skey('user', user_id, 'event', event_id, 'attendees')
           if user_id is not None else skey('event', event_id, 'attendees'))

    return wigo_db.get_sorted_set_size(key)


def get_score_key(time, distance, num_attending):
    if num_attending > 999:
        num_attending = 999

    targets = [0, 20, 50, 70, 100]
    distance_bucket = next(reversed([t for t in targets if t <= distance]), None) + 10
    adjustment = (1 - (distance_bucket / 1000.0)) + (num_attending / 10000.0)
    return str(Decimal(epoch(time)) + Decimal(adjustment))
