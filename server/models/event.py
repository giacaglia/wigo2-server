from __future__ import absolute_import

from datetime import datetime, timedelta
from geodis.location import Location
from repoze.lru import CacheMaker
from schematics.transforms import blacklist
from schematics.types import LongType, StringType, IntType, DateTimeType
from schematics.types.compound import ListType
from schematics.types.serializable import serializable
from server.models import WigoModel, WigoPersistentModel, get_score_key, skey, DoesNotExist, \
    AlreadyExistsException, user_attendees_key, user_eventmessages_key, DEFAULT_EXPIRING_TTL, field_memoize, \
    user_votes_key
from utils import strip_unicode, strip_punctuation, epoch, ValidationException

EVENT_LEADING_STOP_WORDS = {"a", "the"}

cache_maker = CacheMaker(maxsize=1000, timeout=60)


class Event(WigoPersistentModel):
    indexes = (
        ('event', False),
    )

    group_id = LongType(required=True)
    owner_id = LongType()
    name = StringType(required=True)
    privacy = StringType(choices=('public', 'private'), required=True, default='public')

    date = DateTimeType(required=True)
    expires = DateTimeType(required=True)

    tags = ListType(StringType)

    @serializable
    def is_expired(self):
        return datetime.utcnow() > self.expires

    def ttl(self):
        return DEFAULT_EXPIRING_TTL

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

            uniqe_name_key = skey(group, Event, Event.event_key(name))
            event_id = wigo_db.get(uniqe_name_key)
            if event_id:
                try:
                    return Event.find(int(event_id))
                except DoesNotExist:
                    wigo_db.delete(uniqe_name_key)
            raise DoesNotExist()

        return super(Event, cls).find(*args, **kwargs)

    @classmethod
    def event_key(cls, event_name):
        event_key = event_name
        if event_key:
            event_key = strip_unicode(strip_punctuation(event_key.strip().lower())).strip()
            words = event_key.split()
            if len(words) > 0 and words[0] in EVENT_LEADING_STOP_WORDS:
                event_key = ' '.join(words[1:])
        if not event_key:
            event_key = event_name
        return event_key

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
        self.add_to_global_events()

        # TODO on privacy change iterate the attendees of event and update attending
        # TODO also the global event needs to be updated

        self.clean_old(skey('group', self.group_id, 'events'))

    def add_to_global_events(self, remove_empty=False):
        group = self.group

        events_key = skey('group', self.group_id, 'events')
        attendees_key = skey(self, 'attendees')
        event_name_key = skey(group, Event, Event.event_key(self.name))

        if self.privacy == 'public':
            self.db.set(event_name_key, self.id, self.expires, self.expires)
            num_attending = self.db.get_sorted_set_size(attendees_key)
            if remove_empty and (self.owner_id is not None and num_attending == 0):
                self.db.sorted_set_remove(events_key, self.id)
            else:
                self.db.sorted_set_add(events_key, self.id, get_score_key(self.expires, 0, num_attending))
        else:
            try:
                existing_event = self.find(group=group, name=self.name)
                if existing_event.id == self.id:
                    self.db.delete(event_name_key)
            except DoesNotExist:
                pass

            self.db.sorted_set_remove(events_key, self.id)

    def add_to_user_events(self, user, remove_empty=False):
        events_key = skey(user, 'events')

        current_attending = user.get_attending_id()
        if current_attending and current_attending == self.id:
            self.db.sorted_set_add(events_key, self.id, get_score_key(self.expires, 0, 100000))
            self.clean_old(events_key)
        else:
            num_attending = self.db.get_sorted_set_size(user_attendees_key(user, self))
            if remove_empty and num_attending == 0:
                self.db.sorted_set_remove(events_key, self.id)
            else:
                distance = Location.getLatLonDistance((self.group.latitude, self.group.longitude),
                                                      (user.group.latitude, user.group.longitude))

                self.db.sorted_set_add(events_key, self.id, get_score_key(self.expires, distance, num_attending))
                self.clean_old(events_key)

    def add_to_user_attending(self, user, attendee, score=1):
        # add to the users view of who is attending
        attendees_key = user_attendees_key(user, self)
        self.db.sorted_set_add(attendees_key, attendee.id, score)
        self.db.expire(attendees_key, DEFAULT_EXPIRING_TTL)

        # add to the users current events list
        self.add_to_user_events(user)

    def remove_from_user_attending(self, user, attendee):
        # remove from the users view of who is attending
        self.db.sorted_set_remove(user_attendees_key(user, self), attendee.id)
        # update the users event list for this event, removing if now empty
        self.add_to_user_events(user, remove_empty=True)

    def remove_index(self):
        super(Event, self).remove_index()
        group = self.group

        self.db.sorted_set_remove(skey(group, 'events'), self.id)
        self.db.delete(skey(self, 'attendees'))
        self.db.delete(skey(self, 'messages'))

        try:
            existing_event = self.find(group=group, name=self.name)
            if existing_event.id == self.id:
                self.db.delete(skey(group, Event, Event.event_key(self.name)))
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
        group = event.group

        # check if the user is switching events for today
        current_event_id = user.get_attending_id()
        if current_event_id and current_event_id != event.id:
            EventAttendee({'event_id': current_event_id, 'user_id': user.id}).delete()

        # first update the global state of the event
        attendees_key = skey(event, 'attendees')
        self.db.sorted_set_add(attendees_key, user.id, epoch(self.created))
        self.db.expire(attendees_key, DEFAULT_EXPIRING_TTL)
        event.add_to_global_events()

        # now update the users view of the events
        # record the exact event the user is currently attending
        user.set_attending(event)

        # record current user as an attendee
        attendees_key = user_attendees_key(user, event)
        self.db.sorted_set_add(attendees_key, user.id, 'inf')
        self.db.expire(attendees_key, DEFAULT_EXPIRING_TTL)

        # record the event into the events the user can see
        event.add_to_user_events(user)

    def remove_index(self):
        super(EventAttendee, self).remove_index()
        user = self.user
        event = self.event

        # first update the global state of the event
        self.db.sorted_set_remove(skey(event, 'attendees'), user.id)
        event.add_to_global_events(remove_empty=True)

        # now update the users view of the events
        self.db.delete(skey(user, 'current_attending'))
        self.db.sorted_set_remove(user_attendees_key(user, event), user.id)
        event.add_to_user_events(user, remove_empty=True)


class EventMessage(WigoPersistentModel):
    indexes = (
        ('event:{event_id}:messages', False),
        ('user:{user_id}:event_messages', False),
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
    media_mime_type = StringType(required=True)
    media = StringType(required=True)
    thumbnail = StringType()
    vote_boost = IntType()
    tags = ListType(StringType)

    def ttl(self):
        return DEFAULT_EXPIRING_TTL

    def validate(self, partial=False, strict=False):
        super(EventMessage, self).validate(partial, strict)
        if not self.user.is_attending(self.event):
            raise ValidationException('Not attending event')

    def index(self):
        super(EventMessage, self).index()

        event = self.event

        # record to the global by_votes sort
        by_votes_key = skey(event, 'messages', 'by_votes')
        sub_sort = epoch(self.created) / epoch(event.expires + timedelta(days=365))
        self.db.sorted_set_add(by_votes_key, self.id, sub_sort)
        self.db.expire(by_votes_key, DEFAULT_EXPIRING_TTL)

        self.record_for_user(self.user)

    def record_for_user(self, user):
        event = self.event

        # record into users list of messages by time
        user_emessages_key = user_eventmessages_key(user, event)
        self.db.sorted_set_add(user_emessages_key, self.id, epoch(self.created))
        self.db.expire(user_emessages_key, DEFAULT_EXPIRING_TTL)

        # record into users list by vote count
        num_votes = self.db.get_set_size(user_votes_key(user, self))
        sub_sort = epoch() / epoch(event.expires + timedelta(days=365))
        by_votes_key = user_eventmessages_key(user, event, True)
        self.db.sorted_set_add(by_votes_key, self.id, num_votes + sub_sort, replicate=False)
        self.db.expire(by_votes_key, self.ttl())

    def remove_index(self):
        super(EventMessage, self).remove_index()
        self.db.sorted_set_remove(skey(self.event, 'messages', 'by_votes'), self.id)
        self.remove_for_user(self.user)

    def remove_for_user(self, user):
        event = self.event
        self.db.sorted_set_remove(user_eventmessages_key(user, event), self.id)
        self.db.sorted_set_remove(user_eventmessages_key(user, event, True), self.id)


class EventMessageVote(WigoModel):
    indexes = (
        ('eventmessage:{message_id}:votes={user_id}', False),
        ('user:{user_id}:votes={message_id}', False),
    )

    message_id = LongType(required=True)
    user_id = LongType(required=True)

    def ttl(self):
        return DEFAULT_EXPIRING_TTL

    @property
    @field_memoize('message_id')
    def message(self):
        return EventMessage.find(self.message_id)

    def index(self):
        super(EventMessageVote, self).index()

        user = self.user
        message = self.message
        event = message.event

        # record the vote into the global "by_votes" sort order
        num_votes = self.db.get_sorted_set_size(skey(message, 'votes'))
        sub_sort = epoch() / epoch(event.expires + timedelta(days=365))
        by_votes = skey(event, 'messages', 'by_votes')
        self.db.sorted_set_add(by_votes, self.message_id, num_votes + sub_sort, replicate=False)
        self.db.expire(by_votes, self.ttl())

        # record the vote into the users view of votes
        # a job will take care of recording the vote for friends
        self.record_for_user(user)

    def record_for_user(self, user):
        message = self.message
        event = message.event

        user_votes = user_votes_key(user, self.message)
        self.db.set_add(user_votes, self.user.id, replicate=False)
        self.db.expire(user_votes, self.ttl())

        # this forces the message to update its indexes for the user
        message.record_for_user(user)

    def remove_index(self):
        super(EventMessageVote, self).remove_index()

        message = self.message
        event = message.event

        self.db.sorted_set_remove(skey(event, 'messages', 'by_votes'), self.message_id, replicate=False)


@cache_maker.expiring_lrucache(maxsize=5000, timeout=60 * 60)
def get_num_messages(event_id, user_id=None):
    """ Should only be called on expired events. """
    from server.db import wigo_db

    key = (skey('user', user_id, 'event', event_id, 'messages')
           if user_id else skey('event', event_id, 'messages'))

    return wigo_db.get_sorted_set_size(key)
