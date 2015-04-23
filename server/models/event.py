from __future__ import absolute_import
from datetime import timedelta, datetime
from schematics.transforms import blacklist
from schematics.types import LongType, StringType, IntType, DateTimeType
from schematics.types.compound import ListType
from schematics.types.serializable import serializable
from server.models import WigoModel, WigoPersistentModel, get_score_key, skey, DoesNotExist, \
    AlreadyExistsException, user_attendees_key, user_eventmessages_key, DEFAULT_EXPIRING_TTL
from server.models.user import User
from utils import strip_unicode, strip_punctuation, epoch, ValidationException, memoize

EVENT_LEADING_STOP_WORDS = {"a", "the"}


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
                self.db.sorted_set_add(events_key, self.id, get_score_key(self.expires, num_attending))
        else:
            try:
                existing_event = self.find(group=group, name=self.name)
                if existing_event.id == self.id:
                    self.db.delete(event_name_key)
            except DoesNotExist:
                pass

            self.db.sorted_set_remove(events_key, self.id)

    def add_to_user_events(self, user, remove_empty=False):
        events_key = skey(user.group, user, 'events')

        current_attending = user.get_attending_id()
        if current_attending and current_attending == self.id:
            self.db.sorted_set_add(events_key, self.id, get_score_key(self.expires, 100000))
            self.clean_old(events_key)
        else:
            num_attending = self.db.get_sorted_set_size(user_attendees_key(user, self))
            if remove_empty and num_attending == 0:
                self.db.sorted_set_remove(events_key, self.id)
            else:
                self.db.sorted_set_add(events_key, self.id, get_score_key(self.expires, num_attending))
                self.clean_old(events_key)

    def add_to_user_attending(self, user, attendee, score=1):
        # add to the users view of who is attending
        attendees_key = user_attendees_key(user, self)

        self.db.sorted_set_add(attendees_key, attendee.id, score)
        self.db.expire(attendees_key, DEFAULT_EXPIRING_TTL)

        # add the attendees photos to the users view
        emessages_key = user_eventmessages_key(user, self)
        for message_id, score in self.db.sorted_set_iter(user_eventmessages_key(attendee, self)):
            self.db.sorted_set_add(emessages_key, message_id, score)
        self.db.expire(emessages_key, DEFAULT_EXPIRING_TTL)

        # add to the users current events list
        self.add_to_user_events(user)

    def remove_from_user_attending(self, user, attendee):
        # add to the users view of who is attending
        self.db.sorted_set_remove(user_attendees_key(user, self), attendee.id)
        # add to the users current events list
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

    @classmethod
    def annotate_list(cls, events, user, attendees_limit=5, messages_limit=5):
        from server.db import wigo_db

        for event in events:
            if user:
                event.num_attending = wigo_db.get_sorted_set_size(user_attendees_key(user, event))
            else:
                event.num_attending = wigo_db.get_sorted_set_size(skey(event, 'attendees'))

        count, attendees_by_event = EventAttendee.select().events(events).user(user).limit(attendees_limit).execute()
        if count:
            for event, attendees in zip(events, attendees_by_event):
                event.attendees = attendees

        count, messages_by_event = EventMessage.select().events(events).user(user).limit(messages_limit).execute()
        if count:
            for event, messages in zip(events, messages_by_event):
                event.messages = messages
        return events


class EventAttendee(WigoModel):
    user_id = LongType(required=True)
    event_id = LongType(required=True)

    def validate(self, partial=False, strict=False):
        super(EventAttendee, self).validate(partial, strict)
        if not self.user.is_invited(self.event):
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
        if user.privacy != 'private':
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

        # record the event into the events the user can see, as the most important one
        event.add_to_user_events(user)

        for friend_id, score in self.db.sorted_set_iter(skey(user, 'friends')):
            friend = User.find(int(friend_id))
            # add to this users list of friends attending
            if friend.is_attending(event):
                event.add_to_user_attending(user, friend, score)
            # add to each of the users friends that this user is attending
            if friend.is_invited(event):
                event.add_to_user_attending(friend, user, score)

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

        for friend_id, score in self.db.sorted_set_iter(skey(user, 'friends')):
            friend = User.find(int(friend_id))
            event.remove_from_user_attending(friend, user)


class EventMessage(WigoPersistentModel):
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
        user = self.user
        event = self.event

        emessages_key = user_eventmessages_key(user, event)
        self.db.sorted_set_add(emessages_key, self.id, epoch(self.created))
        self.db.expire(emessages_key, DEFAULT_EXPIRING_TTL)

        if user.privacy == 'public':
            emessages_key = skey(event, 'messages')
            self.db.sorted_set_add(emessages_key, self.id, epoch(self.created))
            self.db.expire(emessages_key, DEFAULT_EXPIRING_TTL)

        for friend_id, score in self.db.sorted_set_iter(skey(user, 'friends')):
            friend = User.find(int(friend_id))
            emessages_key = user_eventmessages_key(friend, event)
            self.db.sorted_set_add(emessages_key, self.id, epoch(self.created))
            self.db.expire(emessages_key, DEFAULT_EXPIRING_TTL)

    def remove_index(self):
        super(EventMessage, self).remove_index()
        user = self.user
        event = self.event

        messages_key = skey(event, 'messages')
        self.db.sorted_set_remove(messages_key, self.id)

        for friend_id, score in self.db.sorted_set_iter(skey(user, 'friends')):
            friend = User.find(int(friend_id))
            self.db.sorted_set_remove(user_eventmessages_key(friend, event), user.id)


class EventMessageVote(WigoModel):
    indexes = (
        ('message:{message_id}:votes={user_id}', False),
    )

    message_id = LongType(required=True)
    user_id = LongType(required=True)

    def ttl(self):
        return DEFAULT_EXPIRING_TTL

    @property
    @memoize('message_id')
    def message(self):
        return EventMessage.find(self.message_id)
