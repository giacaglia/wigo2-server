from __future__ import absolute_import

import math
from datetime import datetime, timedelta
from server.models import user_eventmessages_key, skey, user_attendees_key, DoesNotExist, index_key
from server.models.event import EventMessage, EventAttendee, Event
from server.models.group import Group
from server.models.user import Message, User, Notification
from server.rdbms import DataStrings
from utils import returns_clone, epoch


class SelectQuery(object):
    def __init__(self, model_class=None, fields=None):
        super(SelectQuery, self).__init__()
        self._fields = fields
        self._model_class = model_class
        self._id = None
        self._ids = None
        self._key = None
        self._page = 1
        self._limit = 10
        self._order = 'desc'
        self._user = None
        self._to_user = None
        self._group = None
        self._event = None
        self._events = None
        self._friends = False
        self._friend_requested = False
        self._friend_requests = False
        self._min = None
        self._max = None
        self._lat = None
        self._lon = None
        self._where = {}

    def clone(self):
        clone = SelectQuery(self._model_class)
        clone._fields = self._fields
        clone._id = self._id
        clone._ids = self._ids
        clone._key = self._key
        clone._page = self._page
        clone._limit = self._limit
        clone._order = self._order
        clone._event = self._event
        clone._events = list(self._events) if self._events is not None else None
        clone._group = self._group
        clone._user = self._user
        clone._to_user = self._to_user
        clone._friends = self._friends
        clone._friend_requested = self._friend_requested
        clone._friend_requests = self._friend_requests
        clone._where = dict(self._where)
        clone._min = self._min
        clone._max = self._max
        clone._lat = self._lat
        clone._lon = self._lon
        return clone

    @property
    def db(self):
        from server.db import wigo_db

        return wigo_db

    @returns_clone
    def id(self, id):
        self._id = id

    @returns_clone
    def model_class(self, model_class):
        self._model_class = model_class

    @returns_clone
    def ids(self, ids):
        self._ids = ids

    @returns_clone
    def user(self, user):
        self._user = user

    @returns_clone
    def to_user(self, to_user):
        self._to_user = to_user

    @returns_clone
    def group(self, group):
        self._group = group

    @returns_clone
    def key(self, key):
        self._key = key

    @returns_clone
    def event(self, event):
        self._event = event

    @returns_clone
    def events(self, events):
        self._events = events

    @returns_clone
    def friends(self):
        self._friends = True

    @returns_clone
    def friend_requested(self):
        self._friend_requested = True

    @returns_clone
    def friend_requests(self):
        self._friend_requests = True

    @returns_clone
    def page(self, page):
        self._page = max(page, 1)

    @returns_clone
    def order(self, order):
        self._order = order

    @returns_clone
    def limit(self, limit):
        self._limit = max(limit, 1)

    @returns_clone
    def min(self, min):
        self._min = min

    @returns_clone
    def max(self, max):
        self._max = max

    @returns_clone
    def lat(self, lat):
        self._lat = lat

    @returns_clone
    def lon(self, lon):
        self._lon = lon

    @returns_clone
    def where(self, **kwargs):
        self._where.update(kwargs)

    def count(self):
        count, results = self.execute()
        return count

    def execute(self):
        if self._id:
            instance = self._model_class.find(self._id)
            if instance:
                return 1, [instance]
            return 0, []
        elif self._ids:
            instances = self._model_class.find(self._ids)
            if len(instances) > 0:
                return len(instances), instances
            return 0, []
        elif self._key:
            return self.__get_page(self._key)
        elif self._where:
            return self.__filtered()
        elif self._user and self._friends:
            return self.__get_page(skey(self._user, 'friends'))
        elif self._user and self._friend_requests:
            return self.__get_page(skey(self._user, 'friend_requests'))
        elif self._user and self._friend_requested:
            return self.__get_page(skey(self._user, 'friend_requested'))
        elif self._user and self._model_class == Message:
            return self.__get_conversation()
        elif self._user and self._model_class == Notification:
            return self.__get_notifications()
        elif self._user and self._model_class == Event:
            group = self._group if self._group else self._user.group
            return self.__get_page(skey(group, self._user, 'events'))
        elif self._group and self._model_class == Event:
            return self.__get_page(skey(self._group, 'events'))
        elif self._model_class == Group and self._lat and self._lon:
            return self.__get_group()
        elif self._model_class == User and self._group:
            return self.__get_page(skey(self._group, 'users'))
        elif self._event:
            return self.__get_by_event()
        elif self._events:
            return self.__get_by_events()
        else:
            return self.__get_page(skey(self._model_class))

    def __iter__(self):
        query = self.clone()
        count, instances = self.execute()
        pages = int(math.ceil(float(count) / self._limit))
        while True:
            for instance in instances:
                yield instance
            if query.page < pages:
                query = query.page(query._page + 1)
                count, instances = self.execute()
            else:
                break

    def get(self):
        count, instances = self.execute()
        if len(instances) > 0:
            return instances[0]
        raise DoesNotExist()

    def __get_page(self, key):
        start = (self._page - 1) * self._limit

        count = self.db.get_sorted_set_size(key)
        if count == 0:
            return 0, []

        min = self._min or '-inf'
        max = self._max or '+inf'

        if self._model_class == Event and self._group:
            if min is None:
                min = epoch(datetime.utcnow() - timedelta(days=7))
            if max is None:
                max = epoch(self._group.get_day_end())

        if self._order == 'desc':
            range_f = self.db.sorted_set_rrange_by_score
            min, max = max, min
        else:
            range_f = self.db.sorted_set_range_by_score

        model_ids = range_f(key, min, max, start, self._limit)
        return count, self._model_class.find(model_ids)

    def __get_group(self):
        try:
            group = Group.find(lat=self._lat, lon=self._lon)
            return 1, [group]
        except:
            return 0, []

    def __get_conversation(self):
        if self._to_user:
            return self.__get_page(skey(self._user, 'conversation', self._to_user.id))
        else:
            query = User.select().key(
                skey(self._user, 'conversations')
            ).limit(self._limit).page(self._page).order(self._order)
            count, users = query.execute()
            message_ids = []
            for user in users:
                last_message_id = self.db.get(skey(self._user, 'conversation', user.id, 'last_message'))
                message_ids.append(last_message_id)
            return count, Message.find(message_ids)

    def __get_notifications(self):
        return self.__get_page(skey(self._user, 'notifications'))

    def __filtered(self):
        from server.db import wigo_db
        from server.rdbms import db

        try:
            for kwarg in self._where:
                applicable_indexes = [key_tmpl for key_tmpl, unique in self._model_class.indexes if kwarg in key_tmpl]
                if applicable_indexes:
                    for key_tmpl in applicable_indexes:
                        key = index_key(key_tmpl, {kwarg: self._where.get(kwarg)})
                        model_ids = wigo_db.sorted_set_range(key, 0, -1)
                        if model_ids:
                            instances = self._model_class.find(model_ids)
                            return len(instances), instances
                        else:
                            return 0, []
                else:
                    # not a redis indexed key, so look in the rdbms
                    with db.execution_context(False) as ctx:
                        keys = [k[0] for k in DataStrings.select(DataStrings.key).where(
                            DataStrings.value.contains({kwarg: self._where.get(kwarg)})
                        ).tuples()]

                    # still fetch the actual objects from redis
                    results = wigo_db.mget(keys)
                    instances = [self._model_class(result) if result is not None else None for result in results]
                    for instance in instances:
                        if instance is not None:
                            instance.prepared()

                    return len(instances), instances

        except DoesNotExist:
            return 0, []

    def __get_by_event(self):
        if self._model_class == EventMessage:
            if self._user:
                key = self.db.user_eventmessages_key(self._user, self._event)
            else:
                key = skey(self._event, 'messages')
            return self.__get_page(key)
        elif self._model_class == EventAttendee:
            if self._user:
                key = user_attendees_key(self._user, self._event)
            else:
                key = skey(self._event, 'attendees')
            return self.model_class(User).key(key).execute()
        else:
            raise ValueError('Invalid query')


    def __get_by_events(self):
        start = (self._page - 1) * self._limit

        keys = []

        if self._model_class == EventMessage:
            query_class = EventMessage
            if self._user:
                keys = [user_eventmessages_key(self._user, event) for event in self._events]
            else:
                keys = [skey(event, 'messages') for event in self._events]
        elif self._model_class == EventAttendee:
            query_class = User
            if self._user:
                keys = [user_attendees_key(self._user, event) for event in self._events]
            else:
                keys = [skey(event, 'attendees') for event in self._events]
        else:
            raise ValueError('Invalid query')

        results = []
        for key in keys:
            count = self.db.get_sorted_set_size(key)
            ids = self.db.sorted_set_rrange(key, start, start + (self._limit - 1))
            instances = query_class.find(list(ids))
            results.append((count, instances))

        return len(results), results

