from __future__ import absolute_import

import math
import logging
from datetime import datetime, timedelta
from collections import defaultdict, OrderedDict
from newrelic import agent
from server.models import user_eventmessages_key, skey, user_attendees_key, DoesNotExist, index_key
from server.models.event import EventMessage, EventAttendee, Event, get_cached_num_messages, EventMessageVote, \
    get_cached_num_attending
from server.models.group import Group
from server.models.user import Message, User, Notification
from server.rdbms import DataStrings
from utils import returns_clone, epoch

logger = logging.getLogger('wigo.query')


class SelectQuery(object):
    def __init__(self, model_class=None):
        super(SelectQuery, self).__init__()
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
        self._eventmessage = None
        self._by_votes = False
        self._friends = False
        self._friend_requested = False
        self._friend_requests = False
        self._min = None
        self._max = None
        self._lat = None
        self._lon = None
        self._secure = None
        self._start = None
        self._where = {}

    def clone(self):
        clone = SelectQuery(self._model_class)
        clone._id = self._id
        clone._ids = self._ids
        clone._key = self._key
        clone._page = self._page
        clone._limit = self._limit
        clone._order = self._order
        clone._event = self._event
        clone._events = list(self._events) if self._events is not None else None
        clone._eventmessage = self._eventmessage
        clone._by_votes = self._by_votes
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
        clone._start = self._start
        clone._secure = self._secure
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
    def eventmessage(self, eventmessage):
        self._eventmessage = eventmessage

    @returns_clone
    def by_votes(self, by_votes=True):
        self._by_votes = by_votes

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
    def start(self, start):
        self._start = start

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

    @returns_clone
    def secure(self, user):
        self._secure = user

    def count(self):
        count, page, results = self.execute()
        return count

    @agent.function_trace()
    def execute(self):
        if self._id:
            instance = self._model_class.find(self._id)
            if instance:
                return 1, self._page, [instance]
            return 0, self._page, []
        elif self._ids:
            instances = self._model_class.find(self._ids)
            if len(instances) > 0:
                return len(instances), 1, instances
            return 0, self._page, []
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
            return self.__get_page(skey(self._user, 'events'))
        elif self._group and self._model_class == Event:
            return self.__get_page(skey(self._group, 'events'))
        elif self._eventmessage and self._model_class == EventMessageVote:
            return self.model_class(User).__get_page(skey(self._eventmessage, 'votes'))
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
        count, page, instances = query.execute()
        pages = int(math.ceil(float(count) / query._limit))
        while True:
            for instance in instances:
                yield instance
            if page < pages:
                query = query.page(page + 1)
                count, page, instances = query.execute()
            else:
                break

    def get(self):
        count, page, instances = self.execute()
        if len(instances) > 0:
            return instances[0]
        raise DoesNotExist()

    @agent.function_trace()
    def __get_page(self, key):
        start_page = self._page

        min = self._min or '-inf'
        max = self._max or '+inf'

        count = self.db.get_sorted_set_size(key, min, max)
        if count == 0:
            return 0, start_page, []

        if self._model_class == Event and self._group:
            if min is None:
                min = epoch(datetime.utcnow() - timedelta(days=7))
            if max is None:
                # add 1 hour to account for sub-scoring
                max = epoch(self._group.get_day_end() + timedelta(hours=1))

        if self._order == 'desc':
            range_f = self.db.sorted_set_rrange_by_score
            rank_f = self.db.sorted_set_rrank
            min, max = max, min
        else:
            range_f = self.db.sorted_set_range_by_score
            rank_f = self.db.sorted_set_rank

        pages = int(math.ceil(float(count) / self._limit))

        if self._start:
            position = rank_f(key, self._start)
            if position is not None:
                start_page = int((float(position) / self._limit) + 1)

        collected = []
        page = start_page
        for page in range(start_page, pages + 1):
            start = (page - 1) * self._limit
            model_ids = range_f(key, min, max, start, self._limit, withscores=True)
            instances = self._model_class.find([m[0] for m in model_ids])
            for index, instance in enumerate(instances):
                if instance is not None:
                    instance.score = model_ids[index][1]
            secured = self.__clean_results(instances)
            collected.extend(secured)

            # if the results weren't filtered, break
            if len(secured) == len(instances):
                break

        return count, page, collected

    def __get_group(self):
        try:
            group = Group.find(lat=self._lat, lon=self._lon)
            return 1, self._page, [group]
        except:
            return 0, self._page, []

    def __get_conversation(self):
        if self._to_user:
            return self.__get_page(skey(self._user, 'conversation', self._to_user.id))
        else:
            query = User.select().key(
                skey(self._user, 'conversations')
            ).limit(self._limit).page(self._page).order(self._order)

            count, page, users = query.execute()

            message_ids = []
            for user in users:
                last_message_id = self.db.get(skey(self._user, 'conversation', user.id, 'last_message'))
                message_ids.append(last_message_id)

            return count, page, Message.find(message_ids)

    def __get_notifications(self):
        return self.__get_page(skey(self._user, 'notifications'))

    def __filtered(self):
        from server.db import wigo_db
        from server.rdbms import db

        try:
            for kwarg in self._where:
                applicable_indexes = [key_tmpl for key_tmpl, unique, expires in self._model_class.indexes if
                                      kwarg in key_tmpl]
                if applicable_indexes:
                    for key_tmpl in applicable_indexes:
                        key = index_key(key_tmpl, {kwarg: self._where.get(kwarg)})
                        model_ids = wigo_db.sorted_set_range(key)
                        if model_ids:
                            instances = self._model_class.find(model_ids)
                            return len(instances), self._page, instances
                        else:
                            return 0, self._page, []
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

                    return len(instances), self._page, instances

        except DoesNotExist:
            return 0, self._page, []

    def __get_by_event(self):
        if self._model_class == EventMessage:
            if self._user:
                key = user_eventmessages_key(self._user, self._event, self._by_votes)
            else:
                key = skey(self._event, 'messages', 'by_votes') if self._by_votes else skey(self._event, 'messages')
            return self.__get_page(key)
        elif self._model_class == EventAttendee:
            if self._user:
                key = user_attendees_key(self._user, self._event)
            else:
                key = skey(self._event, 'attendees')
            return self.model_class(User).key(key).execute()
        else:
            raise ValueError('Invalid query')

    @agent.function_trace()
    def __get_by_events(self):
        from server.db import wigo_db

        event_keys = self.__get_events_keys()
        query_class = EventMessage if self._model_class == EventMessage else User

        ####################################################
        # In a single pipeline get all the counts

        p = self.db.redis.pipeline()
        for key in event_keys:
            p.zcard(key)
        counts = p.execute()

        #########################################################
        # run_queries pipelines the queries for the event data

        def run_queries(starts):
            p = self.db.redis.pipeline()

            # run the queries
            for key, start in starts.items():
                p.zrevrange(key, start, start + (self._limit + 5))
            ids_by_key = p.execute()

            all_ids = []
            for index, key in enumerate(starts.keys()):
                count = counts[index]
                decoded_ids = wigo_db.decode(ids_by_key[index], dt=int)
                ids_by_key[index] = decoded_ids
                all_ids.extend(decoded_ids)

            # find all of the objects
            instances = query_class.find(all_ids)
            instances_by_id = {i.id: i for i in instances if i}

            # get the instances for all of the queries
            instances_by_key = {}
            for index, key in enumerate(starts.keys()):
                count = counts[index]
                instances = [instances_by_id.get(instance_id) for instance_id in ids_by_key[index]]
                secured = self.__clean_results(instances)
                instances_by_key[key] = secured

                # if nothing was secured, remove from starts
                if len(secured) >= self._limit or len(secured) >= count:
                    del starts[key]
                else:
                    # move starts to the next position to get more results
                    next_start = starts[key] + (self._limit - 1)
                    if next_start <= count:
                        starts[key] = next_start
                    else:
                        del starts[key]

            return instances_by_key

        ####################################################
        # keep running the queries until starts is empty

        all_instances_by_key = defaultdict(list)
        starts = OrderedDict([(k, 0) for k in event_keys])
        while starts:
            instances_by_key = run_queries(starts)
            for key, instances in instances_by_key.items():
                all_instances_by_key[key].extend(instances)

        ####################################################
        # gather results in the expected return format

        results = []
        for index, key in enumerate(event_keys):
            results.append((counts[index], all_instances_by_key[key][0:self._limit]))

        return len(results), 1, results

    def __get_events_keys(self):
        if self._model_class == EventMessage:
            query_class = EventMessage
            if self._user:
                return [user_eventmessages_key(self._user, event, self._by_votes) for event in self._events]
            else:
                return [skey(event, 'messages', 'by_votes') if self._by_votes else
                        skey(event, 'messages') for event in self._events]
        elif self._model_class == EventAttendee:
            query_class = User
            if self._user:
                return [user_attendees_key(self._user, event) for event in self._events]
            else:
                return [skey(event, 'attendees') for event in self._events]
        else:
            raise ValueError('Invalid query')

    def __clean_results(self, objects):
        from server.db import wigo_db

        if not objects:
            return []

        objects = [o for o in objects if o is not None]
        objects = self.__secure_results(objects)

        if self._model_class == Event:
            events = []
            for e in objects:
                if e.is_expired:
                    num_messages = get_cached_num_messages(e.id, self._user.id if self._user else None)
                    num_attending = get_cached_num_attending(e.id, self._user.id if self._user else None)
                    if num_messages == 0 or num_attending == 0:
                        if self._user:
                            logger.debug('cleaning event {} for user'.format(e.id))
                            wigo_db.sorted_set_remove(skey(self._user, 'events'), e.id)
                        elif self._group:
                            logger.debug('cleaning event {} for group'.format(e.id))
                            wigo_db.sorted_set_remove(skey(self._group, 'events'), e.id)
                    else:
                        events.append(e)
                else:
                    events.append(e)

            objects = events

        return objects

    def __secure_results(self, objects):
        if self._model_class not in (User, EventAttendee, EventMessage, Message):
            return objects

        secure_user = self._secure
        private_friends = secure_user.get_private_friend_ids() if secure_user else None
        blocked = secure_user.get_blocked_ids() if secure_user else None

        def can_see_user(u):
            if u is None:
                return False
            if secure_user:
                if u == secure_user:
                    return True
                if u.status == 'hidden':
                    return False
                if u.id in blocked:
                    return False
                if u.privacy == 'public' or u.id in private_friends:
                    return True
                return False
            else:
                if u.status == 'hidden':
                    return False
                return True

        if self._model_class in (User, EventAttendee):
            objects = [u for u in objects if can_see_user(u)]
        elif self._model_class == EventMessage:
            objects = [m for m in objects if can_see_user(m.user)]
        elif self._model_class == Message:
            objects = [m for m in objects if can_see_user(m.user) and can_see_user(m.to_user)]

        return objects
