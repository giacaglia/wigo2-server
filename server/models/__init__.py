from __future__ import absolute_import
import logging

import ujson

from blinker import signal
from pytz import timezone, UTC
from datetime import datetime, tzinfo, timedelta

from flask.ext.restplus import fields as docfields

from schematics.models import Model
from schematics.transforms import blacklist
from schematics.types import BaseType, StringType, DateTimeType, LongType, FloatType, NumberType, BooleanType
from schematics.types.serializable import serializable
from utils import dotget, epoch, memoize

logger = logging.getLogger('wigo.model')


class JsonType(BaseType):
    def _mock(self, context=None):
        return {}

    def to_native(self, value, context=None):
        return value

    def to_primitive(self, value, context=None):
        return value


class WigoModel(Model):
    indexes = ()
    unique_indexes = ()

    created = DateTimeType(default=datetime.utcnow)
    modified = DateTimeType(default=datetime.utcnow)

    class Options:
        roles = {'www': blacklist('group_id', 'user_id', 'owner_id', 'event_id')}
        serialize_when_none = False

    def __init__(self, raw_data=None, deserialize_mapping=None, strict=False):
        self._changes = {}
        super(WigoModel, self).__init__(raw_data, deserialize_mapping, strict)

    @property
    def db(self):
        from server.db import wigo_db
        return wigo_db

    @classmethod
    def select(self):
        from server.query import SelectQuery
        return SelectQuery(self)

    def ttl(self):
        return None

    def check_id(self):
        if self.id is None:
            self.id = self.db.gen_id()
        return object

    @serializable(serialized_name='$type')
    def type_for_ref(self):
        return self.__class__.__name__

    def prepared(self):
        self._changes.clear()

    def is_changed(self, key):
        return key in self._changes

    def get_old_value(self, key):
        if self.is_changed(key):
            return self._changes.get(key)[0]
        return None

    def __setattr__(self, key, value):
        is_field = key in self.__class__.fields
        is_already_dirty = key in self._changes if is_field else False
        old_value = self._data.get(key) if is_field else None
        val = super(WigoModel, self).__setattr__(key, value)
        if is_field and not is_already_dirty:
            new_value = self._data.get(key)
            if new_value != old_value:
                self._changes[key] = (old_value, new_value)
        return val

    def ref_field(self, type, field):
        if getattr(self, field, None):
            return {'$ref': '{}:{}'.format(type.__name__, getattr(self, field))}
        return None

    @property
    @memoize('user_id')
    def user(self):
        if self.user_id:
            from server.models.user import User

            return User.find(self.user_id)
        return None

    @serializable(serialized_name='user', serialize_when_none=False)
    def user_ref(self):
        from server.models.user import User

        return self.ref_field(User, 'user_id')

    @property
    @memoize('owner_id')
    def owner(self):
        if self.owner_id:
            from server.models.user import User

            return User.find(self.owner_id)
        return None

    @serializable(serialized_name='owner', serialize_when_none=False)
    def owner_ref(self):
        from server.models.user import User
        return self.ref_field(User, 'owner_id')

    @property
    @memoize('group_id')
    def group(self):
        if self.group_id:
            from server.models.group import Group

            return Group.find(self.group_id)
        return None

    @serializable(serialized_name='group', serialize_when_none=False)
    def group_ref(self):
        from server.models.group import Group
        return self.ref_field(Group, 'group_id')

    @property
    @memoize('event_id')
    def event(self):
        if self.event_id:
            from server.models.event import Event

            return Event.find(self.event_id)
        return None

    @serializable(serialized_name='event', serialize_when_none=False)
    def event_ref(self):
        from server.models.event import Event
        return self.ref_field(Event, 'event_id')

    def set_custom_property(self, name, value):
        """ Utility method for setting a value in the .properties map. """
        if not self.properties:
            self.properties = {}
        prev_value = self.properties.get(name)
        if prev_value != value:
            self.properties[name] = value

    def get_custom_property(self, name, default_value=None):
        """ Utility method for getting a value in the .properties map. """
        if self.properties:
            if '.' in name:
                return dotget(self.properties, name, default_value)
            else:
                value = self.properties.get(name)
                if value is not None:
                    return value
        return default_value

    @classmethod
    def find(cls, *args, **kwargs):
        from server.db import wigo_db

        model_id = args[0] if args else kwargs.get('id')
        model_ids = model_id if hasattr(model_id, '__iter__') else kwargs.get('ids')

        if hasattr(model_ids, '__iter__'):
            if model_ids:
                results = wigo_db.mget([skey(cls, int(model_id)) for model_id in model_ids])
                instances = [cls(ujson.loads(result)) if result is not None else None for result in results]
                for instance in instances:
                    if instance is not None:
                        instance.prepared()
                return instances
            else:
                return []
        elif model_id:
            result = wigo_db.get(skey(cls, int(model_id)))
            if result:
                instance = cls(ujson.loads(result))
                instance.prepared()
                return instance
            raise DoesNotExist(cls, model_id)

        # search the unique indexes
        if kwargs:
            for kwarg in kwargs:
                if kwarg in cls.unique_indexes:
                    model_id = wigo_db.get(skey('index', cls, kwarg, kwargs.get(kwarg)))
                    if model_id:
                        return cls.find(model_id)

            raise DoesNotExist()

        return None

    def save(self):
        self.validate(strict=True)
        created = self.id is None if hasattr(self, 'id') else True
        pre_model_save.send(self, instance=self)

        # save id'd objects
        if hasattr(self, 'id'):
            self.check_id()
            self.db.set(skey(self), self.to_json(), self.ttl())
            self.db.sorted_set_add(skey(self.__class__), self.id, epoch(self.created))
            self.clean_old(skey(self.__class__))

        try:
            self.index()
            post_model_save.send(self, instance=self, created=created)
            return self
        except:
            try:
                self.delete()  # clean up on failure
            except:
                logger.exception('error cleaning up')

            raise

    def index(self):
        if hasattr(self, 'id'):
            # process indexes
            for name, field in self.indexes:
                if self.is_changed(field):
                    old_value = self.get_old_value(field)
                    if old_value:
                        self.db.sorted_set_remove(skey('index', name, old_value), self.id)

                value = getattr(self, field, None)
                if value is not None:
                    self.db.sorted_set_add(skey('index', name, value), self.id, epoch(self.created))
                    self.clean_old(skey('index', name, value))

            # process unique indexes
            for field in self.__class__.unique_indexes:
                if self.is_changed(field):
                    old_value = self.get_old_value(field)
                    old_key = skey('index', self.__class__, field, old_value)
                    old_indexed = self.db.get(old_key)
                    if old_indexed and int(old_indexed) == self.id:
                        self.db.delete(skey('index', self.__class__, field, old_value))

                value = getattr(self, field, None)
                if value is not None:
                    k = skey('index', self.__class__, field, value)
                    existing = self.db.get(k)
                    if existing and int(existing) != self.id:
                        raise IntegrityException('Unique contraint violation, field={}'.format(field))
                    self.db.set(k, self.id, self.ttl())

    def delete(self):
        if hasattr(self, 'id'):
            self.db.delete(skey(self))
            self.db.sorted_set_remove(skey(self.__class__), self.id)

        self.remove_index()
        return self

    def remove_index(self):
        # process indexes
        for name, field in self.indexes:
            value = getattr(self, field, None)
            if value:
                try:
                    self.db.sorted_set_remove(skey('index', name, value), self.id)
                except:
                    pass

        # process unique indexes
        for field in self.__class__.unique_indexes:
            value = getattr(self, field, None)
            if value:
                self.db.delete(skey('index', self.__class__, field, value))

    def clean_old(self, key, ttl=None):
        if ttl is None:
            ttl = self.ttl()
        if isinstance(ttl, timedelta):
            up_to = datetime.utcnow() - ttl
            self.db.sorted_set_remove_by_score(key, 0, epoch(up_to))

    def to_json(self, role=None):
        return ujson.dumps(self.to_primitive(role=role))

    @classmethod
    def to_doc_model(cls, api):
        fields = {}
        for field in cls.fields.values():
            if field.name in ('created', 'modified', 'id'):
                continue
            if isinstance(field, DateTimeType):
                fields[field.name] = docfields.DateTime
            elif isinstance(field, FloatType):
                fields[field.name] = docfields.Float
            elif isinstance(field, NumberType):
                fields[field.name] = docfields.Integer
            elif isinstance(field, StringType):
                fields[field.name] = docfields.String
            elif isinstance(field, BooleanType):
                fields[field.name] = docfields.Boolean
            elif isinstance(field, JsonType):
                fields[field.name] = docfields.Arbitrary
        return api.model(cls.__name__, fields)


class WigoPersistentModel(WigoModel):
    id = LongType()

    @serializable(serialized_name='$id')
    def id_for_ref(self):
        if self.id:
            return '{}:{}'.format(self.__class__.__name__, self.id)
        return None

    def __cmp__(self, other):
        return cmp(self.id, other.id)

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id


class Dated(WigoModel):
    date = DateTimeType(required=True)
    expires = DateTimeType(required=True)

    def ttl(self):
        return timedelta(days=8)

    def save(self):
        if not self.date and not self.expires and self.group:
            self.date, self.expires = self.get_dates(self.group.timezone)
        super(Dated, self).save()

    @serializable
    def is_expired(self):
        return datetime.utcnow() > self.expires

    @classmethod
    def get_expires(cls, tz_arg, current=None):
        date, expires = Dated.get_dates(tz_arg, current)
        return expires

    @classmethod
    def get_today(cls, tz_arg):
        return Dated.get_expires(tz_arg) - timedelta(days=1)

    @classmethod
    def get_dates(cls, tz_arg, current=None):
        if current is None:
            if not tz_arg:
                tz = timezone('US/Eastern')
            elif isinstance(tz_arg, basestring):
                tz = timezone(tz_arg)
            elif isinstance(tz_arg, tzinfo):
                tz = tz_arg
            else:
                raise ValueError('Invalid timezone')

            # get current time in the correct timezone
            current = datetime.now(tz)

        current = current.replace(minute=0, second=0, microsecond=0)

        # if it is < 6am, the date will be 0am, and the expires will be 6am the SAME day
        # if it is > 6am, the date will be 6am, and the expires will be 6am the NEXT day
        if current.hour < 6:
            date = current.replace(hour=0).astimezone(UTC).replace(tzinfo=None)
            expires = current.replace(hour=6).astimezone(UTC).replace(tzinfo=None)
        else:
            date = current.replace(hour=6).astimezone(UTC).replace(tzinfo=None)
            next_day = current + timedelta(days=1)
            expires = next_day.replace(hour=6).astimezone(UTC).replace(tzinfo=None)

        return date, expires


class Config(WigoPersistentModel):
    group_id = LongType()
    name = StringType()
    properties = JsonType()


class WigoModelException(Exception):
    pass


class IntegrityException(WigoModelException):
    pass


class DoesNotExist(WigoModelException):
    def __init__(self, model_class=None, model_id=None):
        super(DoesNotExist, self).__init__('Does not exist')
        self.code = 404
        self.model_class = model_class
        self.model_id = model_id


class AlreadyExistsException(WigoModelException):
    def __init__(self, instance):
        super(AlreadyExistsException, self).__init__('Already exists')
        self.instance = instance


def get_score_key(d, score):
    return int(epoch(d)) + (score / 10000.0)


def user_attendees_key(user, event):
    return skey(user, event, 'attendees')


def user_eventmessages_key(user, event):
    return skey(user, event, 'messages')


def skey(*keys):
    """ Constructs a storage key. """
    key_list = []
    for key in keys:
        if key is True:
            key = 'true'
        elif key is False:
            key = 'false'
        elif key is None:
            key = 'null'
        elif isinstance(key, Model):
            key = '{}:{}'.format(key.__class__.__name__.lower(), str(key.id))
        elif isinstance(key, type):
            key = key.__name__.lower()
        else:
            key = str(key)
        key_list.append(key)
    return ':'.join(key_list)


pre_model_save = signal('pre_model_save')
post_model_save = signal('post_model_save')
pre_model_delete = signal('pre_model_delete')
post_model_delete = signal('post_model_delete')
