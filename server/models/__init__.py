from __future__ import absolute_import
import logging

import ujson

from blinker import signal
from datetime import datetime, timedelta

from flask.ext.restplus import fields as docfields
import re

from schematics.models import Model
from schematics.transforms import blacklist
from schematics.types import BaseType, StringType, DateTimeType, LongType, FloatType, NumberType, BooleanType
from schematics.types.serializable import serializable
from utils import dotget, epoch, memoize

logger = logging.getLogger('wigo.model')

INDEX_FIELD = re.compile('\{(.*?)\}', re.I)
DEFAULT_EXPIRING_TTL = timedelta(days=10)


class JsonType(BaseType):
    def _mock(self, context=None):
        return {}

    def to_native(self, value, context=None):
        return value

    def to_primitive(self, value, context=None):
        return value


class WigoModel(Model):
    indexes = ()

    created = DateTimeType(default=datetime.utcnow)
    modified = DateTimeType(default=datetime.utcnow)

    class Options:
        roles = {'www': blacklist('group_id', 'user_id', 'owner_id')}
        serialize_when_none = False

    def __init__(self, raw_data=None, deserialize_mapping=None, strict=False):
        self._changes = {}
        super(WigoModel, self).__init__(raw_data, deserialize_mapping, strict)

    @property
    def db(self):
        from server.db import wigo_db
        return wigo_db

    @classmethod
    def select(self, fields=None):
        from server.query import SelectQuery
        return SelectQuery(self, fields)

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

    def is_changed(self, *keys):
        return any(k for k in keys if k in self._changes.keys())

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
        user_id = getattr(self, 'user_id', None)
        if user_id:
            from server.models.user import User

            return User.find(user_id)
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

    @group.setter
    def group(self, group):
        self.group_id = group.id

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
                instances = [cls(result) if result is not None else None for result in results]
                for instance in instances:
                    if instance is not None:
                        instance.prepared()
                return instances
            else:
                return []
        elif model_id:
            result = wigo_db.get(skey(cls, int(model_id)))
            if result:
                instance = cls(result)
                instance.prepared()
                return instance
            raise DoesNotExist(cls, model_id)

        # search the indexes
        if kwargs:
            for kwarg in kwargs:
                applicable_indexes = [key_tmpl for key_tmpl, unique in cls.indexes if kwarg in key_tmpl]
                for key_tmpl in applicable_indexes:
                    key = index_key(key_tmpl, {kwarg: kwargs.get(kwarg)})
                    model_ids = wigo_db.sorted_set_range(key, 0, -1)
                    if model_ids:
                        return cls.find(model_ids[0])

            raise DoesNotExist()

        return None

    def save(self):
        self.validate(strict=True)

        self.modified = datetime.utcnow()

        created = self.id is None if hasattr(self, 'id') else True
        pre_model_save.send(self, instance=self)

        # save id'd objects
        try:
            if hasattr(self, 'id'):
                self.check_id()

            # check indexes to make sure there are no integrity issues
            self.check_indexes()

            # index
            self.index()

            # save
            if hasattr(self, 'id'):
                self.db.set(skey(self), self.to_primitive(), self.ttl())

            self.prepared()
            post_model_save.send(self, instance=self, created=created)
            return self

        except:
            # only cleanup if this was a new record that threw an exception
            if created:
                try:
                    self.delete()  # clean up on failure
                except:
                    logger.exception('error cleaning up')

            raise

    def __each_index(self):
        # process indexes
        for key_template, unique in self.indexes:
            key = None
            fields = get_index_fields(key_template)
            id_field = get_id_field(key_template)
            id_value = getattr(self, id_field, None)
            if id_value:
                # check if the old index entry needs to be removed
                if fields and self.is_changed(fields):
                    old_values = {f: (self.get_old_value(f) if self.is_changed(f)
                                      else getattr(self, f, None)) for f in fields}
                    self.db.sorted_set_remove(index_key(key_template, old_values), id_value)

                # record the new index entry
                if fields:
                    values = {f: getattr(self, f, None) for f in fields}
                    key = index_key(key_template, values)
                else:
                    key = key_template

                if key:
                    yield key, id_value, unique

    def get_index_score(self):
        return epoch(self.created)

    def check_indexes(self):
        for key, id_value, unique in self.__each_index():
            if (unique and self.db.get_sorted_set_size(key) > 0 and
                    not self.db.sorted_set_is_member(key, id_value)):
                raise IntegrityException('Unique contraint violation, key={}'.format(key))

    def index(self):
        for key, id_value, unique in self.__each_index():
            self.db.sorted_set_add(key, id_value, self.get_index_score())
            self.clean_old(key)

    def delete(self):
        if hasattr(self, 'id'):
            self.db.delete(skey(self))

        self.remove_index()
        return self

    def remove_index(self):
        for key, id_value, unique in self.__each_index():
            self.db.sorted_set_remove(key, id_value)

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
        model = api.models.get(cls.__name__)
        if model:
            return model
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

    @classmethod
    def to_doc_list_model(cls, api):
        model_name = '{}List'.format(cls.__name__)
        model = api.models.get(model_name)
        if model:
            return model

        meta = api.models.get('Meta')
        if not meta:
            meta = api.model('Meta', {
                'total': docfields.Integer,
                'previous': docfields.Url(required=False),
                'next': docfields.Url(required=False),
            })

        return api.model('{}List'.format(cls.__name__), {
            'meta': docfields.Nested(meta),
            'objects': docfields.Nested(cls.to_doc_model(api), as_list=True),
            'include': docfields.Nested(WigoModel.to_doc_model(api), as_list=True),
        })


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
            key_list.append('true')
        elif key is False:
            key_list.append('false')
        elif key is None:
            key_list.append('null')
        elif isinstance(key, Model):
            key_list.append(key.__class__.__name__.lower())
            key_list.append(str(key.id))
        elif isinstance(key, type):
            key_list.append(key.__name__.lower())
        else:
            key_list.append(str(key))

    key_str = ''
    if len(key_list) >= 2:
        key_str = '{' + key_list[0] + ':' + key_list[1] + '}'
        key_list = key_list[2:]

    if key_list:
        joined_keys = ':'.join(key_list)
        return (key_str + ':' + joined_keys) if key_str else joined_keys
    else:
        return key_str


def get_index_fields(key):
    return INDEX_FIELD.findall(key.split('=')[0])


def get_id_field(key):
    split = key.split('=')
    if len(split) == 2:
        m = INDEX_FIELD.search(split[1])
        if m:
            return m.group(1)
    return 'id'


def index_key(key_template, values):
    split = key_template.split('=')
    key = split[0].format(**values)
    return skey(*key.split(':'))


pre_model_save = signal('pre_model_save')
post_model_save = signal('post_model_save')
pre_model_delete = signal('pre_model_delete')
post_model_delete = signal('post_model_delete')
