from __future__ import absolute_import

import math
import logging

from time import time
from datetime import datetime
from functools import wraps
from collections import defaultdict
from flask import g, request, Blueprint, url_for, current_app
from flask.ext.restful import Resource, abort
from flask.ext import restplus
from newrelic import agent
from pytz import UnknownTimeZoneError
from repoze.lru import CacheMaker
from schematics.exceptions import ModelValidationError
from werkzeug.http import is_resource_modified, http_date
from werkzeug.urls import url_encode
from server import NotModifiedException
from server.db import wigo_db
from server.models import AlreadyExistsException, skey
from server.models.event import EventMessage, Event, EventAttendee, get_cached_num_attending, get_num_attending
from server.models.group import Group
from server.models.user import User
from server.security import user_token_required
from utils import ValidationException, partition, epoch
from utils import SecurityException


logger = logging.getLogger('wigo.web')

cache_maker = CacheMaker(maxsize=1000, timeout=60)
api_blueprint = Blueprint('api', __name__, url_prefix='/api')


class WigoApi(restplus.Api):
    def __init__(self):
        super(WigoApi, self).__init__(api_blueprint, ui=False, title='Wigo API', catch_all_404s=True)

    def handle_error(self, e):
        if isinstance(e, NotModifiedException):
            agent.ignore_transaction()
            response = current_app.response_class(status=304)
            response.headers.add('Cache-Control', 'max-age=%s' % e.ttl)
            return response
        else:
            return super(WigoApi, self).handle_error(e)


    @property
    def specs_url(self):
        return url_for(self.endpoint('specs'))

    @property
    def base_url(self):
        return url_for(self.endpoint('root'))


api = WigoApi()


class WigoResource(Resource):
    model = None

    def select(self, model=None):
        model = model if model else self.model
        query = self.setup_query(model.select())
        if request.args.get('ordering') == 'asc':
            query = query.order('asc')
        return query

    def get_page(self):
        return int(request.args.get('page', 1))

    def get_limit(self, default=15):
        return int(request.args.get('limit', default))

    def setup_query(self, query):
        return query.page(self.get_page()).limit(self.get_limit())

    def get_id(self, id_value):
        return g.user.id if id_value == 'me' else int(id_value)

    def get_id_field(self, field):
        value = request.get_json().get(field)
        if not value:
            abort(400, message='Field {} is required'.format(field))
        return self.get_id(value)

    def check_get(self, instance):
        pass

    def clean_data(self, data, mode='edit'):
        data = dict(data)

        if 'id' in data:
            del data['id']
        if 'created' in data:
            del data['created']
        if 'modified' in data:
            del data['modified']

        # remove blacklisted fields
        role = None
        for name in ('www-{}'.format(mode), 'www-edit', 'www'):
            role = self.model._options.roles.get(name)
            if role:
                break

        if role:
            for field in role:
                if field in data:
                    del data[field]

        return data

    def check_edit(self, instance):
        if hasattr(instance, 'user_id') and g.user.id != instance.user_id:
            abort(403, message='Security error')
        if hasattr(instance, 'owner_id') and g.user.id != instance.owner_id:
            abort(403, message='Security error')

    def check_create(self, data):
        pass

    def edit(self, model_id, data):
        data = dict(data)

        instance = self.model.find(self.get_id(model_id))
        self.check_edit(instance)

        # can't change created/modified
        data = self.clean_data(data, 'edit')
        for key, value in data.items():
            setattr(instance, key, value)

        instance.save()
        return instance

    def create(self, data):
        self.check_create(data)
        data = self.clean_data(data, 'create')
        instance = self.model(data)

        if 'group_id' in self.model.fields and g.group:
            instance.group_id = g.group.id
        if 'user_id' in self.model.fields and g.user:
            instance.user_id = g.user.id
        if 'owner_id' in self.model.fields and g.user:
            instance.owner_id = g.user.id

        instance.save()
        return instance

    def annotate_object(self, object):
        return object

    def annotate_list(self, model_class, objects):
        if model_class == Event:
            return self.annotate_events(objects)
        return objects

    def annotate_events(self, events):
        alimit = int(request.args.get('attendees_limit', 5))
        mlimit = int(request.args.get('messages_limit', 5))

        current_user = g.user
        user_context = current_user if '/users/' in request.path else None

        for event in events:
            if event.is_expired:
                event.num_attending = get_cached_num_attending(event.id, user_context.id if user_context else None)
            else:
                event.num_attending = get_num_attending(event.id, user_context.id if user_context else None)

        # fill in attending on each event
        query = EventAttendee.select().events(events).user(user_context).secure(g.user)
        count, page, attendees_by_event = query.limit(alimit).execute()
        if count:
            for event, attendees in zip(events, attendees_by_event):
                # make sure the event the current user is attending is in front
                if hasattr(event, 'current_user_attending'):
                    count, attendees = attendees
                    if attendees[0] != g.user:
                        if g.user in attendees:
                            attendees.remove(g.user)
                        attendees.insert(0, g.user)
                    attendees = (count, attendees)

                event.attendees = attendees

        def capture_messages(events, query):
            count, page, messages_by_event = query.limit(mlimit).execute()
            if count:
                for event, messages in zip(events, messages_by_event):
                    event.messages = messages

        expired, current = partition(events, lambda e: e.is_expired)
        base_query = EventMessage.select().user(user_context).secure(g.user)
        capture_messages(current, base_query.events(current))
        capture_messages(expired, base_query.events(expired).by_votes())

        filtered = []
        for event in events:
            if event.is_expired:
                if event.messages and len(event.messages[1]) > 1:
                    filtered.append(event)
            else:
                filtered.append(event)

        return filtered

    def serialize_object(self, obj):
        prim = obj.to_primitive(role='www')

        if isinstance(obj, User) and obj != g.user and User.key.name in prim:
            del prim[User.key.name]

        if hasattr(obj, 'num_attending'):
            prim['num_attending'] = obj.num_attending

        if hasattr(obj, 'invited'):
            prim['is_invited'] = obj.invited

        if hasattr(obj, 'attendees'):
            count = obj.attendees[0]
            attendees = obj.attendees[1]
            prim['attendees'] = {
                'meta': {
                    'total': count,
                },
                'objects': [{'$ref': 'User:{}'.format(u.id)} for u in attendees if u]
            }
            if count > len(attendees):
                path = ('/api/users/me/events/{}/attendees' if '/users/' in request.path
                        else '/api/events/{}/attendees').format(obj.id)
                prim['attendees']['meta']['next'] = '{}?page=2&limit={}'.format(path,
                                                                                request.args.get('attendees_limit', 5))

        if hasattr(obj, 'messages'):
            count = obj.messages[0]
            messages = obj.messages[1]
            prim['messages'] = {
                'meta': {
                    'total': count,
                },
                'objects': [{'$ref': 'EventMessage:{}'.format(m.id)} for m in messages if m]
            }
            if count > len(messages):
                path = ('/api/users/me/events/{}/messages' if '/users/' in request.path
                        else '/api/events/{}/messages').format(obj.id)
                prim['messages']['meta']['next'] = '{}?page=2&limit={}'.format(path,
                                                                               request.args.get('messages_limit', 5))

        return prim

    def serialize_list(self, model_class, objects, count=None, page=1, next=None):
        objects = self.annotate_list(model_class, objects)

        data = {
            'meta': {},
            'objects'.format(model_class.__name__.lower()): [self.serialize_object(i) for i in objects if i]
        }

        def resolve_nested(objects, nested, resolved):
            nested_ids = defaultdict(set)

            def collect_nested(o, value_class, id_field):
                id_value = getattr(o, id_field, None)
                if id_value and id_value not in resolved:
                    cached = o._field_cache.get((id_field, id_value))
                    if cached:
                        resolved.add(id_value)
                        nested.add(cached)
                        resolve_nested([cached], nested, resolved)
                    else:
                        nested_ids[value_class].add(id_value)

            for o in objects:
                if o is None:
                    continue

                if getattr(o, 'group_id', None):
                    collect_nested(o, Group, 'group_id')
                if getattr(o, 'user_id', None):
                    collect_nested(o, User, 'user_id')
                if getattr(o, 'to_user_id', None):
                    collect_nested(o, User, 'to_user_id')
                if getattr(o, 'from_user_id', None):
                    collect_nested(o, User, 'from_user_id')
                if getattr(o, 'event_id', None):
                    collect_nested(o, Event, 'event_id')
                if getattr(o, 'message_id', None):
                    collect_nested(o, EventMessage, 'message_id')
                if getattr(o, 'attendees', None):
                    users = [u for u in o.attendees[1] if u and u.id not in resolved]
                    resolved.update((u.id for u in users))
                    nested.update(users)
                    resolve_nested(users, nested, resolved)
                if getattr(o, 'messages', None):
                    messages = [m for m in o.messages[1] if m and m.id not in resolved]
                    resolved.update((m.id for m in messages))
                    nested.update(messages)
                    resolve_nested(messages, nested, resolved)

            if nested_ids:
                for nested_type, nested_ids in nested_ids.items():
                    if nested_ids:
                        nested_objects = nested_type.find(nested_ids)
                        nested_objects = self.annotate_list(nested_type, nested_objects)
                        nested.update(nested_objects)
                        resolved.update(nested_ids)
                        resolve_nested(nested_objects, nested, resolved)

        nested = set()
        resolve_nested(objects, nested, set([o.id for o in objects]))
        data['include'] = [self.serialize_object(i) for i in nested]

        request_arguments = request.args.copy().to_dict()

        if next is None:
            if count is None:
                count = len(objects)

            data['meta']['total'] = count

            limit = self.get_limit()
            pages = int(math.ceil(float(count) / limit))

            # starting page can be diff from page if the query was filtered
            starting_page = self.get_page()

            if starting_page > 1:
                request_arguments['page'] = starting_page - 1
                data['meta']['previous'] = '%s?%s' % (request.path, url_encode(request_arguments))
            if page < pages:
                request_arguments['page'] = page + 1
                data['meta']['next'] = '%s?%s' % (request.path, url_encode(request_arguments))
        elif next:
            request_arguments.update(next)
            data['meta']['next'] = '%s?%s' % (request.path, url_encode(request_arguments))

        return data


class WigoDbResource(WigoResource):
    @user_token_required
    def get(self, model_id):
        instance = self.model.find(self.get_id(model_id))
        self.check_get(instance)
        return self.serialize_list(self.model, [instance]), 200, {
            'Last-Modified': http_date(instance.modified)
        }

    @user_token_required
    def post(self, model_id):
        data = request.get_json()
        instance = self.edit(model_id, data)
        return self.serialize_list(self.model, [instance])

    @user_token_required
    def delete(self, model_id):
        instance = self.model.find(self.get_id(model_id))
        self.check_edit(instance)
        instance.delete()
        return {'success': True}


class WigoDbListResource(WigoResource):
    @user_token_required
    def get(self):
        count, page, instances = self.setup_query(self.model.select()).execute()
        return self.serialize_list(self.model, instances, count, page)

    @user_token_required
    def post(self):
        try:
            instance = self.create(request.get_json())
            return self.serialize_list(self.model, [instance])
        except AlreadyExistsException, e:
            return self.handle_already_exists_exception(e)

    def handle_already_exists_exception(self, e):
        return self.serialize_list(self.model, [e.instance])


@api.errorhandler(ModelValidationError)
def handle_model_validation_error(error):
    logger.warn('validation error {}'.format(error.message))
    return {'message': error.message}, 400


@api.errorhandler(ValidationException)
def handle_validation_exception(error):
    logger.warn('validation error {}'.format(error.message))
    return {'message': error.message}, 400


@api.errorhandler(SecurityException)
def handle_security_exception(error):
    logger.error('security error {}'.format(error.message))
    return {'message': error.message}, 403


@api.errorhandler(NotImplementedError)
def handle_not_implemented(error):
    return {'message': error.message}, 501


@api.errorhandler(UnknownTimeZoneError)
def handle_unknown_tz(error):
    logger.warn('validation error {}'.format(error.message))
    return {'message': 'Unknown timezone'}, 400


def check_last_modified(field=None, max_age=0):
    def inner(f):
        @wraps(f)
        def decorated(*args, **kw):
            last_change = wigo_db.redis.hget(skey(g.user, 'meta'), field)
            if last_change:
                last_change = datetime.utcfromtimestamp(float(last_change))
            else:
                last_change = datetime.utcnow()
                wigo_db.redis.hset(skey(g.user, 'meta'), field, time())

            headers = {'Last-Modified': http_date(last_change or datetime.utcnow())}

            if max_age:
                headers['Cache-Control'] = 'max-age={}'.format(max_age)

            if last_change and not is_resource_modified(request.environ, last_modified=last_change):
                return 'Not modified', 304, headers

            kw['headers'] = headers
            return f(*args, **kw)

        return decorated

    return inner



import server.rest.register
import server.rest.login
import server.rest.group
import server.rest.user
import server.rest.event
import server.rest.uploads
import server.rest.recommendations
