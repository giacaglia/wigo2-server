from __future__ import absolute_import

import math

from collections import defaultdict
from flask import g, request, Blueprint, url_for
from flask.ext.restful import Resource, abort
from flask.ext import restplus
from pytz import UnknownTimeZoneError
from schematics.exceptions import ModelValidationError
from werkzeug.urls import url_encode
from server.models import AlreadyExistsException
from server.models.event import EventMessage, Event
from server.models.group import Group
from server.models.user import User
from server.security import user_token_required
from utils import ValidationException
from utils import SecurityException


api_blueprint = Blueprint('api', __name__, url_prefix='/api')


class WigoApi(restplus.Api):
    def __init__(self):
        # noinspection PyTypeChecker
        super(WigoApi, self).__init__(api_blueprint, ui=False, title='Wigo API', catch_all_404s=True)

    @property
    def specs_url(self):
        return url_for(self.endpoint('specs'))

    @property
    def base_url(self):
        return url_for(self.endpoint('root'))


api = WigoApi()


class WigoResource(Resource):
    model = None

    def select(self, model=None, fields=None):
        model = model if model else self.model
        query = self.setup_query(model.select(fields))
        if request.args.get('ordering') == 'asc':
            query = query.order('asc')
        return query

    def get_page(self):
        return int(request.args.get('page', 1))

    def get_limit(self):
        return int(request.args.get('limit', 15))

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
            alimit = int(request.args.get('attendees_limit', 5))
            mlimit = int(request.args.get('messages_limit', 5))

            context = g.user if '/users/' in request.path else None

            return Event.annotate_list(objects, context, alimit, mlimit)
        return objects

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

    def serialize_list(self, model_class, objects, count=None, next=None):
        objects = self.annotate_list(model_class, objects)

        data = {
            'meta': {},
            'objects'.format(model_class.__name__.lower()): [self.serialize_object(i) for i in objects if i]
        }

        def resolve_nested(objects, nested, resolved):
            nested_ids = defaultdict(set)
            for o in objects:
                if o is None:
                    continue
                if getattr(o, 'group_id', None):
                    nested_ids[Group].add(o.group_id)
                if getattr(o, 'user_id', None):
                    nested_ids[User].add(o.user_id)
                if getattr(o, 'to_user_id', None):
                    nested_ids[User].add(o.to_user_id)
                if getattr(o, 'from_user_id', None):
                    nested_ids[User].add(o.from_user_id)
                if getattr(o, 'event_id', None):
                    nested_ids[Event].add(o.event_id)
                if getattr(o, 'message_id', None):
                    nested_ids[EventMessage].add(o.message_id)
                if getattr(o, 'attendees', None):
                    nested_ids[User].update((u.id for u in o.attendees[1] if u))
                if getattr(o, 'messages', None):
                    nested_ids[EventMessage].update((m.id for m in o.messages[1] if m))

            if nested_ids:
                for nested_type, nested_ids in nested_ids.items():
                    to_lookup = nested_ids.difference(resolved)
                    if to_lookup:
                        nested_objects = nested_type.find(to_lookup)
                        nested_objects = self.annotate_list(nested_type, nested_objects)
                        nested.update(nested_objects)
                        resolved.update(to_lookup)
                        resolve_nested(nested_objects, nested, resolved)

        nested = set()
        resolve_nested(objects, nested, set([o.id for o in objects]))
        data['include'] = [self.serialize_object(i) for i in nested]

        request_arguments = request.args.copy().to_dict()

        if next is None:
            if count is None:
                count = len(objects)

            data['meta']['total'] = count

            page = self.get_page()
            limit = self.get_limit()
            pages = int(math.ceil(float(count) / limit))

            if page > 1:
                request_arguments['page'] = page - 1
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
        return self.serialize_list(self.model, [instance])

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
        count, instances = self.setup_query(self.model.select()).execute()
        return self.serialize_list(self.model, instances, count)

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
    return {'message': error.message}, 400


@api.errorhandler(ValidationException)
def handle_validation_exception(error):
    return {'message': error.message}, 400


@api.errorhandler(SecurityException)
def handle_security_exception(error):
    return {'message': error.message}, 403


@api.errorhandler(NotImplementedError)
def handle_not_implemented(error):
    return {'message': error.message}, 501


@api.errorhandler(UnknownTimeZoneError)
def handle_unknown_tz(error):
    return {'message': 'Unknown timezone'}, 400


import server.rest.register
import server.rest.login
import server.rest.group
import server.rest.user
import server.rest.event
import server.rest.uploads
import server.rest.recommendations
