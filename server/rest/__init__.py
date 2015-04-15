from __future__ import absolute_import

from __future__ import absolute_import
from collections import defaultdict

import math
from flask import g, request
from flask.ext.restful import Resource, abort
from werkzeug.urls import url_encode
from server.models import AlreadyExistsException
from server.models.event import EventMessage, Event
from server.models.group import Group
from server.models.user import User
from server.security import user_token_required


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

    def get_limit(self):
        return int(request.args.get('limit', 15))

    def setup_query(self, query):
        return query.page(self.get_page()).limit(self.get_limit())

    def get_id(self, user_id):
        return g.user.id if user_id == 'me' else int(user_id)

    def check_get(self, instance):
        pass

    def clean_data(self, data):
        data = dict(data)

        if 'created' in data:
            del data['created']
        if 'modified' in data:
            del data['modified']

        # remove blacklisted fields
        role = self.model._options.roles.get('www')
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
        data = self.clean_data(data)
        for key, value in data.items():
            setattr(instance, key, value)

        for key in data.keys():
            if '_id' in key:
                del data[key]

        instance.save()
        return instance

    def create(self, data):
        self.check_create(data)
        data = self.clean_data(data)
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
            mlimit = int(request.args.get('message_limit', 5))
            return Event.annotate_list(objects, alimit, mlimit)
        return objects

    def serialize_object(self, obj):
        prim = obj.to_primitive(role='www')
        if hasattr(obj, 'attendees'):
            count = obj.attendees[0]
            attendees = obj.attendees[1]
            prim['attendees'] = {
                'meta': {
                    'total': count,
                },
                'users': [{'$ref': 'User:{}'.format(u.id)} for u in attendees]
            }
            if count > len(attendees):
                prim['attendees']['next'] = '/api/events/{}/attendees?' \
                                            'page=2&limit={}'.format(obj.id, request.args.get('attendees_limit', 5))

        return prim

    def serialize_list(self, model_class, objects, count=None):
        objects = self.annotate_list(model_class, objects)

        page = self.get_page()
        limit = self.get_limit()
        request_arguments = request.args.copy()
        pages = int(math.ceil(float(count) / limit))

        data = {
            'meta': {
                'total': count,
            },
            'objects'.format(model_class.__name__.lower()): [self.serialize_object(i) for i in objects]
        }

        def resolve_nested(objects, nested, resolved):
            nested_ids = defaultdict(set)
            for o in objects:
                if getattr(o, 'group_id', None):
                    nested_ids[Group].add(o.group_id)
                if getattr(o, 'user_id', None):
                    nested_ids[User].add(o.user_id)
                if getattr(o, 'event_id', None):
                    nested_ids[Event].add(o.event_id)
                if getattr(o, 'message_id', None):
                    nested_ids[EventMessage].add(o.message_id)
                if getattr(o, 'attendees', None):
                    nested_ids[User].update((u.id for u in o.attendees[1]))

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

        if page > 1:
            request_arguments['page'] = page - 1
            data['meta']['previous'] = '%s?%s' % (request.path, url_encode(request_arguments))
        if page < pages:
            request_arguments['page'] = page + 1
            data['meta']['next'] = '%s?%s' % (request.path, url_encode(request_arguments))

        return data


class WigoDbResource(WigoResource):
    @user_token_required
    def get(self, model_id):
        instance = self.model.find(self.get_id(model_id))
        self.check_get(instance)
        return self.serialize_list(self.model, [instance], 1)

    @user_token_required
    def post(self, model_id):
        data = request.get_json()
        instance = self.edit(model_id, data)
        return self.serialize_list(self.model, [instance], 1)

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
            return self.serialize_list(self.model, [instance], 1)
        except AlreadyExistsException, e:
            return self.serialize_list(self.model, [e.instance], 1)
