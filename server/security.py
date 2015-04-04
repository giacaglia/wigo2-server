from __future__ import absolute_import
from functools import wraps
from flask import request, g
from flask.ext.restful import abort

from flask.ext.security.datastore import Datastore, UserDatastore
from flask.ext.security.decorators import _get_unauthorized_response
from werkzeug.local import LocalProxy
from server.db import wigo_db
from server.models import DoesNotExist
from server.models.user import User, Role
from server.models.group import Group


def wigo_user_token_required(fn):
    @wraps(fn)
    def decorated(*args, **kwargs):
        if g.user is None:
            g.user = None

            user_token = request.headers.get('X-Wigo-User-Key')
            try:
                g.user = User.find(key=user_token)
                if g.group:
                    if g.user.group_id != g.group.id:
                        g.user.group_id = g.group.id
                        g.user.save()
                elif g.user.group_id:
                    g.group = Group.find(g.user.group_id)
            except DoesNotExist:
                abort(404, message='No user found for key')

        if g.user:
            return fn(*args, **kwargs)
        else:
            return _get_unauthorized_response()

    return decorated


class WigoDbDatastore(Datastore):
    def put(self, model):
        if isinstance(model, LocalProxy):
            model = model._get_current_object()
        self.db.save(model)
        return model

    def delete(self, model):
        if isinstance(model, LocalProxy):
            model = model._get_current_object()
        self.db.delete(model)


class WigoDbUserDatastore(WigoDbDatastore, UserDatastore):
    def __init__(self):
        WigoDbDatastore.__init__(self, wigo_db)
        UserDatastore.__init__(self, User, Role)

    def find_user(self, **kwargs):
        id = kwargs.get('id')
        username = kwargs.get('username')
        key = kwargs.get('key')
        user = User.find(id) if id else None
        if not user:
            user = User.find(username=username, key=key)
        return user

    def get_user(self, identifier):
        return User.find(username=identifier, key=identifier)

    def find_role(self, role):
        return Role(role)

    def create_user(self, **kwargs):
        """Creates and returns a new user from the given parameters."""
        roles = kwargs.pop('roles', [])
        prepared = self._prepare_create_user_args(**kwargs)
        prepared['role'] = roles[0]
        del prepared['roles']
        user = self.user_model(prepared)
        user = self.put(user)
        return user

    def add_role_to_user(self, user, role):
        user.role = role
        self.put(user)
        return True

    def remove_role_from_user(self, user, role):
        user.role = 'user'
        self.put(user)
        return True