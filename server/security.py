from __future__ import absolute_import
from functools import wraps
from flask import request, g, Response
from flask.ext.restful import abort

from config import Configuration
from server.models import DoesNotExist
from server.models.user import User, Role
from server.models.group import Group


def user_token_required(fn):
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


def check_auth(username, password):
    return username == 'admin' and password == Configuration.ADMIN_PASSWORD


def check_basic_auth():
    auth = request.authorization
    if not auth or not check_auth(auth.username, auth.password):
        return False
    return True


def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not check_basic_auth():
            return authenticate()
        return f(*args, **kwargs)
    return decorated