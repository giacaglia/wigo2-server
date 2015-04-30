from __future__ import absolute_import

from functools import wraps
from flask import request, g, Response
from flask.ext.restful import abort

from config import Configuration
from server.models import DoesNotExist
from server.models.user import User
from server.models.group import Group


def user_token_required(fn):
    @wraps(fn)
    def decorated(*args, **kwargs):
        user = getattr(g, 'user', None)
        if user is None:
            setup_user_by_token()
            user = getattr(g, 'user', None)
        if user:
            return fn(*args, **kwargs)
        else:
            abort(403, message='Unauthorized')

    return decorated


def setup_user_by_token():
    user_token = request.headers.get('X-Wigo-User-Key')
    if user_token:
        try:
            g.user = User.find(key=user_token)
            existing_group = getattr(g, 'group', None)
            if existing_group:
                g.user.group_id = existing_group.id
            elif g.user.group_id:
                g.group = Group.find(g.user.group_id)

            if hasattr(g, 'latitude') and hasattr(g, 'longitude'):
                g.user.latitude = g.latitude
                g.user.longitude = g.longitude

            if g.user.is_changed():
                g.user.save()

        except DoesNotExist:
            pass


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