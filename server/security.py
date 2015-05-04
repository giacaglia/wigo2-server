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
            user = User.find(key=user_token)
            g.user = user

            group = getattr(g, 'group', None)

            if group and not user.location_locked:
                # if a group was passed in via geo, switch the users group
                user.group_id = group.id
            elif user.group_id:
                # if the user has a group defined, use it
                group = Group.find(user.group_id)
                g.group = group
            else:
                # we need a group, so default to boston
                group = Group.find(code='boston')
                user.group_id = group.id
                g.group = group

            if not user.location_locked and hasattr(g, 'latitude') and hasattr(g, 'longitude'):
                user.latitude = g.latitude
                user.longitude = g.longitude
            else:
                if not user.latitude:
                    user.latitude = group.latitude
                if not user.longitude:
                    user.longitude = group.longitude

            if user.is_changed():
                user.save()

        except DoesNotExist:
            pass


def check_auth(username, password):
    return username == 'admin' and password in (Configuration.ADMIN_PASSWORD, Configuration.DEV_ADMIN_PASSWORD)


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