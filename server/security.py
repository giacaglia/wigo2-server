from __future__ import absolute_import

import logging
from functools import wraps
from flask import request, g, Response
from flask.ext.restful import abort
from datetime import datetime, timedelta
from newrelic import agent

from config import Configuration
from server.models import DoesNotExist, model_cache
from server.models.user import User, get_user_id_for_key, user_lock
from server.models.group import Group

logger = logging.getLogger('wigo.security')


def user_token_required(fn):
    @wraps(fn)
    def decorated(*args, **kwargs):
        user = getattr(g, 'user', None)
        if user is None:
            setup_user_by_token()
            user = getattr(g, 'user', None)
        if user:
            if request.method in ('POST', 'PUT', 'DELETE'):
                with user_lock(user.id, timeout=15):
                    return fn(*args, **kwargs)
            else:
                return fn(*args, **kwargs)
        else:
            abort(403, message='Unauthorized')

    return decorated


def setup_user_by_token():
    user_token = request.headers.get('X-Wigo-User-Key')
    if user_token:
        try:
            user_id = get_user_id_for_key(user_token)

            # the current user should always get a fresh copy of themself
            model_cache.invalidate(user_id)

            user = User.find(user_id)
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
                if user.modified <= (datetime.utcnow() - timedelta(minutes=30)):
                    user.latitude = round(g.latitude, 3)
                    user.longitude = round(g.longitude, 3)
            else:
                if not user.latitude:
                    user.latitude = group.latitude
                if not user.longitude:
                    user.longitude = group.longitude

            platform = request.headers.get('X-Wigo-Device')
            if not platform:
                platform = request.user_agent.platform
            if platform:
                platform = platform.lower()

            if platform in ('android', 'iphone', 'ipad'):
                user.set_custom_property('platforms', [platform])

            if user.is_changed():
                user.save()

            agent.add_custom_parameter('user_id', user.id)
            if user.group_id:
                agent.add_custom_parameter('group_code', group.code)

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