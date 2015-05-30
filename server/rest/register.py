from __future__ import absolute_import

import logging
import pytz
import re

from datetime import datetime
from datetime import timedelta
from dateutil.parser import parse
from random import randint
from uuid import uuid4
from flask import g, request
from flask.ext.restful import abort
from flask.ext.restplus import fields
from config import Configuration
from server.db import redis

from server.models import DoesNotExist
from server.models.user import User
from server.rest import WigoResource, api
from server.services.facebook import Facebook, FacebookTimeoutException
from server.tasks.email import send_email_verification

logger = logging.getLogger('wigo.web')


@api.route('/register')
class RegisterResource(WigoResource):
    @api.expect(api.model('RegisterUser', {
        'facebook_id': fields.String,
        'facebook_access_token': fields.String,
        'facebook_access_token_expires': fields.Integer,
        'email': fields.String,
        'timezone': fields.String
    }))
    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    @api.response(400, 'Bad post data, or account already exists')
    def post(self):
        data = request.get_json()

        if not data:
            abort(400, message='No data posted')

        timezone = pytz.timezone(data.get('timezone', 'US/Eastern'))
        facebook_id = data.get('facebook_id')
        facebook_token = data.get('facebook_access_token')
        facebook_token_expires = datetime.utcnow() + timedelta(
            seconds=data.get('facebook_access_token_expires') or 1728000)
        birthdate = data.get('birthdate')
        education = data.get('education')
        work = data.get('work')

        properties = data.get('properties')

        logger.info('attempting to register user for facebook_id {}'.format(facebook_id))

        if not facebook_id or not facebook_token:
            abort(400, message='Missing facebook id or token')

        with redis.lock('locks:register:{}'.format(facebook_id), timeout=30):
            try:
                User.find(facebook_id=facebook_id)
                abort(400, message='Account already exists')
            except DoesNotExist:
                pass

            user_info = self.get_me(facebook_id, facebook_token, facebook_token_expires)

            logger.info('creating new user account for facebook_id {}'.format(facebook_id))

            user = User()
            user.key = uuid4().hex
            user.facebook_id = facebook_id
            user.facebook_token = facebook_token
            user.facebook_token_expires = facebook_token_expires

            user.email = user_info.get('email')
            if user.email:
                user.email_validated = True
                user.email_validated_date = datetime.utcnow()
                user.email_validated_status = 'validated'

            user.timezone = timezone.zone
            user.first_name = user_info.get('first_name') or user_info.get('given_name')
            user.last_name = user_info.get('last_name') or user_info.get('family_name')
            user.username = get_username(user.email or user.full_name)
            user.gender = user_info.get('gender')

            if not birthdate:
                birthdate = user_info.get('birthday')

            if birthdate:
                try:
                    user.birthdate = parse(birthdate)
                except:
                    logger.info('error parsing birthdate {}'.format(birthdate))

            if education:
                user.education = education

            if work:
                user.work = work

            if g.group:
                user.group_id = g.group.id

            if properties:
                for key, value in properties.items():
                    user.set_custom_property(key, value)

            user.set_custom_property('events', {'triggers': ['find_referrer']})

            platform = request.headers.get('X-Wigo-Device')
            if not platform:
                platform = request.user_agent.platform
            if platform:
                platform = platform.lower()

            if platform in ('android', 'iphone', 'ipad'):
                user.set_custom_property('platforms', [platform])

            enterprise = request.headers.get('X-Wigo-Client-Enterprise')
            if enterprise == 'true':
                user.enterprise = True

            user.save()

        g.user = user

        # if not user.email_validated and Configuration.PUSH_ENABLED:
        #     send_email_verification.delay(user.id)

        logger.info('registered new account for user "%s"' % user.email)

        return self.serialize_list(User, [user])

    def get_me(self, facebook_id, facebook_token, facebook_token_expires):
        # fetch user information from facebook
        if not facebook_id.startswith('xxx'):
            facebook = Facebook(facebook_token, facebook_token_expires)

            def get_me():
                fb_user_info = facebook.get('me')
                if fb_user_info.get('id') != facebook_id:
                    abort(403, message='Facebook token user id does not match passed in user id')
                return fb_user_info

            try:
                return get_me()
            except FacebookTimeoutException:
                logger.warn('register timed out waiting for facebook response, '
                            'trying one more time, facebook_id {}'.format(facebook_id))
                try:
                    return get_me()
                except FacebookTimeoutException:
                    logger.error('AGAIN register timed out waiting for facebook response, '
                                 'aborting, facebook_id {}'.format(facebook_id))
                    raise
        else:
            return {}


def get_username(str_value):
    if '@' in str_value:
        (username, host) = str_value.split('@')
    else:
        username = str_value

    username = re.sub(r'[^A-Za-z0-9_\.\-]+', '', username)
    username = username.lower()

    # make sure the username is unique
    num_checks = 0
    while num_checks < 100:
        try:
            User.find(username=username)
            username = "%s%s" % (username, randint(0, 10000))
            num_checks += 1
        except DoesNotExist:
            break

    if num_checks >= 100:
        raise ValueError('Username couldnt be created for user')

    return username
