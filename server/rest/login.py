from __future__ import absolute_import

import logging

from datetime import datetime
from datetime import timedelta
from flask import request, g
from flask.ext.restful import abort
from flask.ext.restplus import fields
from dateutil.parser import parse

from server.models.user import User
from server.rest import WigoResource, api
from server.services.facebook import Facebook, FacebookTimeoutException, FacebookTokenExpiredException

logger = logging.getLogger('wigo.facebook')

@api.route('/login')
class LoginResource(WigoResource):
    @api.expect(api.model('LoginUser', {
        'facebook_id': fields.String,
        'facebook_access_token': fields.String,
        'facebook_access_token_expires': fields.Integer,
        'email': fields.String
    }))
    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    @api.response(400, 'Bad post data, or account already exists')
    @api.response(403, 'Security error')
    def post(self):
        data = request.get_json()

        facebook_id = data.get('facebook_id')
        facebook_token = data.get('facebook_access_token')
        facebook_token_expires = datetime.utcnow() + timedelta(
            seconds=data.get('facebook_access_token_expires') or 1728000)

        birthdate = data.get('birthdate')
        education = data.get('education')
        work = data.get('work')

        properties = data.get('properties')

        if not facebook_id or not facebook_token:
            abort(400, message='Missing facebook id or token')

        user = User.find(facebook_id=facebook_id)

        if user.facebook_token != facebook_token:
            # hit the facebook api. if this fails, the token is invalid
            try:
                facebook = Facebook(facebook_token, facebook_token_expires)
                fb_user_info = facebook.get('me')
                if fb_user_info.get('id') != facebook_id:
                    abort(403, message='Facebook token user id does not match passed in user id')
                facebook_token_expires = facebook.get_token_expiration()
            except FacebookTimeoutException, e:
                logger.error('timeout validating facebook token for user "%s"' % user.email)
                raise
            except FacebookTokenExpiredException, e:
                logger.warning('access token expired for user "%s"' % user.email)
                abort(400, message='Facebook token expired')
            except Exception, e:
                logger.error('error validating facebook token for user "%s", %s' % (user.email, e.message))
                raise

            user.facebook_token = facebook_token
            user.facebook_token_expires = facebook_token_expires

        if birthdate:
            try:
                user.birthdate = parse(birthdate)
            except:
                logger.info('error parsing birthdate {}'.format(birthdate))

        if education:
            user.education = education

        if work:
            user.work = work

        if user.status == 'imported':
            user.status = 'active'

        if properties:
            for key, value in properties.items():
                if user.get_custom_property(key) != value:
                    user.set_custom_property(key, value)

        if user.is_changed():
            user.save()

        g.user = user

        return self.serialize_list(User, [user])

