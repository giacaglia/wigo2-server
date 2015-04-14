from __future__ import absolute_import

import os
import ujson
import logging
from datetime import datetime
from urlparse import urlparse
from clize import clize
from flask.ext.restful import abort
from flask.ext.restplus import apidoc
from flask.ext.security.utils import encrypt_password
from schematics.exceptions import ModelValidationError

from config import Configuration
from flask import Flask, render_template, g, request, jsonify
from flask.ext import restplus
from flask.ext.admin import Admin
from flask.ext.compress import Compress
from flask.ext.security import Security, login_required
from flask.ext.wtf import CsrfProtect
import logconfig
from server import ApiSessionInterface
from server.admin import UserModelView, GroupModelView, ConfigView, NotificationView, MessageView, EventModelView, \
    WigoAdminIndexView
from server.db import wigo_db
from server.models.user import User, Notification, Message
from server.models.event import Event
from server.models.group import Group
from server.models import Config, DoesNotExist
from server.rest.login import setup_login_resources
from server.tasks.notifications import wire_notifications
from server.rest.register import setup_register_resources
from server.rest.user import setup_user_resources
from server.rest.event import setup_event_resources
from server.security import WigoDbUserDatastore
from utils import ValidationException, SecurityException


logconfig.configure(Configuration.ENVIRONMENT)

logger = logging.getLogger('wigo.web')

app = Flask(__name__)
app.url_map.strict_slashes = False
app.session_interface = ApiSessionInterface()
app.config.from_object(Configuration)

Compress(app)
csrf_protect = CsrfProtect(app)

user_datastore = WigoDbUserDatastore()
security = Security(app, user_datastore)

api = restplus.Api(
    app, ui=False, title='Wigo API', catch_all_404s=True,
    decorators=[csrf_protect.exempt],
    errors={
        'UnknownTimeZoneError': {
            'message': 'Unknown timezone', 'status': 400
        }
    })

setup_login_resources(api)
setup_register_resources(api)
setup_user_resources(api)
setup_event_resources(api)

admin = Admin(app, name='Wigo', index_view=WigoAdminIndexView())
admin.add_view(UserModelView(User))
admin.add_view(GroupModelView(Group))
admin.add_view(EventModelView(Event))
admin.add_view(MessageView(Message))
admin.add_view(NotificationView(Notification))
admin.add_view(ConfigView(Config))

wire_notifications()


@app.route('/docs/', endpoint='docs')
def swagger_ui():
    return apidoc.ui_for(api)


app.register_blueprint(apidoc.apidoc)


@app.route('/')
@login_required
def home():
    return render_template('index.html')


@app.before_first_request
def setup_web_app():
    try:
        user = User.find(username='admin')
        user.role = 'admin'
        user.password = encrypt_password(Configuration.ADMIN_PASSWORD)
        user.save()
    except DoesNotExist:
        User({
            'username': 'admin',
            'password': encrypt_password(Configuration.ADMIN_PASSWORD),
            'email': 'jason@wigo.us',
            'role': 'admin'
        }).save()


@app.before_request
def setup_request():
    g.user = None
    g.group = None

    api_key = request.headers.get('X-Wigo-API-Key')
    if not api_key and 'key' in request.args:
        request.args = request.args.copy()
        api_key = request.args.pop('key')

    if api_key:
        g.api_key = api_key

    if request.path.startswith('/api/hooks/'):
        # webhooks do their own auth
        pass
    elif request.path.startswith('/api') and api_key != app.config['API_KEY']:
        abort(403, message='Bad API key')

    geolocation = request.headers.get('Geolocation')
    if geolocation:
        parsed_geo = urlparse(geolocation)
        if parsed_geo.scheme == 'geo':
            lat, long = parsed_geo.path.split(',')
            g.latitude, g.longitude = float(lat), float(long)
            try:
                group = Group.find(lat=g.latitude, lon=g.longitude)
                if group:
                    g.group = group
            except DoesNotExist:
                logger.info('could not resolve group from geo')


@app.route('/api/app/startup')
def app_startup():
    startup = {
        'analytics': {
            'bigquery': True
        },
        'provisioning': {
            'video': False,
        },
        'uploads': {
            'image': {
                'quality': 0.8,
                'multiple': 1.0
            }
        },
        'cdn': {
            'uploads': Configuration.UPLOADS_CDN
        }
    }

    return jsonify(startup)


@app.route('/c/<code>')
def resolve_code(code):
    data = wigo_db.get_code(code)
    if data['type'] == 'verify_email':
        user_id = data['user_id']
        email = data['email']
        user = User.find(user_id)

        if email != user.email:
            logger.info('email being verified does not match email stored in code, '
                        'email="{}", user email="{}"'.format(email, user.email))
            abort(403, message='error verifying email')

        user.email_validated = True
        user.email_validated_date = datetime.utcnow()
        user.email_validated_status = 'validated'
        user.save()

        g.user = user

        logger.info('verified email for user "{}"'.format(user.email))
        return render_template('email_verified.html', user=user)


@app.route('/api/hooks/sendgrid/update', methods=['POST'])
def sendgrid_hook():
    data = request.get_data() or request.form.get('data') or ''
    if not data:
        abort(400, message='JSON post data invalid')

    try:
        data = ujson.loads(data)
    except ValueError:
        abort(400, message='JSON post data invalid')

    for record in data:
        event = record.get('event')
        user = None
        user_id = record.get('user_id')

        if user_id:
            try:
                user = User.find(user_id)
            except DoesNotExist:
                pass

        if not user:
            try:
                user = User.find(email=record.get('email'))
            except DoesNotExist:
                pass

        # only update the user if not validated already, since if they are validated
        # the email_validated_status will be set
        if user and not user.email_validated:
            event = record.get('event')
            existing_event = user.email_validated_status

            # make sure events progress forward, don't allow "delivered"->"processed"
            sg_evt_ord = ['processed', 'dropped', 'deferred', 'delivered',
                          'bounce', 'open', 'click', 'unsubscribe', 'spamreport']

            if event in sg_evt_ord and existing_event in sg_evt_ord and \
                            sg_evt_ord.index(existing_event) >= sg_evt_ord.index(event):
                continue

            logger.info('updating email validation status for user "%s", %s' % (user.email, event))
            user.email_validated_status = event
            user.modified = datetime.datetime.utcnow()
            user.save()

    return jsonify(success=True)


@api.errorhandler(ModelValidationError)
def handle_model_validation_error(error):
    return error.message, 400


@api.errorhandler(ValidationException)
def handle_validation_exception(error):
    return error.message, 400


@api.errorhandler(SecurityException)
def handle_security_exception(error):
    return error.message, 403


@api.errorhandler(NotImplementedError)
def handle_not_implemented(error):
    return error.message, 501


@clize
def run_server():
    port = int(os.environ.get('WIGO_PORT', 5100))
    logger.info('starting wigo web server on port %s, pid=%s' % (port, os.getpid()))
    app.run(host='0.0.0.0', port=port, debug=True)


if __name__ == '__main__':
    import sys

    run_server(*sys.argv)
