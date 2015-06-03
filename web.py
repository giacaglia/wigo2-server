from __future__ import absolute_import

import logconfig
from config import Configuration

logconfig.configure(Configuration.ENVIRONMENT)

import os
import ujson
import logging
import requests
import click

from newrelic import agent
from datetime import datetime
from urlparse import urlparse
from flask.ext.restful import abort
from flask.ext.sslify import SSLify
from rq_dashboard import RQDashboard
from flask import Flask, render_template, g, request, jsonify
from flask.ext.admin import Admin
from flask.ext.compress import Compress
from flask.ext.restplus import apidoc

from server import ApiSessionInterface
from server.admin import UserModelView, GroupModelView, ConfigView, NotificationView, \
    MessageView, EventModelView, WigoAdminIndexView, EventMessageView
from server.rest import api_blueprint
from server.tasks.uploads import wire_uploads_listeners
from server.tasks.images import wire_images_listeners
from server.tasks.notifications import wire_notifications_listeners
from server.tasks.parse import wire_parse_listeners
from server.tasks.predictions import wire_predictions_listeners
from server.tasks.data import wire_data_listeners
from server.db import wigo_db
from server.models.user import User, Notification, Message
from server.models.event import Event, EventMessage
from server.models.group import Group
from server.models import Config, DoesNotExist
from server.security import check_basic_auth, setup_user_by_token

from utils import Version, ValidationException

requests.packages.urllib3.disable_warnings()

logger = logging.getLogger('wigo.web')

app = Flask(__name__, template_folder='server/templates')
app.url_map.strict_slashes = False
app.config.from_object(Configuration)
app.session_interface = ApiSessionInterface()

SSLify(app)
Compress(app)
RQDashboard(app, '/admin/rq', check_basic_auth)

app.register_blueprint(api_blueprint)

admin = Admin(app, name='Wigo', index_view=WigoAdminIndexView())
admin.add_view(GroupModelView(Group))
admin.add_view(UserModelView(User))
admin.add_view(EventModelView(Event))
admin.add_view(EventMessageView(EventMessage))
admin.add_view(MessageView(Message))
admin.add_view(NotificationView(Notification))
admin.add_view(ConfigView(Config))

wire_notifications_listeners()
wire_uploads_listeners()
wire_images_listeners()
wire_predictions_listeners()
wire_parse_listeners()
wire_data_listeners()


@app.before_request
def setup_request():
    g.user = None
    g.group = None

    if is_request_secure() and request.environ.get('wsgi.url_scheme') != 'https':
        request.environ['wsgi.url_scheme'] = 'https'

    if request.path.startswith('/api/swagger') or request.path.startswith('/admin'):
        agent.ignore_transaction()

    api_key = request.headers.get('X-Wigo-API-Key')
    if not api_key and 'key' in request.args:
        request.args = request.args.copy()
        api_key = request.args.pop('key')

    if api_key:
        g.api_key = api_key

    api_version = request.headers.get('X-Wigo-API-Version')
    if not api_version:
        api_version = '1000000000.0.0'

    try:
        g.api_version = Version(api_version)
    except:
        raise ValidationException('Invalid version number', 'X-Wigo-API-Version')

    # check api key auth
    if request.path.startswith('/api/hooks/'):
        # webhooks do their own auth
        pass
    elif request.path.startswith('/api/swagger'):
        pass
    elif request.path.startswith('/api') and api_key != app.config['API_KEY']:
        abort(403, message='Bad API key')

    # resolve by lat/long
    geolocation = request.headers.get('Geolocation')
    if geolocation:
        parsed_geo = urlparse(geolocation)
        if parsed_geo.scheme == 'geo':
            lat, lon = parsed_geo.path.split(',')
            lat, lon = float(lat), float(lon)
            if lat and lon:
                g.latitude, g.longitude = float(lat), float(lon)
                try:
                    g.group = Group.find(lat=g.latitude, lon=g.longitude)
                except DoesNotExist:
                    logger.info('could not resolve group from geo, lat={}, lon={}'.format(g.latitude, g.longitude))

    city_id = request.headers.get('X-Wigo-City-ID')
    if city_id:
        g.group = Group.find(city_id=int(city_id))

    group_id = request.headers.get('X-Wigo-Group-ID')
    if group_id:
        g.group = Group.find(int(group_id))

    # setup the user after the geo lookup, since the user might need to update its group
    setup_user_by_token()


@app.after_request
def after_request(response):
    if 'Cache-Control' not in response.headers:
        response.headers.add('Cache-Control', 'max-age=0, must-revalidate')
    if hasattr(g, 'user') and g.user:
        response.headers.add('X-Wigo-User-ID', g.user.id)
        response.headers.add('X-Wigo-User', g.user.username)
    if hasattr(g, 'group') and g.group:
        response.headers.add('X-Wigo-Group', g.group.code)
        response.headers.add('X-Wigo-Group-ID', g.group.id)
        response.headers.add('X-Wigo-City-ID', g.group.city_id)

    agent.end_of_transaction()

    return response


@app.route('/home')
def wigo_home():
    return render_template('index.html')


@app.route('/docs/', endpoint='docs')
def swagger_ui():
    from server.rest import api

    return apidoc.ui_for(api)


app.register_blueprint(apidoc.apidoc)


@app.route('/api/app/startup')
def app_startup():
    if request.args.get('source') == 'newrelic':
        agent.ignore_transaction()

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

    response = jsonify(startup)
    response.headers.add('Cache-Control', 'max-age=3600')
    return response


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
            user.save()

    return jsonify(success=True)


@app.route('/who', methods=['GET'])
def referrer_dashboard():
    # summary = self.analytics.get_leading_referrers()
    payouts = []

    referrer_ids = wigo_db.sorted_set_rrange('referrers', 0, 100, withscores=True)
    referrers = [u for u in User.find(r[0] for r in referrer_ids)]

    for index, r in enumerate(referrers):
        r.num_referrals = int(referrer_ids[index][1])

    return render_template('who_dashboard.html', referrers=referrers, payouts=payouts)


def is_request_secure():
    secure = True if request.is_secure else False
    forwarded_proto = request.headers.get('X-Forwarded-Proto')
    if forwarded_proto == 'https':
        secure = True
    return secure


@app.template_filter('strftime')
def strftime(value, format='%H:%M / %d-%m-%Y'):
    return value.strftime(format)


@click.command()
def run_server():
    port = int(os.environ.get('WIGO_PORT', 5100))
    logger.info('starting wigo web server on port %s, pid=%s' % (port, os.getpid()))
    app.run(host='0.0.0.0', port=port, debug=True)


if __name__ == '__main__':
    run_server()