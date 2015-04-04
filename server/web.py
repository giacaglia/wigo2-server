from __future__ import absolute_import

import os
import ujson
import logging
from datetime import datetime
from urlparse import urlparse, urljoin
from uuid import uuid4
from boto.s3.connection import S3Connection
from clize import clize
from flask.ext.restful import abort
from flask.ext.restplus import apidoc
from schematics.exceptions import ModelValidationError
from werkzeug.utils import secure_filename

from config import Configuration
from flask import Flask, render_template, g, request, jsonify
from flask.ext import restplus
from flask.ext.admin import Admin
from flask.ext.compress import Compress
from flask.ext.security import Security, login_required
from flask.ext.wtf import CsrfProtect
import logconfig
from server import ApiSessionInterface
from server.admin import UserModelView, GroupModelView, ConfigView, NotificationView, MessageView, EventModelView
from server.db import wigo_db
from server.models.user import User, Notification, Message
from server.models.event import Event, EventMessage
from server.models.group import Group
from server.models import Config, DoesNotExist
from server.rest.login import setup_login_resources
from server.tasks.notifications import wire_notifications
from server.rest.register import setup_register_resources
from server.rest.user import setup_user_resources
from server.rest.event import setup_event_resources
from server.security import WigoDbUserDatastore, wigo_user_token_required
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

admin = Admin(app, name='Wigo')
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
            group = Group.find(lat=g.latitude, lon=g.longitude)
            if group:
                g.group = group


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
            abort(403, 'error verifying email')

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
        abort(400, 'JSON post data invalid')

    try:
        data = ujson.loads(data)
    except ValueError:
        abort(400, 'JSON post data invalid')

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


@app.route('/api/hooks/blitline/<key>/', methods=['POST'])
def on_thumbnail_upload(key):
    if key != app.config['API_HOOK_KEY']:
        abort(400, 'Invalid api key')

    data = request.data or request.form.get('results') or ''
    try:
        data = ujson.loads(data)
    except ValueError:
        logger.error('error parsing blitline callback data, {data}'.format(data=data))
        abort(400, 'Bad json data')

    images = data.get('images')
    errors = data.get('errors')

    if images:
        for image in images:
            type_and_id = ujson.loads(image.get('image_identifier'))
            s3_path = urlparse(image.get('s3_url')).path.replace('/wigo-uploads/', '')
            if type_and_id['type'] == 'EventMessage':
                message = EventMessage.get(type_and_id['id'])
                if type_and_id.get('thumbnail') is not False:
                    message.thumbnail = s3_path
                else:
                    message.media = s3_path
                message.save()

        return jsonify(success=True)
    else:
        failed_ids = data.get('failed_image_identifiers')
        logger.error('error creating thumbnail for {ids}, {error}'.format(ids=failed_ids, error=errors))
        return jsonify(success=False)


def get_upload_location(user, mime_type, filename, path_id=None):
    """
    Hits the Amazon API to generate an upload signature/policy for direct
    uploads to Amazon S3 from the browser. Returns a set of fields to be appended
    to an HTML form.
    """

    if path_id is None:
        path_id = uuid4().hex

    filename = filename.encode('utf8').replace(' ', '_')
    key = os.path.join(str(user.id % 100), str(user.id), path_id, filename)

    s3conn = S3Connection(app.config['WIGO_UPLOADS_AWS_ACCESS_KEY_ID'],
                          app.config['WIGO_UPLOADS_AWS_SECRET_ACCESS_KEY'])

    params = s3conn.build_post_form_args('wigo-uploads', key, expires_in=6000,
                                         acl='public-read', max_content_length=10485760,
                                         http_method='https',
                                         fields=[
                                             {'name': 'Content-Type', 'value': mime_type},
                                             {'name': 'Cache-Control', 'value': 'max-age=86400'}
                                         ],
                                         conditions=['["eq", "$Content-Type", "%s"]' % mime_type,
                                                     '["eq", "$Cache-Control", "max-age=86400"]'])

    if 'iOS 7' in request.user_agent.string or 'CFNetwork/672' in request.user_agent.string:
        params['action'] = urljoin(request.host_url, '/api/uploads/photos/')

    return params


@wigo_user_token_required
@app.route('/api/uploads/videos/', methods=['GET'])
def get_video_upload_location():
    user = g.user
    filename = request.args.get('filename')
    if not filename:
        filename = 'video.mp4'

    path_id = uuid4().hex
    video_form_args = get_upload_location(user, 'video/mp4', filename, path_id)
    thumbnail_form_args = get_upload_location(user, 'image/jpeg', 'thumbnail.jpg', path_id)

    return jsonify(video=video_form_args, thumbnail=thumbnail_form_args)


@wigo_user_token_required
@app.route('/api/uploads/photos/', methods=['GET'])
def get_photo_upload_location():
    user = g.user
    filename = request.args.get('filename')
    if not filename:
        filename = 'photo.jpg'

    path_id = uuid4().hex
    photo_form_args = get_upload_location(user, 'image/jpeg', filename, path_id)
    if request.args.get('thumbnail') == 'true':
        thumb_form_args = get_upload_location(user, 'image/jpeg', 'thumbnail.jpg', path_id)
        return jsonify(photo=photo_form_args, thumbnail=thumb_form_args)
    else:
        return jsonify(photo_form_args)


@wigo_user_token_required
@app.route('/api/uploads/photos/', methods=['POST'])
def upload_photo():
    multipart_file = request.files.get('file')
    filename = secure_filename(multipart_file.filename)
    tmp_filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    multipart_file.save(tmp_filepath)

    cache_control = request.values.get('Cache-Control')
    content_type = request.values.get('Content-Type')
    key = request.values.get('key')
    logger.info('handling photo upload directly for key {key}'.format(key=key))

    try:
        s3conn = S3Connection(app.config['WIGO_UPLOADS_AWS_ACCESS_KEY_ID'],
                              app.config['WIGO_UPLOADS_AWS_SECRET_ACCESS_KEY'])
        bucket = s3conn.get_bucket('wigo-uploads', validate=False)
        key = bucket.new_key(key)
        key.set_metadata('Content-Type', content_type)
        key.set_metadata('Cache-Control', cache_control)
        key.set_contents_from_filename(tmp_filepath)
        key.set_acl('public-read')
        return jsonify(success=True)

    finally:
        if tmp_filepath and os.path.exists(tmp_filepath):
            os.unlink(tmp_filepath)


@clize
def run_server():
    port = int(os.environ.get('WIGO_PORT', 5100))
    logger.info('starting wigo web server on port %s, pid=%s' % (port, os.getpid()))
    app.run(host='0.0.0.0', port=port, debug=True)


if __name__ == '__main__':
    import sys

    run_server(*sys.argv)
