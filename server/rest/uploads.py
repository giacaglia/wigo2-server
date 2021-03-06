from __future__ import absolute_import

import logging
import os
import ujson

from urlparse import urlparse, urljoin
from uuid import uuid4
from boto.s3.connection import S3Connection
from flask.ext.restful import abort
from flask.ext.restplus import fields
from werkzeug.utils import secure_filename
from flask import g, request, jsonify
from config import Configuration
from server.models.event import EventMessage
from server.rest import api_blueprint, WigoResource, api
from server.security import user_token_required

logger = logging.getLogger('wigo.web.uploads')

param = api.model('UploadParam', {
    'key': fields.String(required=True),
    'value': fields.String(required=True)
})

params = api.model('UploadParams', {
    'action': fields.String(description='Form action', required=True),
    'fields': fields.Nested(param, as_list=True, required=True)
})


class UploadResource(WigoResource):
    @user_token_required
    def post(self):
        multipart_file = request.files.get('file')
        filename = secure_filename(multipart_file.filename)
        tmp_filepath = os.path.join(Configuration.UPLOAD_FOLDER, filename)
        multipart_file.save(tmp_filepath)

        cache_control = request.values.get('Cache-Control')
        content_type = request.values.get('Content-Type')
        key = request.values.get('key')
        logger.info('handling photo upload directly for key {key}'.format(key=key))

        try:
            s3conn = S3Connection(Configuration.UPLOADS_AWS_ACCESS_KEY_ID,
                                  Configuration.UPLOADS_AWS_SECRET_ACCESS_KEY)
            bucket = s3conn.get_bucket('wigo-uploads', validate=False)
            key = bucket.new_key(key)
            key.set_metadata('Content-Type', content_type)
            key.set_metadata('Cache-Control', cache_control)
            key.set_contents_from_filename(tmp_filepath)
            key.set_acl('public-read')
            return {'success': True}

        finally:
            if tmp_filepath and os.path.exists(tmp_filepath):
                os.unlink(tmp_filepath)


@api.route('/uploads/photos/')
class PhotoUploadResource(UploadResource):
    @user_token_required
    @api.doc(params={'filename': 'Optional filename'})
    @api.response(200, 'Success', model=params)
    def get(self):
        user = g.user
        filename = request.args.get('filename')
        if not filename:
            filename = 'photo.jpg'

        path_id = uuid4().hex
        photo_form_args = get_upload_location(user, 'image/jpeg', filename, path_id)
        if request.args.get('thumbnail') == 'true':
            thumb_form_args = get_upload_location(user, 'image/jpeg', 'thumbnail.jpg', path_id)
            return {'photo': photo_form_args, 'thumbnail': thumb_form_args}
        else:
            return photo_form_args


@api.route('/uploads/videos/')
class VideoUploadResource(UploadResource):
    @user_token_required
    @api.doc(params={'filename': 'Optional filename'})
    @api.response(200, 'Success', model=params)
    def get(self):
        user = g.user
        filename = request.args.get('filename')
        if not filename:
            filename = 'video.mp4'

        path_id = uuid4().hex
        video_form_args = get_upload_location(user, 'video/mp4', filename, path_id)
        image_form_args = get_upload_location(user, 'image/jpeg', 'image.jpg', path_id)

        return {'video': video_form_args, 'image': image_form_args}


@api_blueprint.route('/hooks/blitline/<key>/', methods=['POST'])
def on_thumbnail_upload(key):
    if key != Configuration.API_HOOK_KEY:
        abort(400, message='Invalid api key')

    data = request.data or request.form.get('results') or ''
    try:
        data = ujson.loads(data)
    except ValueError:
        logger.error('error parsing blitline callback data, {data}'.format(data=data))
        abort(400, message='Bad json data')

    images = data.get('images')
    errors = data.get('errors')

    if images:
        for image in images:
            type_and_id = ujson.loads(image.get('image_identifier'))
            s3_path = urlparse(image.get('s3_url')).path.replace('/wigo-uploads/', '')
            if type_and_id['type'] == 'EventMessage':
                message = EventMessage.find(type_and_id['id'])
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

    s3conn = S3Connection(Configuration.UPLOADS_AWS_ACCESS_KEY_ID,
                          Configuration.UPLOADS_AWS_SECRET_ACCESS_KEY)

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
