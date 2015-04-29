from __future__ import absolute_import

import logging
import os
import ujson
import requests
from urlparse import urlparse
from rq.decorators import job
from config import Configuration
from server.tasks import redis_queues
from server.models import post_model_save
from server.models.event import EventMessage

logger = logging.getLogger('wigo.uploads')


@job('images', connection=redis_queues, timeout=30, result_ttl=0)
def process_eventmessage_image(message_id):
    if not Configuration.BLITLINE_APPLICATION_ID:
        logger.warning('blitline not configured, ignoring thumbnail request')
        return

    message = EventMessage.find(message_id)
    path = os.path.split(urlparse(message.media).path)[0]

    function = {
        'name': 'resize_to_fit',
        'params': {
            'width': 150
        },
        'save': {
            'image_identifier': ujson.dumps({
                'type': 'EventMessage',
                'id': message.id,
                'thumbnail': True
            }),
            's3_destination': {
                'bucket': 'wigo-uploads', 'key': path + '/' + str(message.id) + '-thumb.jpg',
                'headers': {
                    'Content-Type': 'image/jpeg',
                    'Cache-Control': 'max-age=86400'
                }
            }
        }
    }

    # the hook is defined in uploads.py
    job_data = {
        'application_id': Configuration.BLITLINE_APPLICATION_ID,
        'src': 'https://' + Configuration.UPLOADS_CDN + '/' + message.media,
        'postback_url': 'https://' + Configuration.API_HOST +
                        '/api/hooks/blitline/' + Configuration.API_HOOK_KEY + '/',
        'functions': [function]
    }

    resp = requests.post('http://api.blitline.com/job', data={
        'json': ujson.dumps(job_data)
    }, timeout=10)

    if resp.status_code == 200:
        json = resp.json()
        if 'error' in json:
            logger.error('error creating blitline thumbnail job for '
                         'event message {id}, {error}'.format(id=message.id, error=json.get('error')))
    else:
        logger.error('error creating blitline thumbnail job '
                     'for event message {id}, {error}'.format(id=message.id, error=resp.content))


def wire_uploads_listeners():
    def uploads_model_listener(sender, instance, created):
        if created and isinstance(instance, EventMessage):
            if instance.media_mime_type == 'image/jpeg' and not instance.thumbnail:
                process_eventmessage_image.delay(instance.id)

    post_model_save.connect(uploads_model_listener, weak=False)
