from __future__ import absolute_import

import logging
import urllib
import ujson
import requests

from rq.decorators import job

from server.tasks import redis_queues, parse_queue
from server.models import post_model_save
from server.models.user import User
from config import Configuration

logger = logging.getLogger('wigo.parse')


@job(parse_queue, timeout=30, result_ttl=0)
def sync_parse(self, user_id, data=None):
    user = User.find(user_id)

    if not data:
        data = {
            'group_id': user.group_id,
            'group_name': user.group.name,
            'group_locked': user.group.locked,
            'gender': user.gender,
            'status': user.status,
            'privacy': user.privacy,
        }

        if user.work:
            data['work'] = user.work

        if user.education:
            data['education'] = user.education


    headers = {
        'Content-Type': 'application/json',
        'X-Parse-Application-Id': Configuration.PARSE_APPLICATION_ID,
        'X-Parse-Master-Key': Configuration.PARSE_MASTER_REST_KEY
    }

    params = urllib.urlencode({'where': ujson.dumps({'wigo_id': user.id})})

    installations = requests.get('https://api.parse.com/1/installations?%s' % params,
                                 headers=headers, timeout=20).json()

    results = installations.get('results')
    if isinstance(results, list):
        log_suffix = 'for user %s' % user.email

        for installation in results:
            # if the values are exactly the same, skip
            if {k: installation.get(k) for k in data.keys()} == data:
                continue

            installation_id = installation.get('objectId')
            resp = requests.put('https://api.parse.com/1/installations/%s' % installation_id,
                                ujson.dumps(data), headers=headers)

            if resp.status_code != 200:
                error = resp.json()
                logger.warn('error updating parse fields %s, %s' % (log_suffix, str(error)))
                resp.raise_for_status()


def wire_parse_listeners():
    if Configuration.ENVIRONMENT != 'production':
        return

    def sync_parse_listener(sender, instance, created):
        if isinstance(instance, User):
            sync_parse.delay(user_id=instance.id)

    post_model_save.connect(sync_parse_listener, weak=False)


