from __future__ import absolute_import

import logging
import requests
import ujson
from requests.exceptions import Timeout
from config import Configuration
from simplejson import JSONDecodeError

logger = logging.getLogger('wigo.push')


def alert(data, where, enterprise=False, scheduled_datetime=None):
    packet = {'data': data, 'where': where}
    if scheduled_datetime:
        packet['push_time'] = scheduled_datetime.isoformat()
    return __send(packet, enterprise)


def __send(data, enterprise=False):
    result = None
    if Configuration.PUSH_ENABLED:
        result = __post(data, Configuration.PARSE_APPLICATION_ID, Configuration.PARSE_REST_KEY)
        if enterprise:
            result = __post(data, Configuration.PARSE_ENTERPRISE_APPLICATION_ID,
                          Configuration.PARSE_ENTERPRISE_REST_KEY)
        return result
    else:
        logger.info('NOT sending push, data={}'.format(data))
        return None


def __post(data, app_id, rest_key):
    try:
        resp = requests.post('https://api.parse.com/1/push', data=ujson.dumps(data), headers={
            'Content-Type': 'application/json',
            'X-Parse-Application-Id': app_id,
            'X-Parse-REST-API-Key': rest_key
        }, timeout=15)
    except Timeout:
        logger.warn('timed out attempting to send parse push notification')
        raise PushTimeoutException()
    if resp.status_code == 200:
        return resp.json()
    else:
        try:
            data = resp.json()
            raise PushException(data.get('error'), data.get('code'))
        except JSONDecodeError:
            raise PushException('error decoding push error from parse, '
                                'status=%s, text=%s' % (resp.status_code, resp.text))


class PushException(Exception):
    def __init__(self, message, code=None):
        super(PushException, self).__init__(message)
        self.code = code


class PushTimeoutException(PushException):
    def __init__(self):
        super(PushTimeoutException, self).__init__('Timeout', 'timeout')