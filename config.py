from __future__ import absolute_import

import os
from datetime import timedelta


def parse_env_list(prefix):
    index = 1
    values = []
    while True:
        value = os.environ.get('{}_{}'.format(prefix, index), None)
        if value:
            values.append(value)
            index += 1
        else:
            break

    return values


class Configuration(object):
    ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')
    DEBUG = True if ENVIRONMENT == 'dev' else False
    SECRET_KEY = 'bb034599jkrtbg30ijwerrgjvn'
    PREFERRED_URL_SCHEME = 'http' if ENVIRONMENT == 'dev' else 'https'

    WEB_HOST = os.environ.get('WEB_HOST', 'localhost:5100')
    API_HOST = os.environ.get('API_HOST', WEB_HOST.replace('verify', 'api'))

    PERMANENT_SESSION_LIFETIME = timedelta(days=365)
    SESSION_COOKIE_HTTPONLY=True
    SESSION_COOKIE_SECURE=True

    SEND_FILE_MAX_AGE_DEFAULT = 60 * 60 * 24
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024

    ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'test')
    DEV_ADMIN_PASSWORD = os.environ.get('DEV_ADMIN_PASSWORD', 'vv309409r4jd')

    API_KEY = os.environ.get('WIGO_API_KEY', 'oi34u53205ju34ik23')
    API_HOOK_KEY = os.environ.get('WIGO_API_HOOK_KEY', '9tejvdoikkf')

    REDIS_URL = os.environ.get('REDIS_URL', os.environ.get('REDISCLOUD_URL', 'redis://localhost:6379'))
    REDIS_URLS = parse_env_list('REDIS_URL')
    if not REDIS_URLS:
        REDIS_URLS.append(REDIS_URL)

    DATABASE_URL = os.environ.get('DATABASE_URL', 'postgres://wigo:4090ejsAdff3@localhost/wigo')
    OLD_DATABASE_URL = os.environ.get('OLD_DATABASE_URL', DATABASE_URL)
    RDBMS_REPLICATE = os.environ.get('RDBMS_REPLICATE', 'true' if ENVIRONMENT != 'test' else 'false') == 'true'

    MAIL_SERVER = 'smtp.sendgrid.net'
    MAIL_USERNAME = os.environ.get('SENDGRID_USERNAME', None)
    MAIL_PASSWORD = os.environ.get('SENDGRID_PASSWORD', None)
    DEFAULT_MAIL_SENDER = 'support@wigo.us'
    MAIL_DEBUG = False

    FACEBOOK_APP_ID = os.environ.get('FACEBOOK_APP_ID', None)
    FACEBOOK_APP_SECRET = os.environ.get('FACEBOOK_APP_SECRET', None)
    FACEBOOK_APP_NAMESPACE = os.environ.get('FACEBOOK_APP_NAMESPACE', 'wigo')

    IMAGE_CDN = os.environ.get('IMAGE_CDN', 'dh72np710c78e.cloudfront.net')
    IMAGES_BUCKET = os.environ.get('IMAGES_BUCKET', 'wigo-images')
    IMAGES_AWS_ACCESS_KEY_ID = os.environ.get('IMAGES_AWS_ACCESS_KEY_ID', '')
    IMAGES_AWS_SECRET_ACCESS_KEY = os.environ.get('IMAGES_AWS_SECRET_ACCESS_KEY', '')

    UPLOADS_AWS_ACCESS_KEY_ID = os.environ.get('UPLOADS_AWS_ACCESS_KEY_ID', '')
    UPLOADS_AWS_SECRET_ACCESS_KEY = os.environ.get('UPLOADS_AWS_SECRET_ACCESS_KEY', '')
    UPLOADS_CDN = os.environ.get('UPLOADS_CDN', 'd9vw4owxyhc4n.cloudfront.net')

    TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
    TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
    TWILIO_FROM = os.environ.get('TWILIO_FROM', '+16173074688')

    PARSE_APPLICATION_ID = os.environ.get('PARSE_APPLICATION_ID', None)
    PARSE_REST_KEY = os.environ.get('PARSE_REST_KEY', None)
    PARSE_MASTER_REST_KEY = os.environ.get('PARSE_MASTER_REST_KEY', None)
    PARSE_ENTERPRISE_APPLICATION_ID = os.environ.get('PARSE_ENTERPRISE_APPLICATION_ID', None)
    PARSE_ENTERPRISE_REST_KEY = os.environ.get('PARSE_ENTERPRISE_REST_KEY', None)

    BLITLINE_APPLICATION_ID = os.environ.get('BLITLINE_APPLICATION_ID', '')

    PAPERTRAIL_API_TOKEN = os.environ.get('PAPERTRAIL_API_TOKEN', 'BG2MGQF1cinTAHhRZiFe')
    PUSH_ENABLED = os.environ.get('PUSH_ENABLED', 'true') == 'true'

    CAPTURE_IMAGES = True

    PREDICTION_IO_APP_ID = int(os.environ.get('PREDICTION_IO_APP_ID', '1'))
    PREDICTION_IO_ACCESS_KEY = os.environ.get('PREDICTION_IO_ACCESS_KEY', '8Jp9T1nKYOmCmWqQpFV5ziLlkzesWOCgjgnelFOiAqgKwtoV0KPQG646EWfbkz1T')
    PREDICTION_IO_HOST = os.environ.get('PREDICTION_IO_HOST', '104.197.21.254')
    PREDICTION_IO_PORT = os.environ.get('PREDICTION_IO_PORT', '8001')

    UPLOAD_FOLDER = 'uploads'

    if not os.path.exists(UPLOAD_FOLDER):
        try:
            os.mkdir(UPLOAD_FOLDER)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
            if not os.path.isdir(UPLOAD_FOLDER):
                os.unlink(UPLOAD_FOLDER)
                os.mkdir(UPLOAD_FOLDER)
