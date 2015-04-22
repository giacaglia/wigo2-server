from __future__ import absolute_import

import os


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
    PREFERRED_URL_SCHEME = 'http' if ENVIRONMENT == 'dev' else 'https'
    SECRET_KEY = 'bb034599jkrtbg30ijwerrgjvn'

    WEB_HOST = os.environ.get('WEB_HOST', 'localhost:5100')
    API_HOST = os.environ.get('API_HOST', WEB_HOST.replace('verify', 'api'))
    SERVER_NAME = os.environ.get('SERVER_NAME', API_HOST)

    PROPAGATE_EXCEPTIONS = False
    WTF_CSRF_ENABLED = False
    ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'test')

    API_KEY = os.environ.get('WIGO_API_KEY', 'oi34u53205ju34ik23')
    API_HOOK_KEY = os.environ.get('WIGO_API_HOOK_KEY', '9tejvdoikkf')

    REDIS_URL = os.environ.get('REDIS_URL', os.environ.get('REDISCLOUD_URL', 'redis://localhost:6379'))
    REDIS_URLS = parse_env_list('REDIS_URL')
    if not REDIS_URLS:
        REDIS_URLS.append(REDIS_URL)

    REDIS_QUEUES_URL = os.environ.get('REDIS_QUEUES_URL', REDIS_URL)

    DATABASE_URL = os.environ.get('DATABASE_URL', 'postgres://wigo:4090ejsAdff3@localhost/wigo')
    RDBMS_REPLICATE = os.environ.get('RDBMS_REPLICATE', 'true' if ENVIRONMENT != 'test' else 'false') == 'true'

    MAIL_SERVER = 'smtp.sendgrid.net'
    MAIL_USERNAME = os.environ.get('SENDGRID_USERNAME', None)
    MAIL_PASSWORD = os.environ.get('SENDGRID_PASSWORD', None)
    DEFAULT_MAIL_SENDER = 'support@wigo.us'
    MAIL_DEBUG = False

    SECURITY_PASSWORD_HASH = 'bcrypt'
    SECURITY_PASSWORD_SALT = '490odmvsxoro31'
    SECURITY_CONFIRM_SALT = 'sdfsdf235tuh3'
    SECURITY_RESET_SALT = 'uyp5vjmnj235tuh3'
    SECURITY_LOGIN_SALT = 'hnbio4391278'
    SECURITY_REMEMBER_SALT = 'iuyi3jvkmfj'
    SECURITY_DEFAULT_REMEMBER_ME = True
    SECURITY_CHANGEABLE = True
    SESSION_PROTECTION = False

    BROKER_TRANSPORT_OPTIONS = {
        'visibility_timeout': 3600,
        'fanout_prefix': True,
        'fanout_patterns': True
    }

    CELERY_TASK_SERIALIZER = 'json'
    CELERY_RESULT_SERIALIZER = 'json'
    CELERY_ACCEPT_CONTENT = ['json']
    CELERY_IMPORTS = ('server.tasks.email', 'server.tasks.images', 'server.tasks.notifications')
    CELERY_IGNORE_RESULT = True
    CELERY_ACKS_LATE = True
    CELERY_TRACK_STARTED = True

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
