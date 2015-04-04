from __future__ import absolute_import

import os
import urlparse

class Configuration(object):
    ENVIRONMENT = os.environ.get('WIGO_ENV', 'dev')
    SECRET_KEY = 'bb034599jkrtbg30ijwerrgjvn'
    PROPAGATE_EXCEPTIONS = False
    WTF_CSRF_ENABLED = False

    API_KEY = os.environ.get('WIGO_API_KEY', 'oi34u53205ju34ik23')
    API_HOOK_KEY = os.environ.get('WIGO_API_HOOK_KEY', '9tejvdoikkf')

    WIGO_WEB_HOST = os.environ.get('WIGO_WEB_HOST', 'localhost:5000')
    WIGO_API_HOST = os.environ.get('WIGO_API_HOST', WIGO_WEB_HOST.replace('verify', 'api'))

    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')
    REDIS_ANALYTICS_URL = os.environ.get('REDIS_ANALYTICS_URL', REDIS_URL)
    REDIS_QUEUES_URL = os.environ.get('REDIS_QUEUES_URL', REDIS_URL)

    DATABASE_URL = os.environ.get('DATABASE_URL', 'postgres://wigo:4090ejsAdff3@localhost/wigo')

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

    WIGO_UPLOADS_AWS_ACCESS_KEY_ID = os.environ.get('WIGO_UPLOADS_AWS_ACCESS_KEY_ID', '')
    WIGO_UPLOADS_AWS_SECRET_ACCESS_KEY = os.environ.get('WIGO_UPLOADS_AWS_SECRET_ACCESS_KEY', '')
    WIGO_UPLOADS_CDN = os.environ.get('WIGO_UPLOADS_CDN', 'd9vw4owxyhc4n.cloudfront.net')

    TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
    TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
    TWILIO_FROM = os.environ.get('TWILIO_FROM', '+16173074688')

    PARSE_APPLICATION_ID = os.environ.get('PARSE_APPLICATION_ID', None)
    PARSE_REST_KEY = os.environ.get('PARSE_REST_KEY', None)
    PARSE_MASTER_REST_KEY = os.environ.get('PARSE_MASTER_REST_KEY', None)
    PARSE_ENTERPRISE_APPLICATION_ID = os.environ.get('PARSE_ENTERPRISE_APPLICATION_ID', None)
    PARSE_ENTERPRISE_REST_KEY = os.environ.get('PARSE_ENTERPRISE_REST_KEY', None)

    BLITLINE_APPLICATION_ID = os.environ.get('BLITLINE_APPLICATION_ID', '')
    BLITLINE_URL = os.environ.get('BLITLINE_URL', '')

    PAPERTRAIL_API_TOKEN = os.environ.get('PAPERTRAIL_API_TOKEN', 'BG2MGQF1cinTAHhRZiFe')
    PUSH_ENABLED = os.environ.get('PUSH_ENABLED', 'true') == 'true'

    CAPTURE_IMAGES = True

    def is_push_enabled(self, user):
        if not self.PUSH_ENABLED:
            return False
        if self.ENVIRONMENT == 'production':
            return True
        return user._data.get('group') == 1