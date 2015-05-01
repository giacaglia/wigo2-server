from __future__ import absolute_import

from urlparse import urlparse
from redis import Redis
from rq import Queue
from config import Configuration
from server.db import redis

is_not_tests = Configuration.ENVIRONMENT != 'test'

email_queue = Queue(name='email', connection=redis, async=is_not_tests)
images_queue = Queue(name='images', connection=redis, async=is_not_tests)
notifications_queue = Queue(name='notifications', connection=redis, async=is_not_tests)
push_queue = Queue(name='push', connection=redis, async=is_not_tests)
parse_queue = Queue(name='parse', connection=redis, async=is_not_tests)
predictions_queue = Queue(name='predictions', connection=redis, async=is_not_tests)
data_queue = Queue(name='data', connection=redis, async=is_not_tests)