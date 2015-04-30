from __future__ import absolute_import

from urlparse import urlparse
from redis import Redis
from rq import Queue
from config import Configuration

is_not_tests = Configuration.ENVIRONMENT != 'test'

redis_queues_url = urlparse(Configuration.REDIS_QUEUES_URL)
redis_queues = Redis(host=redis_queues_url.hostname,
                     port=redis_queues_url.port, password=redis_queues_url.password)


email_queue = Queue(name='email', connection=redis_queues, async=is_not_tests)
images_queue = Queue(name='images', connection=redis_queues, async=is_not_tests)
notifications_queue = Queue(name='notifications', connection=redis_queues, async=is_not_tests)
push_queue = Queue(name='push', connection=redis_queues, async=is_not_tests)
parse_queue = Queue(name='parse', connection=redis_queues, async=is_not_tests)
predictions_queue = Queue(name='predictions', connection=redis_queues, async=is_not_tests)
data_queue = Queue(name='data', connection=redis_queues, async=is_not_tests)