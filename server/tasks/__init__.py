from __future__ import absolute_import

from urlparse import urlparse
from redis import Redis
from config import Configuration


redis_queues_url = urlparse(Configuration.REDIS_QUEUES_URL)
redis_queues = Redis(host=redis_queues_url.hostname,
                     port=redis_queues_url.port, password=redis_queues_url.password)

