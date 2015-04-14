from __future__ import absolute_import
from celery import Celery
from config import Configuration


celery = Celery('wigo', broker=Configuration.REDIS_QUEUES_URL)
celery.config_from_object(Configuration)