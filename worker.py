from __future__ import absolute_import

import logconfig
from config import Configuration

logconfig.configure(Configuration.ENVIRONMENT)

import requests
import logging
from time import sleep
from datetime import datetime, timedelta
from threading import Thread
from server.db import redis, scheduler
from server.tasks.data import wire_data_listeners, process_waitlist
from server.tasks.parse import wire_parse_listeners
from server.tasks.predictions import wire_predictions_listeners
from server.tasks.uploads import wire_uploads_listeners
from server.tasks.images import wire_images_listeners
from server.tasks.notifications import wire_notifications_listeners

logger = logging.getLogger('wigo.worker')

requests.packages.urllib3.disable_warnings()

Configuration.IS_WORKER = True
REDIS_URL = Configuration.REDIS_URL

QUEUES = ['email', 'images', 'notifications', 'push',
          'parse', 'predictions', 'data', 'scheduled']

logger.info('wiring listeners')

wire_notifications_listeners()
wire_uploads_listeners()
wire_images_listeners()
wire_parse_listeners()
wire_predictions_listeners()
wire_data_listeners()

logger.info('starting scheduler')


class SchedulerThread(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.daemon = True

    def run(self):
        logger.info('running rq scheduler')
        while True:
            with redis.lock('locks:schedule_jobs', timeout=30):
                scheduler.enqueue_jobs()
            sleep(10)

# clear any pre-existing scheduled jobs
jobs = scheduler.get_jobs()
for job in jobs:
    if job.meta['interval']:
        scheduler.cancel(job)

# schedule job to process the user wait list
scheduler.schedule(datetime.utcnow() + timedelta(seconds=10),
                   process_waitlist, interval=60, timeout=600)


def process_expired():
    from server.db import wigo_db

    wigo_db.process_expired()

# schedule job to expire redis keys
scheduler.schedule(datetime.utcnow() + timedelta(seconds=10),
                   process_expired, interval=60 * 60, timeout=600)

# start schedule processor
thread = SchedulerThread()
thread.start()
