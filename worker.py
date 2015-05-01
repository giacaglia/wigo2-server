from __future__ import absolute_import

import logconfig
from config import Configuration

logconfig.configure(Configuration.ENVIRONMENT)

import requests
from server.tasks.data import wire_data_listeners
from server.tasks.parse import wire_parse_listeners
from server.tasks.predictions import wire_predictions_listeners
from server.tasks.uploads import wire_uploads_listeners
from server.tasks.images import wire_images_listeners
from server.tasks.notifications import wire_notifications_listeners


requests.packages.urllib3.disable_warnings()


REDIS_URL = Configuration.REDIS_URL

QUEUES = ['email', 'images', 'notifications',
          'push', 'parse', 'predictions', 'data']

wire_notifications_listeners()
wire_uploads_listeners()
wire_images_listeners()
wire_parse_listeners()
wire_predictions_listeners()
wire_data_listeners()