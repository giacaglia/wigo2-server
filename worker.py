from __future__ import absolute_import
from config import Configuration
from server.rest.uploads import wire_uploads_listeners
from server.tasks.images import wire_images_listeners
from server.tasks.notifications import wire_notifications_listeners


REDIS_URL = Configuration.REDIS_URL
QUEUES = ['email', 'images', 'notifications']

wire_notifications_listeners()
wire_uploads_listeners()
wire_images_listeners()
