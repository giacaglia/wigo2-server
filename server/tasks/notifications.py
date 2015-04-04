from __future__ import absolute_import

from server.db import wigo_db, rate_limit
from server.models import DoesNotExist, post_model_save
from server.models.event import EventMessage
from server.models.user import User, Notification
from server.worker import celery


@celery.task(max_retries=10, default_retry_delay=10)
def notify_on_eventmessage(self, message_id):
    try:
        message = EventMessage.find(message_id)
    except DoesNotExist:
        return

    user = message.user
    with rate_limit('eventmessage:{}:'.format(user.id, message.event.id), message.event.expires) as limited:
        if limited:
            return

        for friend in User.select().user(message.user).event(message.event):
            message_text = '{name} posted a photo in {event}'.format(
                name=user.full_name.encode('utf-8'), event=message.event.name.encode('utf-8'))

            Notification({
                'user_id': friend.id,
                'type': 'eventmessage.post',
                'from_user_id': message.user_id,
                'properties': {
                    'message': message_text,
                    'navigate': '/events/{}/messages/{}'.format(
                        message.event.id,
                        message.id
                    )
                }
            }).save()


def create_notification(sender, instance, created):
    if not created:
        return

    if isinstance(instance, EventMessage):
        notify_on_eventmessage.delay(message_id=instance.id)


def wire_notifications():
    post_model_save.connect(create_notification)


