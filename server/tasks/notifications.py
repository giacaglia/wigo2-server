from __future__ import absolute_import

from server.db import rate_limit
from server.models import DoesNotExist, post_model_save
from server.models.event import EventMessage
from server.models.user import User, Notification


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


def wire_notifications_listeners():
    def notifications_model_listener(sender, instance, created):
        if not created:
            return
            #
            # if isinstance(instance, EventMessage):
            # notify_on_eventmessage.delay(message_id=instance.id)


    post_model_save.connect(notifications_model_listener, weak=False)


