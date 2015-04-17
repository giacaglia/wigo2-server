from __future__ import absolute_import
import logging

from server.tasks import push
from rq.decorators import job
from server.db import rate_limit, redis
from server.models import DoesNotExist, post_model_save
from server.models.event import EventMessage, EventMessageVote, Event
from server.models.user import User, Notification, Message, Tap, Invite, Friend


logger = logging.getLogger('wigo.notifications')


@job('notifications', connection=redis, timeout=30, result_ttl=0)
def notify_on_eventmessage(message_id):
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

            notification = Notification({
                'user_id': friend.id,
                'type': 'eventmessage.post',
                'from_user_id': message.user_id,
                'navigate': '/events/{}/messages/{}'.format(message.event_id, message.id),
                'message': message_text
            }).save()

            send_notification_push.delay(notification_id=notification.id)


@job('notifications', connection=redis, timeout=30, result_ttl=0)
def notify_on_eventmessage_vote(voter_id, message_id):
    voter = User.find(voter_id)
    message = EventMessage.find(message_id)

    # don't send a notification if blocked
    if voter_id == message.user_id or voter.group_id != message.user.group_id or \
            message.user.is_blocked(voter):
        return

    expires = message.user.group.get_day_end()
    with rate_limit('eventmessagevote:%s:%s:%s' % (message.user_id, message_id, voter_id), expires) as limited:
        if not limited:
            message_text = '{name} liked your photo in {event}'.format(
                name=voter.full_name.encode('utf-8'), event=message.event.name.encode('utf-8'))

            notification = Notification({
                'user_id': message.user_id,
                'type': 'eventmessage.vote',
                'from_user_id': voter_id,
                'navigate': '/events/{}/messages/{}'.format(message.event_id, message.id),
                'message': message_text
            }).save()

            send_notification_push.delay(notification_id=notification.id)


@job('notifications', connection=redis, timeout=30, result_ttl=0)
def notify_on_message(message_id):
    message = Message.find(message_id)
    user = message.to_user
    from_user = message.user

    message_text = message.message
    if message_text and len(message_text) > 1000:
        message_text = message_text[0:1000]

    push.alert(data={
        'navigate': '/messages/{}'.format(from_user.id),
        'sound': 'chord',
        'alert': {
            'body': '{}: {}'.format(from_user.full_name, message_text),
        }
    }, where={
        'wigo_id': user.id,
        'deviceType': 'ios'
    }, enterprise=user.enterprise)


@job('notifications', connection=redis, timeout=30, result_ttl=0)
def notify_on_tap(user_id, tapped_id):
    tapped = User.find(tapped_id)
    expires = tapped.group.get_day_end()
    with rate_limit('tap:{}:{}'.format(user_id, tapped_id), expires) as limited:
        if not limited:
            user = User.find(user_id)
            message_text = '{} wants to see you out'.format(user.full_name)
            notification = Notification({
                'user_id': tapped_id,
                'type': 'tap',
                'from_user_id': user_id,
                'navigate': '/users/{}'.format(user_id),
                'message': message_text
            }).save()

            send_notification_push.delay(notification_id=notification.id)


@job('notifications', connection=redis, timeout=30, result_ttl=0)
def notify_on_invite(inviter_id, invited_id, event_id):
    inviter = User.find(inviter_id)
    invited = User.find(invited_id)
    event = Event.find(event_id) if event_id else None

    message_text = '{} invited you out to {}'.format(inviter.full_name, event.name)

    notification = Notification({
        'user_id': invited_id,
        'type': 'invite',
        'from_user_id': inviter_id,
        'navigate': '/events/{}'.format(event_id),
        'message': message_text
    }).save()

    send_notification_push.delay(notification_id=notification.id)


@job('notifications', connection=redis, timeout=30, result_ttl=0)
def notify_on_friend(user_id, friend_id, accepted):
    user = User.find(user_id)
    friend = User.find(friend_id)

    if not accepted:
        message_text = '{} wants to be friends with you'.format(user.full_name)
    else:
        message_text = '{} accepted your friend request'.format(user.full_name)

    notification = Notification({
        'user_id': friend_id,
        'type': 'friend.request' if not accepted else 'friend.accept',
        'from_user_id': user.id,
        'navigate': '/users/{}'.format(user_id),
        'message': message_text
    }).save()

    send_notification_push.delay(notification_id=notification.id)


@job('push', connection=redis, timeout=30, result_ttl=0)
def send_notification_push(notification_id):
    notification = Notification.find(notification_id)

    push.alert(data={
        'id': notification.id,
        'navigate': notification.navigate,
        'alert': {
            'body': notification.message,
        }
    }, where={
        'wigo_id': notification.user_id,
        'deviceType': 'ios'
    }, enterprise=notification.user.enterprise)


def wire_notifications_listeners():
    def notifications_model_listener(sender, instance, created):
        if not created:
            return

        if isinstance(instance, EventMessage):
            notify_on_eventmessage.delay(message_id=instance.id)
        elif isinstance(instance, EventMessageVote):
            notify_on_eventmessage_vote.delay(voter_id=instance.user_id, message_id=instance.message_id)
        elif isinstance(instance, Message):
            notify_on_message.delay(message_id=instance.id)
        elif isinstance(instance, Friend):
            notify_on_friend.delay(user_id=instance.user_id, friend_id=instance.friend_id, accepted=instance.accepted)
        elif isinstance(instance, Tap):
            notify_on_tap.delay(user_id=instance.user_id, tapped_id=instance.tapped_id)
        elif isinstance(instance, Invite):
            notify_on_invite.delay(inviter_id=instance.user_id,
                                   invited_id=instance.invited_id, event_id=instance.event_id)

    post_model_save.connect(notifications_model_listener, weak=False)


