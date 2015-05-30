from __future__ import absolute_import

import logging
from itertools import islice
from retry import retry
from datetime import timedelta
from rq.decorators import job
from config import Configuration
from server.db import rate_limit
from server.services import push
from server.models import DoesNotExist, post_model_save, friend_attending
from server.models.event import EventMessage, EventMessageVote, Event, EventAttendee, get_num_attending
from server.models.user import User, Notification, Message, Tap, Invite, Friend
from server.services.facebook import FacebookTokenExpiredException, Facebook
from server.tasks import notifications_queue, push_queue, is_new_user
from utils import epoch

logger = logging.getLogger('wigo.notifications')


@job(notifications_queue, timeout=600, result_ttl=0)
def new_user(user_id):
    if Configuration.ENVIRONMENT == 'test':
        return

    user = User.find(user_id)

    facebook = Facebook(user.facebook_token, user.facebook_token_expires)

    try:
        for fb_friend in facebook.iter('/me/friends?fields=installed', timeout=600):
            facebook_id = fb_friend.get('id')
            with rate_limit('notify:friend_joined:{}:{}'.format(user_id, facebook_id),
                            timedelta(hours=1)) as limited:
                if not limited:
                    try:
                        friend = User.find(facebook_id=facebook_id)

                        notification = Notification({
                            'user_id': friend.id,
                            'type': 'friend.joined',
                            'navigate': '/find/users/user/{}'.format(user.id),
                            'badge': 1,
                            'message': 'Your Facebook friend {} just joined Wigo'.format(user.full_name)
                        })

                        __send_notification_push(notification)

                    except DoesNotExist:
                        pass

        user.track_meta('last_facebook_check')
    except FacebookTokenExpiredException:
        logger.warn('error finding facebook friends to alert for user {}, token expired'.format(user_id))
        user.track_meta('last_facebook_check')


@job(notifications_queue, timeout=30, result_ttl=0)
def notify_unlocked(user_id):
    with rate_limit('notify:unlock:{}'.format(user_id), timedelta(hours=1)) as limited:
        if not limited:
            user = User.find(user_id)

            notification = Notification({
                'user_id': user.id,
                'type': 'unlocked',
                'badge': 1,
                'message': 'You\'re in! It\'s time to party!'
            })

            __send_notification_push(notification)


@job(notifications_queue, timeout=30, result_ttl=0)
def notify_on_attendee(event_id, user_id):
    try:
        event = Event.find(event_id)
        if event.owner is None:
            return
    except DoesNotExist:
        return

    targets = [5, 10, 20, 30, 50, 75, 100, 150, 200, 250, 300, 350, 400, 450, 500]
    num_attending = get_num_attending(event_id)
    target = next(reversed([t for t in targets if t <= num_attending]), None)

    if target is None:
        return

    rl_key = 'notify:event_creators:{}:{}:{}'.format(event.id, event.owner_id, target)
    with rate_limit(rl_key, event.expires) as limited:
        if not limited:
            notification = Notification({
                'user_id': event.owner_id,
                'type': 'system',
                'navigate': '/events/{}'.format(event_id),
                'badge': 1,
                'message': '{} people are going to your event {}'.format(
                    num_attending, event.name.encode('utf-8'))
            }).save()

            send_notification_push.delay(notification.id)


@job(notifications_queue, timeout=30, result_ttl=0)
def notify_on_eventmessage(message_id):
    try:
        message = EventMessage.find(message_id)
    except DoesNotExist:
        return

    user = message.user
    type = 'video' if message.media_mime_type == 'video/mp4' else 'photo'

    with rate_limit('notify:eventmessage:{}:{}'.format(user.id, message.event.id),
                    timedelta(hours=2)) as limited:
        if limited:
            return

        for friend in EventAttendee.select().user(message.user).event(message.event):
            if friend == user:
                continue

            message_text = '{name} posted a {type} in {event}'.format(
                name=user.full_name.encode('utf-8'),
                type=type,
                event=message.event.name.encode('utf-8'))

            notification = Notification({
                'user_id': friend.id,
                'type': 'eventmessage.post',
                'from_user_id': message.user_id,
                'navigate': '/users/me/events/{}/messages/{}'.format(message.event_id, message.id),
                'message': message_text
            }).save()

            send_notification_push.delay(notification.id)


@job(notifications_queue, timeout=30, result_ttl=0)
def notify_on_eventmessage_vote(voter_id, message_id):
    voter = User.find(voter_id)
    message = EventMessage.find(message_id)
    user = message.user
    type = 'video' if message.media_mime_type == 'video/mp4' else 'photo'

    # don't send to self or if not friend
    if (voter_id == message.user_id) or (not user.is_friend(voter_id)):
        return

    with rate_limit('notify:vote:%s:%s:%s' % (message.user_id, message_id, voter_id),
                    timedelta(hours=2)) as limited:
        if not limited:
            message_text = '{name} liked your {type} in {event}'.format(
                name=voter.full_name.encode('utf-8'),
                type=type,
                event=message.event.name.encode('utf-8'))

            notification = Notification({
                'user_id': message.user_id,
                'type': 'eventmessage.vote',
                'from_user_id': voter_id,
                'navigate': '/users/me/events/{}/messages/{}'.format(message.event_id, message.id),
                'message': message_text
            }).save()

            send_notification_push.delay(notification.id)


@job(notifications_queue, timeout=30, result_ttl=0)
def notify_on_message(message_id):
    message = Message.find(message_id)
    user = message.to_user
    from_user = message.user

    message_text = message.message
    if message_text and len(message_text) > 1000:
        message_text = message_text[0:1000]

    push.alert(data={
        'message_id': message_id,
        'navigate': '/messages/{}'.format(from_user.id),
        'sound': 'chord',
        'badge': 1,
        'alert': {
            'body': '{}: {}'.format(from_user.full_name.encode('utf-8'), message_text.encode('utf-8')),
        }
    }, where={
        'wigo_id': user.id,
        'deviceType': 'ios',
        'api_version_num': {
            '$gte': 2
        }
    }, enterprise=user.enterprise)


@job(notifications_queue, timeout=30, result_ttl=0)
def notify_on_tap(user_id, tapped_id):
    with rate_limit('notify:tap:{}:{}'.format(user_id, tapped_id), timedelta(hours=2)) as limited:
        if not limited:
            user = User.find(user_id)
            message_text = '{} wants to see you out'.format(user.full_name.encode('utf-8'))
            notification = Notification({
                'user_id': tapped_id,
                'type': 'tap',
                'from_user_id': user_id,
                'navigate': '/users/{}'.format(user_id),
                'badge': 1,
                'message': message_text
            }).save()

            send_notification_push.delay(notification.id)


@job(notifications_queue, timeout=30, result_ttl=0)
def notify_on_invite(inviter_id, invited_id, event_id):
    rl_key = 'notify:invite:{}:{}:{}'.format(inviter_id, invited_id, event_id)
    with rate_limit(rl_key, timedelta(hours=2)) as limited:
        if not limited:
            inviter = User.find(inviter_id)
            invited = User.find(invited_id)
            event = Event.find(event_id) if event_id else None

            message_text = '{} invited you out to {}'.format(inviter.full_name.encode('utf-8'), event.name.encode('utf-8'))

            notification = Notification({
                'user_id': invited_id,
                'type': 'invite',
                'from_user_id': inviter_id,
                'navigate': '/users/me/events/{}'.format(event_id),
                'badge': 1,
                'message': message_text
            }).save()

            send_notification_push.delay(notification.id)


@job(notifications_queue, timeout=30, result_ttl=0)
def notify_on_friend(user_id, friend_id, accepted):
    user = User.find(user_id)
    friend = User.find(friend_id)

    if not accepted:
        message_text = '{} wants to be friends with you'.format(user.full_name.encode('utf-8'))
    else:
        message_text = '{} accepted your friend request'.format(user.full_name.encode('utf-8'))

    notification = Notification({
        'user_id': friend_id,
        'type': 'friend.request' if not accepted else 'friend.accept',
        'from_user_id': user.id,
        'navigate': '/users/{}'.format(user_id) if accepted else '/find/users/user/{}'.format(user_id),
        'badge': 1,
        'message': message_text
    })

    if accepted:
        notification.save()
        send_notification_push.delay(notification.id)
    else:
        __send_notification_push(notification)

        # for imported users from wigo1, send them a push saying their friend just joined wigo2
        if friend.status == 'imported':
            notification = Notification({
                'user_id': friend_id,
                'type': 'system',
                'message': '{} added you to {} Wigo Summer friend list. Update Wigo now!'.format(
                    user.full_name.encode('utf-8'), ('his' if user.gender == 'male' else
                                                     'her' if user.gender == 'female' else 'their'))
            })

            __send_notification_push(notification, api_version_num=1)


@job(notifications_queue, timeout=30, result_ttl=0)
def notify_on_friend_attending(event_id, user_id, friend_id):
    num_attending = get_num_attending(event_id, user_id)
    if num_attending < 5:
        return

    try:
        event = Event.find(event_id)
        user = User.find(user_id)
    except DoesNotExist:
        return

    rl_key = 'notify:nofa:{}:{}'.format(event_id, user_id)
    with rate_limit(rl_key, event.expires) as limited:
        if not limited:
            friends = list(islice(EventAttendee.select().event(event).user(user).limit(6), 5))
            if user in friends:
                friends.remove(user)
                num_attending -= 1

            logger.info('notifying user {} of {} friends attending event {}'.format(user_id, num_attending, event_id))
            if len(friends) >= 2:
                notification = Notification({
                    'user_id': user.id,
                    'type': 'system',
                    'navigate': '/users/me/events/{}'.format(event_id),
                    'badge': 1,
                    'message': '{}, {}, and {} others are going to {}'.format(
                        friends[0].full_name, friends[1].full_name,
                        num_attending - 2, event.name.encode('utf-8'))
                }).save()

                send_notification_push.delay(notification.id)



@job(push_queue, timeout=30, result_ttl=0)
@retry(tries=3, delay=2, backoff=2)
def send_notification_push(notification_id):
    notification = Notification.find(notification_id)
    __send_notification_push(notification)


def __send_notification_push(notification, api_version_num=2):
    data = {
        'alert': {
            'body': notification.message
        }
    }

    if notification.id:
        data['id'] = notification.id

    if notification.type:
        data['type'] = notification.type

    if notification.navigate:
        data['navigate'] = notification.navigate

    if notification.badge:
        data['badge'] = notification.badge

    where = {'wigo_id': notification.user_id,
             'deviceType': 'ios'}

    if api_version_num >= 2:
        where['api_version_num'] = {'$gte': api_version_num}
    else:
        where['api_version'] = '1.0.7'

    push.alert(data=data, where=where, enterprise=notification.user.enterprise)


def wire_notifications_listeners():
    def notifications_model_listener(sender, instance, created):
        if isinstance(instance, User):
            if is_new_user(instance, created):
                new_user.delay(instance.id)
            if not created and (instance.was_changed('status')
                                and instance.get_previous_old_value('status') == 'waiting'):
                notify_unlocked.delay(instance.id)
        elif isinstance(instance, EventAttendee) and created:
            notify_on_attendee.delay(instance.event_id, instance.user_id)
        elif isinstance(instance, EventMessage) and created:
            notify_on_eventmessage.delay(instance.id)
        elif isinstance(instance, EventMessageVote) and created:
            notify_on_eventmessage_vote.delay(instance.user_id, instance.message_id)
        elif isinstance(instance, Message) and created:
            notify_on_message.delay(instance.id)
        elif isinstance(instance, Friend) and created:
            notify_on_friend.delay(instance.user_id, instance.friend_id, instance.accepted)
        elif isinstance(instance, Tap) and created:
            notify_on_tap.delay(instance.user_id, instance.tapped_id)
        elif isinstance(instance, Invite) and created:
            notify_on_invite.delay(instance.user_id, instance.invited_id, instance.event_id)
        elif isinstance(instance, Notification) and created:
            instance.user.track_meta('last_notification', epoch(instance.created))

    post_model_save.connect(notifications_model_listener, weak=False)

    def on_friend_attending(sender, event, user, friend):
        if event.owner_id != user.id and get_num_attending(event.id, user.id) >= 5:
            notify_on_friend_attending.delay(event.id, user.id, friend.id)

    friend_attending.connect(on_friend_attending, weak=False)
