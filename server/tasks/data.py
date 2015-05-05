from __future__ import absolute_import

import logging

from time import time
from datetime import timedelta, datetime
from rq.decorators import job
from server.db import wigo_db, redis
from server.models.group import Group, get_close_groups
from server.models.user import User, Friend, Invite
from server.tasks import data_queue
from server.models import post_model_save, skey, user_privacy_change, DoesNotExist, post_model_delete, \
    user_attendees_key
from server.models.event import Event, EventMessage, EventMessageVote, EventAttendee
from utils import epoch

logger = logging.getLogger('wigo.tasks.data')


@job(data_queue, timeout=30, result_ttl=0)
def event_landed_in_group(group_id):
    logger.info('recording event landed in group {}'.format(group_id))

    group = Group.find(group_id)
    population = group.population or 50000
    radius = 100

    if population >= 500000:
        radius = 20
    elif population >= 100000:
        radius = 30
    elif population >= 50000:
        radius = 50

    # tell each close group that this group just had a new event
    for close_group in get_close_groups(group.latitude, group.longitude, radius):
        key = skey(close_group, 'close_groups_with_events')

        if close_group.id != group.id:
            wigo_db.sorted_set_add(key, group_id, time(), replicate=False)

        # remove groups that haven't had events in 8 days
        wigo_db.sorted_set_remove_by_score(key, 0, epoch(datetime.utcnow() - timedelta(days=8)))


@job(data_queue, timeout=60, result_ttl=0)
def user_invited(event_id, inviter_id, invited_id):
    event = Event.find(event_id)
    inviter = User.find(inviter_id)
    invited = User.find(invited_id)

    # make sure i am seeing all my friends attending now
    for friend_id, score in wigo_db.sorted_set_iter(skey(invited, 'friends')):
        friend = User.find(friend_id)
        if friend.is_attending(event):
            event.add_to_user_attending(invited, friend, score)


@job(data_queue, timeout=60, result_ttl=0)
def tell_friends_user_attending(user_id, event_id):
    user = User.find(user_id)
    event = Event.find(event_id)

    if user.is_attending(event):
        with user_lock(user.id):
            for friend_id, score in wigo_db.sorted_set_iter(skey(user, 'friends')):
                friend = User.find(int(friend_id))
                if friend.can_see_event(event):
                    event.add_to_user_attending(friend, user, score)


@job(data_queue, timeout=60, result_ttl=0)
def tell_friends_user_not_attending(user_id, event_id):
    user = User.find(user_id)
    event = Event.find(event_id)

    if not user.is_attending(event):
        with user_lock(user.id):
            for friend_id, score in wigo_db.sorted_set_iter(skey(user, 'friends')):
                friend = User.find(int(friend_id))
                event.remove_from_user_attending(friend, user)


@job(data_queue, timeout=60, result_ttl=0)
def tell_friends_event_message(message_id):
    message = EventMessage.find(message_id)
    user = message.user

    with user_lock(user.id):
        for friend_id, score in wigo_db.sorted_set_iter(skey(user, 'friends')):
            friend = User.find(int(friend_id))
            message.record_for_user(friend)


@job(data_queue, timeout=60, result_ttl=0)
def tell_friends_delete_event_message(user_id, event_id, message_id):
    message = EventMessage({
        'id': message_id,
        'user_id': user_id,
        'event_id': event_id
    })

    with user_lock(user_id):
        for friend_id, score in wigo_db.sorted_set_iter(skey('user', user_id, 'friends')):
            friend = User.find(int(friend_id))
            message.remove_for_user(friend)


@job(data_queue, timeout=60, result_ttl=0)
def tell_friends_about_vote(message_id, user_id):
    vote = EventMessageVote({
        'message_id': message_id,
        'user_id': user_id
    })

    for friend_id, score in wigo_db.sorted_set_iter(skey('user', user_id, 'friends')):
        friend = User.find(int(friend_id))
        vote.record_for_user(friend)


@job(data_queue, timeout=60, result_ttl=0)
def new_friend(user_id, friend_id):
    user = User.find(user_id)
    friend = User.find(friend_id)

    # tells each friend about the event history of the other
    def capture_history(u, f):
        with user_lock(u.id):
            # capture photo votes first, so when adding the photos they can be sorted by vote
            for message_id, score in wigo_db.sorted_set_iter(skey(u, 'votes')):
                try:
                    EventMessageVote({
                        'user_id': u.id,
                        'message_id': message_id
                    }).record_for_user(f)
                except DoesNotExist:
                    pass

            # capture each of the users posted photos
            for message_id, score in wigo_db.sorted_set_iter(skey(u, 'event_messages')):
                try:
                    message = EventMessage.find(message_id)
                    message.record_for_user(f)
                except DoesNotExist:
                    pass

            # capture the events being attended
            for event_id, score in wigo_db.sorted_set_iter(skey(u, 'events')):
                try:
                    event = Event.find(event_id)
                    if u.is_attending(event) and f.can_see_event(event):
                        event.add_to_user_attending(f, u)
                except DoesNotExist:
                    pass

    capture_history(user, friend)
    capture_history(friend, user)


@job(data_queue, timeout=60, result_ttl=0)
def delete_friend(user_id, friend_id):
    user = User.find(user_id)
    friend = User.find(friend_id)

    def delete_history(u, f):
        with user_lock(u.id):
            for event_id, score in wigo_db.sorted_set_iter(skey(u, 'events')):
                try:
                    event = Event.find(event_id)
                    if wigo_db.sorted_set_is_member(user_attendees_key(u, event), f.id):
                        event.remove_from_user_attending(u, f)
                except DoesNotExist:
                    pass

    delete_history(user, friend)
    delete_history(friend, user)


@job(data_queue, timeout=60, result_ttl=0)
def privacy_changed(user_id):
    # tell all friends about the privacy change
    with user_lock(user_id):
        user = User.find(user_id)
        for friend_id, score in wigo_db.sorted_set_iter(skey(user, 'friends')):
            if user.privacy == 'public':
                wigo_db.set_remove(skey('user', friend_id, 'friends', 'private'), user_id)
            else:
                wigo_db.set_add(skey('user', friend_id, 'friends', 'private'), user_id)


def user_lock(user_id):
    with redis.lock('locks:user:{}'.format(user_id), timeout=30):
        yield


def wire_data_listeners():
    def data_save_listener(sender, instance, created):
        if isinstance(instance, Friend) and created:
            if instance.accepted:
                new_friend.delay(instance.user_id, instance.friend_id)
        elif isinstance(instance, Event):
            event_landed_in_group.delay(instance.group_id)
        elif isinstance(instance, Invite):
            user_invited.delay(instance.event_id, instance.user_id, instance.invited_id)
        elif isinstance(instance, EventAttendee):
            tell_friends_user_attending.delay(instance.user_id, instance.event_id)
        elif isinstance(instance, EventMessage):
            tell_friends_event_message.delay(instance.id)
        elif isinstance(instance, EventMessageVote):
            tell_friends_about_vote.delay(instance.message_id, instance.user_id)

    def data_delete_listener(sender, instance):
        if isinstance(instance, EventMessage):
            tell_friends_delete_event_message.delay(instance.user_id, instance.event_id, instance.id)
        if isinstance(instance, EventAttendee):
            tell_friends_user_not_attending.delay(instance.user_id, instance.event_id)
        if isinstance(instance, Friend):
            delete_friend.delay(instance.user_id, instance.friend_id)

    def privacy_changed_listener(sender, instance):
        privacy_changed.delay(instance.id)

    post_model_save.connect(data_save_listener, weak=False)
    post_model_delete.connect(data_delete_listener, weak=False)
    user_privacy_change.connect(privacy_changed_listener, weak=False)

