from __future__ import absolute_import

import logging
import ujson

from random import randint
from threading import Thread
from time import time, sleep
from datetime import datetime, timedelta
from contextlib import contextmanager
from urlparse import urlparse
from redis import Redis
from rq.decorators import job
from config import Configuration

from server.db import wigo_db, scheduler, redis
from server.models.group import Group, get_close_groups, get_all_groups

from server.models.user import User, Friend, Invite, Tap, Block, Message
from server.tasks import data_queue, is_new_user
from server.models import post_model_save, skey, user_privacy_change, DoesNotExist, post_model_delete, \
    user_attendees_key, user_votes_key, friend_attending
from server.models.event import Event, EventMessage, EventMessageVote, EventAttendee
from utils import epoch

EVENT_CHANGE_TIME_BUFFER = 60

logger = logging.getLogger('wigo.tasks.data')


def new_user(user_id, score=None):
    user = User.find(user_id)

    if user.status != 'waiting':
        return

    user_queue_key = skey('user_queue')

    if score is None:
        last_waiting = wigo_db.sorted_set_range(user_queue_key, -1, -1, True)
        if last_waiting:
            last_waiting_score = last_waiting[0][1]
            if last_waiting_score > (time() + (60 * 60 * 11)):
                score = last_waiting_score + 10
            else:
                score = last_waiting_score + randint(0, 120)
        else:
            score = time() + randint(120, 60 * 20)

    wigo_db.sorted_set_add(user_queue_key, user_id, score, replicate=False)

    scheduler.schedule(datetime.utcfromtimestamp(score), process_waitlist,
                       result_ttl=0, timeout=600)


def process_waitlist():
    while True:
        lock = redis.lock('locks:process_waitlist', timeout=600)
        if lock.acquire(blocking=False):
            try:
                user_ids = wigo_db.sorted_set_range_by_score(skey('user_queue'), 0, time(), 0, 50)
                if user_ids:
                    for user_id in user_ids:
                        logger.info('unlocking user id {}'.format(user_id))
                        user = User.find(user_id)
                        if user.is_waiting():
                            user.status = 'active'
                            user.save()

                        # remove from wait list
                        wigo_db.sorted_set_remove(skey('user_queue'), user.id, replicate=False)
                else:
                    break
            finally:
                lock.release()


@job(data_queue, timeout=600, result_ttl=0)
def new_group(group_id):
    group = Group.find(group_id)
    logger.alert('new group {} created, importing events'.format(group.name.encode('utf-8')))
    num_imported = 0
    imported = set()

    min = epoch(group.get_day_end() - timedelta(days=7))

    for close_group in get_close_groups(group.latitude, group.longitude, 100):
        if close_group.id == group.id:
            continue

        for event in Event.select().group(close_group).min(min):
            # only import the events the group actually owns
            if event.group_id != close_group.id:
                continue
            # no double imports
            if event.id not in imported:
                event.update_global_events(group=group)
                imported.add(event.id)
                num_imported += 1

    for event in Event.select().key(skey('global', 'events')).min(min):
        if event.id not in imported:
            event.update_global_events(group=group)
            imported.add(event.id)
            num_imported += 1

    logger.info('imported {} events into group {}'.format(num_imported, group.name.encode('utf-8')))
    group.track_meta('last_event_change', expire=None)
    group.status = 'active'
    group.save()


@job(data_queue, timeout=60, result_ttl=0)
def event_related_change(group_id, event_id):
    from server.db import redis

    logger.info('recording event change in group {}'.format(group_id))

    try:
        event = Event.find(event_id)
        event.deleted = False
    except DoesNotExist:
        event = Event({
            'id': event_id,
            'group_id': group_id
        })
        event.deleted = True

    lock = redis.lock('locks:group_event_change:{}:{}'.format(group_id, event_id), timeout=360)
    if lock.acquire(blocking=False):
        try:
            group = Group.find(group_id)

            # add to the time in case other changes come in while this lock is taken,
            # or in case the job queues get backed up
            group.track_meta('last_event_change', time() + EVENT_CHANGE_TIME_BUFFER, expire=None)

            if event.is_global:
                groups_to_add_to = get_all_groups()
            else:
                radius = 100
                population = group.population or 50000
                if population < 50000:
                    radius = 40
                elif population < 100000:
                    radius = 60

                groups_to_add_to = get_close_groups(group.latitude, group.longitude, radius)

            for group_to_add_to in groups_to_add_to:
                if group_to_add_to.id == group.id:
                    continue

                # index this event into the close group
                if event.deleted is False:
                    event.update_global_events(group=group_to_add_to)
                else:
                    event.remove_index(group=group_to_add_to)

                # clean out old events
                wigo_db.clean_old(skey(group_to_add_to, 'events'), Event.TTL)

                # track the change for the group
                group_to_add_to.track_meta('last_event_change', time() + EVENT_CHANGE_TIME_BUFFER, expire=None)

        finally:
            lock.release()


@job(data_queue, timeout=60, result_ttl=0)
def user_invited(event_id, inviter_id, invited_id):
    event = Event.find(event_id)
    inviter = User.find(inviter_id)
    invited = User.find(invited_id)

    # make sure i am seeing all my friends attending now
    for friend, score in invited.friends_iter():
        if friend.is_attending(event):
            event.add_to_user_attending(invited, friend, score)


@job(data_queue, timeout=60, result_ttl=0)
def tell_friends_user_attending(user_id, event_id):
    user = User.find(user_id)
    event = Event.find(event_id)

    if user.is_attending(event):
        with user_lock(user.id):
            for friend, score in user.friends_iter():
                if friend.can_see_event(event):
                    event.add_to_user_attending(friend, user, score)
                    friend_attending.send(None, event=event, user=friend, friend=user)

@job(data_queue, timeout=60, result_ttl=0)
def tell_friends_user_not_attending(user_id, event_id):
    user = User.find(user_id)
    event = Event.find(event_id)

    if not user.is_attending(event):
        with user_lock(user.id):
            for friend, score in user.friends_iter():
                event.remove_from_user_attending(friend, user)


@job(data_queue, timeout=60, result_ttl=0)
def tell_friends_event_message(message_id):
    message = EventMessage.find(message_id)
    user = message.user

    with user_lock(user.id):
        for friend, score in user.friends_iter():
            message.record_for_user(friend)


@job(data_queue, timeout=60, result_ttl=0)
def tell_friends_delete_event_message(user_id, event_id, message_id):
    user = User.find(user_id)

    message = EventMessage({
        'id': message_id,
        'user_id': user_id,
        'event_id': event_id
    })

    with user_lock(user_id):
        for friend, score in user.friends_iter():
            message.remove_for_user(friend)


@job(data_queue, timeout=60, result_ttl=0)
def tell_friends_about_vote(message_id, user_id):
    user = User.find(user_id)

    vote = EventMessageVote({
        'message_id': message_id,
        'user_id': user_id
    })

    for friend, score in user.friends_iter():
        vote.record_for_user(friend)


@job(data_queue, timeout=600, result_ttl=0)
def new_friend(user_id, friend_id):
    user = User.find(user_id)
    friend = User.find(friend_id)

    if not user.is_friend(friend):
        return

    min = epoch(datetime.utcnow() - timedelta(days=8))

    # tells each friend about the event history of the other
    def capture_history(u, f):
        with user_lock(f.id, 300):
            # capture photo votes first, so when adding the photos they can be sorted by vote
            for message in EventMessage.select().key(skey(u, 'votes')).min(min):
                if message.user and message.event:
                    EventMessageVote({
                        'user_id': u.id,
                        'message_id': message.id
                    }).record_for_user(f)

            # capture each of the users posted photos
            for message in EventMessage.select().key(skey(u, 'event_messages')).min(min):
                if message.user and message.event:
                    message.record_for_user(f)

            # capture the events being attended
            for event in Event.select().user(u).min(min):
                if u.is_attending(event) and f.can_see_event(event):
                    event.add_to_user_attending(f, u)

    capture_history(user, friend)
    capture_history(friend, user)


@job(data_queue, timeout=600, result_ttl=0)
def delete_friend(user_id, friend_id):
    user = User.find(user_id)
    friend = User.find(friend_id)

    if user.is_friend(friend):
        return

    def delete_history(u, f):
        with user_lock(f.id, 300):
            for message in EventMessage.select().key(skey(u, 'event_messages')):
                if message.user and message.event:
                    message.remove_for_user(f)

            for event in Event.select().user(u):
                if wigo_db.sorted_set_is_member(user_attendees_key(f, event), u.id):
                    event.remove_from_user_attending(f, u)

    delete_history(user, friend)
    delete_history(friend, user)


@job(data_queue, timeout=60, result_ttl=0)
def privacy_changed(user_id):
    # tell all friends about the privacy change
    with user_lock(user_id):
        user = User.find(user_id)

        for friend, score in user.friends_iter():
            if user.privacy == 'public':
                wigo_db.set_remove(skey(friend, 'friends', 'private'), user_id)
            else:
                wigo_db.set_add(skey(friend, 'friends', 'private'), user_id)


@job(data_queue, timeout=60, result_ttl=0)
def delete_user(user_id, group_id):
    logger.info('deleting user {}'.format(user_id))

    friend_ids = wigo_db.sorted_set_range(skey('user', user_id, 'friends'))

    # remove from attendees
    for event_id, score in wigo_db.sorted_set_iter(skey('user', user_id, 'events')):
        wigo_db.sorted_set_remove(skey('event', event_id, 'attendees'), user_id)
        for friend_id in friend_ids:
            wigo_db.sorted_set_remove(skey('user', friend_id, 'event', event_id, 'attendees'), user_id)

    # remove event message votes
    for message_id, score in wigo_db.sorted_set_iter(skey('user', user_id, 'votes')):
        wigo_db.sorted_set_remove(skey('eventmessage', message_id, 'votes'), user_id)
        for friend_id in friend_ids:
            wigo_db.sorted_set_remove(user_votes_key(friend_id, message_id), user_id)

    # remove event messages
    for message_id, score in wigo_db.sorted_set_iter(skey('user', user_id, 'event_messages')):
        message = EventMessage.find(message_id)
        event_id = message.event_id
        wigo_db.sorted_set_remove(skey('event', event_id, 'messages'), message_id)
        wigo_db.sorted_set_remove(skey('event', event_id, 'messages', 'by_votes'), message_id)
        for friend_id in friend_ids:
            wigo_db.sorted_set_remove(skey('user', friend_id, 'event', event_id, 'messages'), message_id)
            wigo_db.sorted_set_remove(skey('user', friend_id, 'event', event_id, 'messages', 'by_votes'), message_id)

    for friend_id in friend_ids:
        # remove conversations
        wigo_db.sorted_set_remove(skey('user', friend_id, 'conversations'), user_id)
        wigo_db.delete(skey('user', friend_id, 'conversation', user_id))
        wigo_db.delete(skey('user', user_id, 'conversation', friend_id))

        # remove friends
        wigo_db.sorted_set_remove(skey('user', friend_id, 'friends'), user_id)
        wigo_db.sorted_set_remove(skey('user', friend_id, 'friends', 'alpha'), user_id)
        wigo_db.set_remove(skey('user', friend_id, 'friends', 'private'), user_id)

    # remove friend requests
    for friend_id in wigo_db.sorted_set_range(skey('user', user_id, 'friend_requested')):
        wigo_db.sorted_set_remove(skey('user', friend_id, 'friend_requests'), user_id)
        wigo_db.sorted_set_remove(skey('user', friend_id, 'friend_requests', 'common'), user_id)

    # remove messages
    for message_id, score in wigo_db.sorted_set_iter(skey('user', user_id, 'messages')):
        wigo_db.delete(skey('message', message_id))

    wigo_db.delete(skey('user', user_id, 'events'))
    wigo_db.delete(skey('user', user_id, 'friends'))
    wigo_db.delete(skey('user', user_id, 'friends', 'private'))
    wigo_db.delete(skey('user', user_id, 'friends', 'alpha'))
    wigo_db.delete(skey('user', user_id, 'friends', 'friend_requested'))
    wigo_db.delete(skey('user', user_id, 'friends', 'friend_requests'))
    wigo_db.delete(skey('user', user_id, 'friends', 'friend_requests', 'common'))
    wigo_db.delete(skey('user', user_id, 'blocked'))
    wigo_db.delete(skey('user', user_id, 'notifications'))
    wigo_db.delete(skey('user', user_id, 'conversations'))
    wigo_db.delete(skey('user', user_id, 'tapped'))
    wigo_db.delete(skey('user', user_id, 'votes'))
    wigo_db.delete(skey('user', user_id, 'messages'))


@contextmanager
def user_lock(user_id, timeout=30):
    if Configuration.ENVIRONMENT != 'test':
        from server.db import redis

        with redis.lock('locks:user:{}'.format(user_id), timeout=timeout):
            yield
    else:
        yield


def on_model_change_broadcast(message):
    from server.models import model_cache
    from server.models.group import cache_maker as group_cache_maker

    data = ujson.loads(message['data'])
    logger.debug('evicting {} from cache'.format(data))
    model_cache.invalidate(data['id'])

    if data['type'] == 'Group':
        group_cache_maker.clear()


def wire_event_bus():
    if Configuration.ENVIRONMENT == 'test':
        return

    pubsub_redis_url = urlparse(Configuration.REDIS_URL)
    pubsub_redis = Redis(host=pubsub_redis_url.hostname,
                         port=pubsub_redis_url.port,
                         password=pubsub_redis_url.password)

    pubsub = pubsub_redis.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(model_change=on_model_change_broadcast)

    class WorkerThread(Thread):
        def __init__(self):
            Thread.__init__(self)
            self.daemon = True

        def run(self):
            while pubsub.subscribed:
                message = pubsub.get_message(ignore_subscribe_messages=True)
                if message is None:
                    sleep(2)

    thread = WorkerThread()
    thread.start()


def wire_data_listeners():
    def publish_model_change(instance):
        from server.db import redis

        redis.publish('model_change', ujson.dumps({
            'type': instance.__class__.__name__,
            'id': instance.id
        }))

    def data_save_listener(sender, instance, created):
        if isinstance(instance, User):
            if is_new_user(instance, created):
                new_user(instance.id)

            if instance.status == 'deleted':
                delete_user.delay(instance.id, instance.group_id)

            publish_model_change(instance)

        elif isinstance(instance, Group):
            if created:
                new_group.delay(instance.id)

            publish_model_change(instance)

        elif isinstance(instance, Event):
            event_related_change.delay(instance.group_id, instance.id)

            if not created:
                publish_model_change(instance)

        elif isinstance(instance, Friend) and created:
            if instance.accepted:
                new_friend.delay(instance.user_id, instance.friend_id)
            else:
                instance.friend.track_meta('last_friend_request', epoch(instance.created))

            instance.user.track_meta('last_friend_change')
            instance.friend.track_meta('last_friend_change')

        elif isinstance(instance, Tap):
            instance.user.track_meta('last_tap_change')
        elif isinstance(instance, Block):
            instance.user.track_meta('last_block_change')
        elif isinstance(instance, Invite):
            user_invited.delay(instance.event_id, instance.user_id, instance.invited_id)
        elif isinstance(instance, EventAttendee):
            event_related_change.delay(instance.event.group_id, instance.event_id)
            tell_friends_user_attending.delay(instance.user_id, instance.event_id)
        elif isinstance(instance, EventMessage):
            event_related_change.delay(instance.event.group_id, instance.event_id)
            tell_friends_event_message.delay(instance.id)
        elif isinstance(instance, EventMessageVote):
            tell_friends_about_vote.delay(instance.message_id, instance.user_id)
            event_related_change.delay(instance.message.event.group_id, instance.message.event_id)
        elif isinstance(instance, Message):
            instance.user.track_meta('last_message_change')
            instance.to_user.track_meta('last_message_change')
            instance.to_user.track_meta('last_message_received', epoch(instance.created))

    def data_delete_listener(sender, instance):
        if isinstance(instance, User):
            delete_user.delay(instance.id, instance.group_id)
            publish_model_change(instance)
        elif isinstance(instance, Event):
            event_related_change.delay(instance.group_id, instance.id)
        elif isinstance(instance, EventMessage):
            event_related_change.delay(instance.event.group_id, instance.event_id)
            tell_friends_delete_event_message.delay(instance.user_id, instance.event_id, instance.id)
        elif isinstance(instance, EventAttendee):
            if instance.event is not None:
                event_related_change.delay(instance.event.group_id, instance.event_id)
                tell_friends_user_not_attending.delay(instance.user_id, instance.event_id)
        elif isinstance(instance, Friend):
            delete_friend.delay(instance.user_id, instance.friend_id)
            instance.user.track_meta('last_friend_change')
            instance.friend.track_meta('last_friend_change')
        elif isinstance(instance, Tap):
            instance.user.track_meta('last_tap_change')
        elif isinstance(instance, Block):
            instance.user.track_meta('last_block_change')

    def privacy_changed_listener(sender, instance):
        privacy_changed.delay(instance.id)

    post_model_save.connect(data_save_listener, weak=False)
    post_model_delete.connect(data_delete_listener, weak=False)
    user_privacy_change.connect(privacy_changed_listener, weak=False)

    # listen to the redis event bus for model changes
    wire_event_bus()
