from __future__ import absolute_import

import logging
import ujson

from random import randint
from threading import Thread
from time import time, sleep
from datetime import datetime
from contextlib import contextmanager
from urlparse import urlparse
from redis import Redis
from rq.decorators import job
from config import Configuration

from server.db import wigo_db, scheduler, redis
from server.models.group import Group, get_close_groups
from server.models.user import User, Friend, Invite, Tap, Block, Message
from server.tasks import data_queue
from server.models import post_model_save, skey, user_privacy_change, DoesNotExist, post_model_delete, \
    user_attendees_key
from server.models.event import Event, EventMessage, EventMessageVote, EventAttendee
from utils import epoch

EVENT_CHANGE_TIME_BUFFER = 60

logger = logging.getLogger('wigo.tasks.data')


def new_user(user_id, score=None):
    user_queue_key = skey('user_queue')

    if score is None:
        last_waiting = wigo_db.sorted_set_range(user_queue_key, -1, -1, True)
        if last_waiting:
            last_waiting_score = last_waiting[0][1]
            if last_waiting_score > (time() + (60 * 60 * 11)):
                score = last_waiting_score + 10
            else:
                score = last_waiting_score + randint(60, 60 * 60)
        else:
            score = time() + randint(120, 60 * 60)

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
    logger.info('new group {} created, importing events'.format(group.name))
    num_imported = 0

    for close_group in get_close_groups(group.latitude, group.longitude, 100):
        if close_group.id == group.id:
            continue

        for event in Event.select().group(close_group):
            event.update_global_events(group=group)
            num_imported += 1

    logger.info('imported {} events into group {}'.format(num_imported, group.name))
    group.track_meta('last_event_change', time(), expire=None)


@job(data_queue, timeout=30, result_ttl=0)
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

    lock = redis.lock('locks:group_event_change:{}'.format(group_id), timeout=60)
    if lock.acquire(blocking=False):
        try:
            group = Group.find(group_id)

            # add to the time in case other changes come in while this lock is taken,
            # or in case the job queues get backed up
            group.track_meta('last_event_change', time() + EVENT_CHANGE_TIME_BUFFER, expire=None)

            radius = 100
            population = group.population or 50000
            if population < 50000:
                radius = 40
            elif population < 100000:
                radius = 60

            for close_group in get_close_groups(group.latitude, group.longitude, radius):
                if close_group.id == group.id:
                    continue

                # index this event into the close group
                if event.deleted is False:
                    event.update_global_events(group=close_group)
                else:
                    event.remove_index(group=close_group)

                # clean out old events
                event.clean_old(skey(close_group, 'events'))

                # track the change for the group
                close_group.track_meta('last_event_change', time() + EVENT_CHANGE_TIME_BUFFER, expire=None)

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


@job(data_queue, timeout=60, result_ttl=0)
def new_friend(user_id, friend_id):
    user = User.find(user_id)
    friend = User.find(friend_id)

    # tells each friend about the event history of the other
    def capture_history(u, f):
        with user_lock(f.id):
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
        with user_lock(f.id):
            for message_id, score in wigo_db.sorted_set_iter(skey(u, 'event_messages')):
                try:
                    message = EventMessage.find(message_id)
                    message.remove_for_user(f)
                except DoesNotExist:
                    pass

            for event_id, score in wigo_db.sorted_set_iter(skey(f, 'events')):
                try:
                    event = Event.find(event_id)
                    if wigo_db.sorted_set_is_member(user_attendees_key(f, event), u.id):
                        event.remove_from_user_attending(f, u)
                except DoesNotExist:
                    pass

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


@contextmanager
def user_lock(user_id):
    if Configuration.ENVIRONMENT != 'test':
        from server.db import redis

        with redis.lock('locks:user:{}'.format(user_id), timeout=30):
            yield
    else:
        yield


def on_model_change_broadcast(message):
    from server.models import model_cache

    data = ujson.loads(message['data'])
    logger.debug('evicting {} from cache'.format(data))
    model_cache.invalidate(data['id'])


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
            publish_model_change(instance)

            if created:
                new_user(instance.id)

        elif isinstance(instance, Group):
            if created:
                new_group.delay(instance.id)
            publish_model_change(instance)
        elif isinstance(instance, Friend) and created:
            if instance.accepted:
                new_friend.delay(instance.user_id, instance.friend_id)
            else:
                instance.friend.track_meta('last_friend_request', epoch(instance.created))

            instance.user.track_meta('last_friend', epoch(instance.created))
            instance.friend.track_meta('last_friend', epoch(instance.created))
        elif isinstance(instance, Tap):
            instance.user.track_meta('last_tap', epoch(instance.created))
        elif isinstance(instance, Block):
            instance.user.track_meta('last_block', epoch(instance.created))
        elif isinstance(instance, Event):
            event_related_change.delay(instance.group_id, instance.id)
        elif isinstance(instance, Invite):
            user_invited.delay(instance.event_id, instance.user_id, instance.invited_id)
        elif isinstance(instance, EventAttendee):
            event_related_change.delay(instance.event.group_id, instance.event_id)
            tell_friends_user_attending.delay(instance.user_id, instance.event_id)
        elif isinstance(instance, EventMessage):
            event_related_change.delay(instance.event.group_id, instance.event_id)
            tell_friends_event_message.delay(instance.id)
        elif isinstance(instance, EventMessageVote):
            event_related_change.delay(instance.message.event.group_id, instance.message.event_id)
            tell_friends_about_vote.delay(instance.message_id, instance.user_id)
        elif isinstance(instance, Message):
            instance.user.track_meta('last_message', epoch(instance.created))
            instance.to_user.track_meta('last_message', epoch(instance.created))
            instance.to_user.track_meta('last_message_received', epoch(instance.created))

    def data_delete_listener(sender, instance):
        if isinstance(instance, Event):
            event_related_change.delay(instance.group_id, instance.id)
        elif isinstance(instance, EventMessage):
            event_related_change.delay(instance.event.group_id, instance.event_id)
            tell_friends_delete_event_message.delay(instance.user_id, instance.event_id, instance.id)
        elif isinstance(instance, EventAttendee):
            event_related_change.delay(instance.event.group_id, instance.event_id)
            tell_friends_user_not_attending.delay(instance.user_id, instance.event_id)
        elif isinstance(instance, Friend):
            delete_friend.delay(instance.user_id, instance.friend_id)
            instance.user.track_meta('last_friend')
            instance.friend.track_meta('last_friend')
        elif isinstance(instance, Tap):
            instance.user.track_meta('last_tap')
        elif isinstance(instance, Block):
            instance.user.track_meta('last_block')

    def privacy_changed_listener(sender, instance):
        privacy_changed.delay(instance.id)

    post_model_save.connect(data_save_listener, weak=False)
    post_model_delete.connect(data_delete_listener, weak=False)
    user_privacy_change.connect(privacy_changed_listener, weak=False)

    # listen to the redis event bus for model changes
    wire_event_bus()
