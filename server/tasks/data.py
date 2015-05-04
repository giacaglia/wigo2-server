from __future__ import absolute_import
import logging

from time import time
from datetime import timedelta, datetime
from rq.decorators import job
from server.db import wigo_db
from server.models.group import Group, get_close_groups
from server.models.user import User, Friend
from server.tasks import data_queue
from server.models import post_model_save, skey, user_privacy_change, DoesNotExist, user_eventmessages_key, \
    DEFAULT_EXPIRING_TTL
from server.models.event import Event, EventMessage
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
def privacy_changed(user_id):
    user = User.find(user_id)

    # tell all friends about the privacy change
    for friend_id, score in wigo_db.sorted_set_iter(skey(user, 'friends')):
        if user.privacy == 'public':
            wigo_db.set_remove(skey('user', friend_id, 'friends', 'private'), user_id)
        else:
            wigo_db.set_add(skey('user', friend_id, 'friends', 'private'), user_id)


@job(data_queue, timeout=60, result_ttl=0)
def new_friend(user_id, friend_id):
    """ Process a new friend. """

    user = User.find(user_id)
    friend = User.find(friend_id)

    # tells each friend about the event history of the other
    def capture_attending(u, f):
        for event_id, score in wigo_db.sorted_set_iter(skey(u, 'events')):
            try:
                event = Event.find(event_id)
            except DoesNotExist:
                continue

            if u.is_attending(event) and f.can_see_event(event):
                event.add_to_user_attending(f, u)

    capture_attending(user, friend)
    capture_attending(friend, user)


@job(data_queue, timeout=60, result_ttl=0)
def fill_in_photo_history(event_id, user_id, attendee_id):
    """
    This fills in all of the attendees photos for an event into the users view.
    """

    event = Event.find(event_id)
    user = User.find(user_id)
    attendee = User.find(attendee_id)

    user_messages_key = user_eventmessages_key(user, event)
    attendees_view = wigo_db.sorted_set_iter(user_eventmessages_key(attendee, event))
    for message_id, score in attendees_view:
        message = EventMessage.find(message_id)
        # only care about the messages the user themselves created
        if message.user_id == attendee_id:
            wigo_db.sorted_set_add(user_messages_key, message_id, score)

    wigo_db.expire(user_messages_key, DEFAULT_EXPIRING_TTL)

def wire_data_listeners():
    def data_save_listener(sender, instance, created):
        if isinstance(instance, Friend) and created:
            if instance.accepted:
                try:
                    new_friend.delay(instance.user_id, instance.friend_id)
                except Exception, e:
                    logger.error('error creating new_friend job for ({}, {})'.format(
                        instance.user_id,
                        instance.friend_id
                    ))

        if isinstance(instance, Event):
            try:
                event_landed_in_group.delay(instance.group_id)
            except Exception, e:
                logger.exception('error adding group to close groups, {}'.format(instance.group_id))

    def privacy_changed_listener(sender, instance):
        try:
            privacy_changed.delay(instance.id)
        except Exception, e:
            logger.exception('error creating privacy change job for user {}'.format(instance.id))

    post_model_save.connect(data_save_listener, weak=False)
    user_privacy_change.connect(privacy_changed_listener, weak=False)

