from __future__ import absolute_import
import logging

from time import time
from datetime import timedelta, datetime
from rq.decorators import job
from server.db import wigo_db
from server.models.group import Group, get_close_groups
from server.models.user import User
from server.tasks import data_queue
from server.models import post_model_save, skey, user_privacy_change
from server.models.event import Event
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


def wire_data_listeners():
    def data_save_listener(sender, instance, created):
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
