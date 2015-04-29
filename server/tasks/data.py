from __future__ import absolute_import
import logging

from time import time
from datetime import timedelta, datetime
from rq.decorators import job
from server.db import wigo_db
from server.models.group import Group, get_close_groups
from server.tasks import redis_queues
from server.models import post_model_save, skey
from server.models.event import Event
from utils import epoch

logger = logging.getLogger('wigo.tasks.data')


@job('data', connection=redis_queues, timeout=30, result_ttl=0)
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


def wire_data_listeners():
    def data_listener(sender, instance, created):
        if isinstance(instance, Event):
            event_landed_in_group.delay(instance.group_id)

    post_model_save.connect(data_listener, weak=False)


