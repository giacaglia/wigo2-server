from __future__ import absolute_import

from time import time
from datetime import timedelta, datetime
from rq.decorators import job
from server.db import wigo_db
from server.models.group import Group, get_close_groups
from server.tasks import redis_queues
from server.models import post_model_save, skey
from server.models.event import Event
from utils import epoch


@job('data', connection=redis_queues, timeout=30, result_ttl=0)
def event_landed_in_group(group_id):
    group = Group.find(group_id)

    # tell each close group that this group just had a new event
    for group in get_close_groups(group.latitude, group.longitude, 100):
        key = skey(group, 'close_groups_with_events')
        wigo_db.sorted_set_add(key, group_id, time(), replicate=False)

        # remove groups that haven't had events in 8 days
        wigo_db.sorted_set_remove_by_score(key, 0, epoch(datetime.utcnow() - timedelta(days=8)))


def wire_data_listeners():
    def data_listener(sender, instance, created):
        if isinstance(instance, Event):
            event_landed_in_group.delay(instance.group_id)

    post_model_save.connect(data_listener, weak=False)


