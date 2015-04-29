from __future__ import absolute_import

import logging
import predictionio
from pytz import UTC, timezone

from rq.decorators import job

from server.tasks import redis_queues
from server.models import post_model_save
from server.models.user import User, Tap, Message, Invite, Friend
from config import Configuration

logger = logging.getLogger('wigo.predictions')

client = predictionio.EventClient(
  access_key=Configuration.PREDICTION_IO_ACCESS_KEY,
  url='http://{}:7070'.format(Configuration.PREDICTION_IO_HOST),
  threads=5,
  qsize=500
)

@job('predictions', connection=redis_queues, timeout=30, result_ttl=0)
def capture_interaction(user_id, with_user_id, t, action='view'):
    logger.info('capturing prediction event data between {} and {}'.format(user_id, with_user_id))

    user = User.find(user_id)
    with_user = User.find(with_user_id)

    tz = timezone(with_user.group.timezone)
    event_time = t.replace(tzinfo=UTC).astimezone(tz)

    r = client.set_user(user_id, event_time=user.created.replace(tzinfo=UTC).astimezone(tz))

    if r.status not in (200, 201):
        raise Exception('Error returned from prediction io')

    r = client.set_item(with_user_id, {'categories': [str(with_user.group_id)]},
                        event_time=with_user.created.replace(tzinfo=UTC).astimezone(tz))

    if r.status not in (200, 201):
        raise Exception('Error returned from prediction io')

    r = client.record_user_action_on_item(action, user_id, with_user_id, event_time=event_time)
    if r.status not in (200, 201):
        raise Exception('Error returned from prediction io')


def wire_predictions_listeners():
    def predictions_listener(sender, instance, created):
        if isinstance(instance, Tap):
            capture_interaction.delay(user_id=instance.user_id, with_user_id=instance.tapped_id,
                                      t=instance.created)
        elif isinstance(instance, Message):
            capture_interaction.delay(user_id=instance.user_id, with_user_id=instance.to_user_id,
                                      t=instance.created)
        elif isinstance(instance, Invite):
            capture_interaction.delay(user_id=instance.user_id, with_user_id=instance.invited_id,
                                      t=instance.created)
        elif isinstance(instance, Friend) and instance.accepted:
            capture_interaction.delay(user_id=instance.user_id, with_user_id=instance.friend_id,
                                      t=instance.created, action='buy')
            capture_interaction.delay(user_id=instance.friend_id, with_user_id=instance.user_id,
                                      t=instance.created, action='buy')

    post_model_save.connect(predictions_listener, weak=False)

