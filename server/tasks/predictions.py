from __future__ import absolute_import

import logging
from newrelic import agent
from playhouse.dataset import DataSet
import predictionio
from datetime import timedelta, datetime
from pytz import UTC, timezone
from rq.decorators import job
from server.db import wigo_db, rate_limit
from server.services.facebook import Facebook, FacebookTokenExpiredException
from server.tasks import predictions_queue, is_new_user
from server.models import post_model_save, skey, DoesNotExist
from server.models.user import User, Tap, Message, Invite, Friend
from config import Configuration

logger = logging.getLogger('wigo.predictions')

if Configuration.ENVIRONMENT != 'test':
    client = predictionio.EventClient(
        access_key=Configuration.PREDICTION_IO_ACCESS_KEY,
        url='http://{}:7070'.format(Configuration.PREDICTION_IO_HOST),
        threads=5,
        qsize=500
    )
else:
    client = None


@agent.background_task()
@job(predictions_queue, timeout=30, result_ttl=0)
def capture_interaction(user_id, with_user_id, t, action='view'):
    if not client:
        return

    logger.debug('capturing prediction event data between {} and {}'.format(user_id, with_user_id))

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


def generate_friend_recs(user, num_friends_to_recommend=100, force=False):
    with rate_limit('gen_f_recs:{}'.format(user.id), timedelta(minutes=10)) as limited:
        if force or not limited:
            _do_generate_friend_recs.delay(user.id, num_friends_to_recommend)


@agent.background_task()
@job(predictions_queue, timeout=600, result_ttl=0)
def _do_generate_friend_recs(user_id, num_friends_to_recommend=100, force=False):
    is_dev = Configuration.ENVIRONMENT == 'dev'

    user = User.find(user_id)

    suggestions_key = skey(user, 'friend', 'suggestions')
    suggested = {}
    blocked = user.get_blocked_ids()

    def is_limited(field, ttl=1):
        last_check = user.get_meta(field)
        if last_check:
            last_check = datetime.utcfromtimestamp(float(last_check))
        if not last_check:
            return False
        return last_check >= (datetime.utcnow() - timedelta(hours=ttl))

    def should_suggest(suggest_id):
        if (suggest_id == user.id) or (suggest_id in suggested) or (suggest_id in blocked):
            return False
        if user.is_friend(suggest_id) or user.is_friend_request_sent(suggest_id):
            return False
        try:
            suggest_user = User.find(suggest_id)
            return suggest_user.status != 'hidden'
        except DoesNotExist:
            return False

    def add_friend(suggest_id, boost=0):
        if boost == 0:
            existing_score = suggested.get(suggest_id, -1)
            if existing_score >= 10000:
                boost = 10000

        score = user.get_num_friends_in_common(suggest_id) + boost
        wigo_db.sorted_set_add(suggestions_key, suggest_id, score, replicate=False)
        suggested[suggest_id] = score

    #################################################
    # first clean up all the old suggestions

    for suggest_id, score in wigo_db.sorted_set_iter(suggestions_key):
        if not should_suggest(suggest_id):
            wigo_db.sorted_set_remove(suggestions_key, suggest_id, replicate=False)
        else:
            # update the scores
            boost = 10000 if score >= 10000 else 0
            score = user.get_num_friends_in_common(suggest_id) + boost
            if score != suggested.get(suggest_id, -1):
                wigo_db.sorted_set_add(suggestions_key, suggest_id, score, replicate=False)
                suggested[suggest_id] = score

    ##################################
    # add facebook friends

    if force or (len(suggested) < num_friends_to_recommend and not is_limited('last_facebook_check')
                 and user.facebook_token_expires < datetime.utcnow()):

        facebook = Facebook(user.facebook_token, user.facebook_token_expires)

        try:
            for fb_friend in facebook.iter('/me/friends?fields=installed', timeout=600):
                facebook_id = fb_friend.get('id')
                try:
                    friend = User.find(facebook_id=facebook_id)
                    if should_suggest(friend.id):
                        add_friend(friend.id, 10000)
                        if len(suggested) >= num_friends_to_recommend:
                            break

                except DoesNotExist:
                    pass

            user.track_meta('last_facebook_check')
        except FacebookTokenExpiredException:
            logger.warn('error finding facebook friends to suggest for user {}, token expired'.format(user_id))
            user.track_meta('last_facebook_check')
            user.facebook_token_expires = datetime.utcnow()
            user.save()
        except Exception:
            logger.exception('error finding facebook friends to suggest for user {}'.format(user_id))

    ##################################
    # add friends of friends

    def each_friends_friend():
        for friend_id in wigo_db.sorted_set_range(skey(user, 'friends'), 0, 50):
            friends_friends = wigo_db.sorted_set_range(skey('user', friend_id, 'friends'), 0, 50)
            for friends_friend in friends_friends:
                if should_suggest(friends_friend):
                    yield friends_friend

    for friends_friend in each_friends_friend():
        num_friends_in_common = user.get_num_friends_in_common(friends_friend)
        if num_friends_in_common > 0:
            add_friend(friends_friend, num_friends_in_common)
            if len(suggested) >= num_friends_to_recommend:
                break

    ##################################
    # add via prediction io

    if force or (len(suggested) < num_friends_to_recommend
                 and not is_limited('last_pio_check')):

        try:
            # flesh out the rest via prediction io
            engine_client = predictionio.EngineClient(
                url='http://{}:{}'.format(Configuration.PREDICTION_IO_HOST,
                                          Configuration.PREDICTION_IO_PORT)
            )

            predictions = engine_client.send_query({
                'user': str(user_id),
                'num': 50,
                'blackList': [str(user_id)]
            })

            for r in predictions.get('itemScores'):
                suggest_id = int(r['item'])
                if should_suggest(suggest_id):
                    add_friend(suggest_id)
                    if len(suggested) >= num_friends_to_recommend:
                        break

            user.track_meta('last_pio_check')
        except Exception, e:
            logger.error('error connecting to prediction.io, {}'.format(e.message))

    ##################################
    # add old friends

    if force or (user.id < 150000 and len(suggested) < num_friends_to_recommend
                 and not is_limited('last_legacy_check')):

        rdbms = DataSet(Configuration.OLD_DATABASE_URL)

        results = rdbms.query("""
            select t1.follow_id from follow t1, follow t2 where
            t1.user_id = {} and t1.user_id = t2.follow_id and t1.follow_id = t2.user_id and
            t1.accepted is True and t2.accepted is True limit 50
        """.format(user.id))

        for result in results:
            suggest_id = result[0]

            # make sure the user exists here
            try:
                User.find(suggest_id)
            except DoesNotExist:
                continue

            if should_suggest(suggest_id):
                add_friend(suggest_id)
                if len(suggested) >= num_friends_to_recommend:
                    break

    num_suggestions = wigo_db.get_sorted_set_size(suggestions_key)
    if num_suggestions > num_friends_to_recommend:
        wigo_db.sorted_set_remove_by_rank(suggestions_key, 0, num_suggestions - num_friends_to_recommend)

    wigo_db.redis.expire(suggestions_key, timedelta(days=30))
    logger.info('generated {} friend suggestions for user {}'.format(len(suggested), user_id))


def wire_predictions_listeners():
    if Configuration.ENVIRONMENT == 'test':
        return

    def predictions_listener(sender, instance, created):
        if isinstance(instance, User):
            if is_new_user(instance, created):
                generate_friend_recs(instance, force=True)
        elif isinstance(instance, Tap):
            capture_interaction.delay(instance.user_id, instance.tapped_id, instance.created)
        elif isinstance(instance, Message):
            capture_interaction.delay(instance.user_id, instance.to_user_id, instance.created)
        elif isinstance(instance, Invite):
            capture_interaction.delay(instance.user_id, instance.invited_id, instance.created)
        elif isinstance(instance, Friend) and instance.accepted:
            capture_interaction.delay(instance.user_id, instance.friend_id, instance.created, action='view')
            capture_interaction.delay(instance.friend_id, instance.user_id, instance.created, action='view')

            capture_interaction.delay(instance.user_id, instance.friend_id, instance.created, action='buy')
            capture_interaction.delay(instance.friend_id, instance.user_id, instance.created, action='buy')

            generate_friend_recs(instance.user)
            generate_friend_recs(instance.friend)

            wigo_db.sorted_set_remove(skey('user', instance.user_id,
                                           'friend', 'suggestions'), instance.friend_id)

            wigo_db.sorted_set_remove(skey('user', instance.friend_id,
                                           'friend', 'suggestions'), instance.user_id)

    post_model_save.connect(predictions_listener, weak=False)
