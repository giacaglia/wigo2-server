from __future__ import absolute_import

import logging

from itertools import groupby
from random import random
from bigquery.client import get_client
from datetime import timedelta, datetime
from rq.decorators import job
from newrelic import agent

from server.db import scheduler, wigo_db
from server.models import skey
from server.models.user import User
from server.tasks import predictions_queue

BIGQUERY_PROJECT_ID = 1001456183496
BIGQUERY_SERVICE_ACCOUNT = '1001456183496-i56f7tujm9d6u0jjpfj9pm3velbjtegr@developer.gserviceaccount.com'

logger = logging.getLogger('wigo.analytics')


def get_bigquery_client():
    return get_client(BIGQUERY_PROJECT_ID, service_account=BIGQUERY_SERVICE_ACCOUNT,
                      private_key_file='data/wigo2.key.p12')


def import_friend_interactions():
    bq = get_bigquery_client()
    job_id, results = bq.query("""
        SELECT
          user.id, target_user.id, SUM(count) score
        FROM (
          SELECT
            (30 - DATEDIFF(time, DATE_ADD(CURRENT_TIMESTAMP(), -30, "DAY"))) days_ago,
            user.id, target_user.id, COUNT(DISTINCT(category)) count
          FROM
            wigo2_production_stream.2015_01_01
          WHERE
            type = 'action'
            AND category IN (
              'friend_request_accept',
              'friend_request_sent',
              'tap',
              'message_sent'
            )
            AND user.id IS NOT NULL
            AND target_user.id IS NOT NULL
            AND time > DATE_ADD(CURRENT_TIMESTAMP(), -30, "DAY")
          GROUP BY days_ago, user.id, target_user.id
        )
        GROUP BY user.id, target_user.id
        HAVING score > 1
        ORDER BY user.id, target_user.id
    """)

    scheduler.schedule(datetime.utcnow() + timedelta(seconds=5),
                       check_friend_interactions_job,
                       args=[job_id], result_ttl=0, timeout=600, queue_name='predictions')


@agent.background_task()
@job(predictions_queue, timeout=600, result_ttl=0)
def check_friend_interactions_job(job_id):
    logger.debug('checking bigquery results for job {}'.format(job_id))
    bq = get_bigquery_client()
    complete, row_count = bq.check_job(job_id)
    if complete:
        logger.info('importing interaction scores, {} rows'.format(row_count))
        results = bq.get_query_rows(job_id)

        for user_id, user_results in groupby(results, lambda r: r['user_id']):
            scores = {s['target_user_id']: s['score'] for s in user_results}
            update_top_friends(user_id, scores)

        logger.info('finished importing interaction scores')

    else:
        scheduler.schedule(datetime.utcnow() + timedelta(seconds=5),
                           check_friend_interactions_job,
                           args=[job_id], result_ttl=0, timeout=30)


def update_top_friends(user_id, interaction_scores):
    last_active_window = datetime.utcnow() - timedelta(days=30)

    friend_ids = wigo_db.sorted_set_rrange(skey('user', user_id, 'friends'))
    friends = User.find(friend_ids)

    # remove missing friends
    with wigo_db.transaction(commit_on_select=False):
        for index, f in enumerate(friends):
            if f is None:
                friend_id = friend_ids[index]
                if not wigo_db.exists(skey('user', friend_id)):
                    wigo_db.sorted_set_remove(skey('user', user_id, 'friends'), friend_id)
                    wigo_db.set_remove(skey('user', user_id, 'friends', 'private'), friend_id)
                    for type in ['top', 'alpha', 'friend_requests', 'friend_requested']:
                        wigo_db.sorted_set_remove(skey('user', user_id, 'friends', type), friend_id)

    # remove nulls
    friends = [f for f in friends if f is not None]

    # get all my friends last active dates
    p = wigo_db.redis.pipeline()
    for index, f in enumerate(friends):
        p.hget(skey(f, 'meta'), 'last_active')
    last_active_dates = p.execute()
    for index, f in enumerate(friends):
        last_active = last_active_dates[index]
        last_active = datetime.utcfromtimestamp(float(last_active)) if last_active else last_active_window
        f.last_active = last_active

    # bucket and score
    def get_score(f):
        score = 1
        if f.gender == 'female':
            score += 2
        if f.last_active > last_active_window:
            score += 2
        return score

    buckets = [[], [], [], []]
    for f in friends:
        if f.id in friends:
            score = interaction_scores.get(f.id, 0)
            if score == 0:
                buckets[0].append(f)
            elif score <= 3:
                buckets[1].append(f)
            elif score <= 15:
                buckets[2].append(f)
            else:
                buckets[3].append(f)

    for b in buckets:
        b.sort(key=lambda f: random() * get_score(f))

    # 0 out all of the existing top friend scores
    top_friends_key = skey('user', user_id, 'friends', 'top')
    wigo_db.redis.tag_zunionstore(top_friends_key, {top_friends_key: 0})

    with wigo_db.transaction(commit_on_select=False):
        score = 0
        for b in buckets:
            for f in b:
                wigo_db.sorted_set_add(top_friends_key, f.id, score)
                score += 1


def import_active_user_ids(window=2):
    bq = get_bigquery_client()
    job_id, results = bq.query("""
        SELECT user.id, TIMESTAMP_TO_SEC(max(time)) last_active
        FROM wigo2_production_stream.2015_01_01
        WHERE time > DATE_ADD(CURRENT_TIMESTAMP(), -{}, "DAY")
        GROUP BY user.id
    """.format(window))

    scheduler.schedule(datetime.utcnow() + timedelta(seconds=5),
                       check_active_users_job,
                       args=[job_id], result_ttl=0, timeout=600, queue_name='predictions')


@agent.background_task()
@job(predictions_queue, timeout=600, result_ttl=0)
def check_active_users_job(job_id):
    logger.debug('checking bigquery results for job {}'.format(job_id))
    bq = get_bigquery_client()
    complete, row_count = bq.check_job(job_id)
    if complete:
        logger.info('importing active user records, {} rows'.format(row_count))
        results = bq.get_query_rows(job_id)

        with wigo_db.transaction(commit_on_select=False):
            for record in results:
                user_id = record['user_id']
                last_active = record['last_active']
                wigo_db.get_redis(True).hset(skey('user', user_id, 'meta'), 'last_active', last_active)

        logger.info('finished importing active user records')

    else:
        scheduler.schedule(datetime.utcnow() + timedelta(seconds=5),
                           check_friend_interactions_job,
                           args=[job_id], result_ttl=0, timeout=30)
