from __future__ import absolute_import

import logging

from itertools import groupby
from bigquery.client import get_client
from datetime import timedelta, datetime
from rq.decorators import job
from newrelic import agent

from server.db import scheduler, wigo_db
from server.models import skey
from server.tasks import predictions_queue

BIGQUERY_PROJECT_ID = 1001456183496
BIGQUERY_SERVICE_ACCOUNT = '1001456183496-i56f7tujm9d6u0jjpfj9pm3velbjtegr@developer.gserviceaccount.com'

logger = logging.getLogger('wigo.analytics')


def get_bigquery_client():
    return get_client(BIGQUERY_PROJECT_ID, service_account=BIGQUERY_SERVICE_ACCOUNT,
                      private_key_file='data/wigo2.key.p12')


@agent.background_task()
@job(predictions_queue, timeout=30, result_ttl=0)
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
                       args=[job_id], result_ttl=0, timeout=600)


@agent.background_task()
@job(predictions_queue, timeout=600, result_ttl=0)
def check_friend_interactions_job(job_id):
    logger.debug('checking bigquery results for job {}'.format(job_id))
    bq = get_bigquery_client()
    complete, row_count = bq.check_job(job_id)
    if complete:
        logger.info('importing interaction scores, {} rows'.format(row_count))
        results = bq.get_query_rows(job_id)

        # zunionstore test_union 1 test_union WEIGHTS 0
        for user_id, scores in groupby(results, lambda r: r['user_id']):
            friends = wigo_db.sorted_set_rrange(skey('user', user_id, 'friends'))
            top_friends_key = skey('user', user_id, 'friends', 'top')

            # 0 out all of the existing top friend scores
            wigo_db.redis.tag_zunionstore(top_friends_key, {top_friends_key: 0})

            with wigo_db.transaction(commit_on_select=False):
                for score in scores:
                    friend_id = score['target_user_id']
                    if friend_id in friends:
                        score = score['score']
                        wigo_db.sorted_set_add(top_friends_key, friend_id, score)

        logger.info('finished importing interaction scores')

    else:
        scheduler.schedule(datetime.utcnow() + timedelta(seconds=5),
                           check_friend_interactions_job,
                           args=[job_id], result_ttl=0, timeout=30)
