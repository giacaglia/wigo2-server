from __future__ import absolute_import

from bigquery.client import get_client

BIGQUERY_PROJECT_ID = 1001456183496
BIGQUERY_SERVICE_ACCOUNT = '1001456183496-i56f7tujm9d6u0jjpfj9pm3velbjtegr@developer.gserviceaccount.com'


def get_bigquery_client():
    return get_client(BIGQUERY_PROJECT_ID, service_account=BIGQUERY_SERVICE_ACCOUNT,
                      private_key_file='data/wigo2.key.p12')


def import_friend_interactions(user_id, days_back=30):
    bq = get_bigquery_client()
    results = bq.query("""
        SELECT
          user.id, target_user.id, category, count(*) num_actions
        FROM
          wigo2_production_stream.2015_01_01
        WHERE
          type = 'action'
          AND user.id = {}
          AND category IN ('friend_request_accept', 'friend_request_sent', 'tap', 'message_sent')
          AND user.id IS NOT NULL and target_user.id IS NOT NULL
          AND time > DATE_ADD(CURRENT_TIMESTAMP(), -{}, "DAY")
        GROUP BY user.id, target_user.id, category
        ORDER BY user.id, target_user.id, category DESC
    """.format(user_id, days_back), timeout=30)

    print results
