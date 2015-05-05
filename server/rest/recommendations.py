from __future__ import absolute_import
from collections import Counter

import logging
import predictionio
from flask import g
from repoze.lru import CacheMaker
from config import Configuration
from server.db import wigo_db
from server.models import skey
from server.models.user import User
from server.rest import WigoResource, api
from server.security import user_token_required

logger = logging.getLogger('wigo.recommendations')

cache_maker = CacheMaker(maxsize=1000, timeout=60)

engine_client = predictionio.EngineClient(
    url='http://{}:{}'.format(Configuration.PREDICTION_IO_HOST,
                              Configuration.PREDICTION_IO_PORT)
)


@api.route('/users/suggestions')
class UserRecommendationsResource(WigoResource):
    model = User

    @user_token_required
    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    def get(self):
        page = self.get_page()
        limit = self.get_limit()
        current_user = g.user
        num_remaining = limit

        suggested = set()
        users = []

        blocked = current_user.get_blocked_ids()

        def should_suggest(suggest_id):
            if (suggest_id == current_user.id) or (suggest_id in suggested) or (suggest_id in blocked):
                return False
            if current_user.is_friend(suggest_id) or current_user.is_friend_request_sent(suggest_id):
                return False
            return True

        if page == 1:
            # prediction io search
            user_ids = get_user_predictions(current_user.id, limit)
            user_ids = [uid for uid in user_ids if should_suggest(uid)]
            suggested.update(user_ids)
            users.extend(User.find(user_ids))
            num_remaining -= len(users)

        if num_remaining > 0:
            # friends of friends search
            start = 0
            completed_friends = set()
            to_fetch = Counter()

            while len(to_fetch) < num_remaining:
                found_something = False
                friends = wigo_db.sorted_set_iter(skey(current_user, 'friends'), count=20)
                for friend_id, score in friends:
                    if friend_id not in completed_friends:
                        friends_friends = wigo_db.sorted_set_range(
                            skey('user', friend_id, 'friends'), start, start+5)
                        if friends_friends:
                            found_something = True
                            for friends_friend in friends_friends:
                                if should_suggest(friends_friend):
                                    to_fetch[friends_friend] += 1
                                    suggested.add(friends_friend)
                                    if len(to_fetch) >= num_remaining:
                                        break
                        else:
                            completed_friends.add(friend_id)

                    if len(to_fetch) >= num_remaining:
                        break

                if found_something:
                    start += 6
                else:
                    break

            to_fetch_ordered = [u[0] for u in to_fetch.most_common()]
            users.extend(User.find(to_fetch_ordered))
            num_remaining -= len(to_fetch_ordered)

        return self.serialize_list(User, users)


@cache_maker.expiring_lrucache(maxsize=5000, timeout=60 * 60)
def get_user_predictions(user_id, limit):
    try:
        predictions = engine_client.send_query({
            'user': str(user_id),
            'num': limit,
            'blackList': [str(user_id)]
        })

        return [int(r['item']) for r in predictions.get('itemScores')]
    except Exception, e:
        logger.exception('error fetching predictions')
        return []