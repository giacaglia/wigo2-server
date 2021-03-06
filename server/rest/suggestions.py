from __future__ import absolute_import

import logging
from random import shuffle
from time import time
from datetime import datetime
from flask import g
from server.db import wigo_db
from server.models import skey
from server.models.user import User
from server.rest import WigoResource, api
from server.security import user_token_required
from server.tasks.predictions import generate_friend_recs, capture_interaction
from utils import partition

logger = logging.getLogger('wigo.suggestions')


@api.route('/users/suggestions')
class UserSuggestionsResource(WigoResource):
    model = User

    def get_limit(self, default=20):
        return super(UserSuggestionsResource, self).get_limit(default)

    @user_token_required
    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    def get(self):
        user = g.user
        generate_friend_recs(user)
        count, page, users = self.select().key(skey(user, 'friend', 'suggestions')).execute()

        if count == 0:
            count, page, users = self.select().group(g.group).execute()
        if count == 0:
            count, page, users = self.select().execute()

        fb, other = partition(users, lambda u: True if hasattr(u, 'score') and u.score >= 10000 else False)

        shuffle(fb)
        shuffle(other)

        users = fb + other

        for u in users:
            if hasattr(u, 'score'):
                score = u.score
                delattr(u, 'score')
                if score >= 10000:
                    score -= 10000
                if score < 100000:
                    u.num_friends_in_common = int(score)
                else:
                    u.num_friends_in_common = 0
            else:
                u.num_friends_in_common = 0

        return self.serialize_list(User, users, count, page), 200, {
            'Cache-Control': 'max-age=60'
        }


@api.route('/users/suggestions/<int:suggest_id>')
class DeleteSuggestionResource(WigoResource):
    model = User

    @user_token_required
    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    def delete(self, suggest_id):
        user = g.user
        wigo_db.sorted_set_remove(skey(user, 'friend', 'suggestions'), suggest_id)
        wigo_db.sorted_set_add(skey(user, 'friend', 'suggestions', 'deleted'), suggest_id, time())
        capture_interaction.delay(g.user.id, suggest_id, datetime.utcnow())
        return {'success': True}
