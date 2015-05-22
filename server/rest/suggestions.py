from __future__ import absolute_import

import logging
from random import shuffle
from flask import g
from server.models import skey
from server.models.user import User
from server.rest import WigoResource, api
from server.security import user_token_required
from server.tasks.predictions import generate_friend_recs

logger = logging.getLogger('wigo.suggestions')


@api.route('/users/suggestions')
class UserSuggestionsResource(WigoResource):
    model = User

    @user_token_required
    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    def get(self):
        user = g.user
        generate_friend_recs(user_id=user.id)
        count, page, users = self.select().key(skey(user, 'friend', 'suggestions')).execute()

        if count == 0:
            count, page, users = self.select().group(g.group).execute()
        if count == 0:
            count, page, users = self.select().execute()

        shuffle(users)

        for u in users:
            if hasattr(u, 'score'):
                score = u.score
                delattr(u, 'score')
                u.num_friends_in_common = int(score)
            else:
                u.num_friends_in_common = 0

        return self.serialize_list(User, users, count, page), 200, {
            'Cache-Control': 'max-age=60'
        }
