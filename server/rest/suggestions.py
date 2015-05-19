from __future__ import absolute_import

import logging
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

        return self.serialize_list(User, users, count, page), 200, {
            'Cache-Control': 'max-age=60'
        }
