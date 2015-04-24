from __future__ import absolute_import
from flask import g

import predictionio
from config import Configuration
from server.models.user import User
from server.rest import WigoResource, api
from server.security import user_token_required

engine_client = predictionio.EngineClient(
    url='http://{}:8000'.format(Configuration.PREDICTION_IO_HOST))


@api.route('/users/<user_id>/recommend')
class UserRecommendationsResource(WigoResource):
    model = User

    @user_token_required
    @api.response(200, 'Success', model=User.to_doc_list_model(api))
    def get(self, user_id):
        return engine_client.send_query({
            'user': str(g.user.id),
            'num': self.get_limit(),
            'categories': [str(g.user.group_id)],
            'blackList': [g.user.id]
        })