from __future__ import absolute_import
from flask import g

import predictionio
from config import Configuration
from server.models.user import User
from server.rest import WigoResource, api
from server.security import user_token_required

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
        return engine_client.send_query({
            'user': str(g.user.id),
            'num': self.get_limit(),
            'blackList': [g.user.id]
        })