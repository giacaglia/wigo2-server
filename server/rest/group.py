from __future__ import absolute_import

from server.models.group import Group, get_close_cities
from server.rest import WigoResource, api
from server.security import user_token_required

@api.route('/groups/<int:group_id>/close/cities')
class CloseCitiesResource(WigoResource):
    model = Group

    @user_token_required
    @api.response(200, 'Success')
    def get(self, group_id):
        group = Group.find(group_id)

        cities = get_close_cities(group.latitude, group.longitude)

        return [
            {
                'city_id': c.city_id,
                'name': c.name,
                'population': int(c.population)
            } for c in cities
        ]


