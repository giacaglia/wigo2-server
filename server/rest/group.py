from __future__ import absolute_import

from server.models.group import Group, get_close_cities, get_biggest_close_cities
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

        by_pop = get_biggest_close_cities(group.latitude, group.longitude)

        cities = cities[:9]

        if by_pop and by_pop[0].cityId not in [c.cityId for c in cities]:
            cities.append(by_pop[0])

        return [
            {
                'city_id': c.cityId,
                'name': c.name,
                'population': int(c.population)
            } for c in cities
        ]


