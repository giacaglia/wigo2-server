from __future__ import absolute_import

from server.models.group import Group, get_close_cities, get_close_groups
from server.rest import WigoResource, api, WigoDbListResource
from server.security import user_token_required


@api.route('/groups')
class GroupsResource(WigoDbListResource):
    model = Group


@api.route('/groups/<int:group_id>/close/groups')
class CloseGroupsResource(WigoResource):
    model = Group

    @user_token_required
    @api.response(200, 'Success')
    def get(self, group_id):
        group = Group.find(group_id)
        groups = get_close_groups(group.latitude, group.longitude)
        return self.serialize_list(self.model, groups)


@api.route('/groups/<int:group_id>/close/cities')
class CloseCitiesResource(WigoResource):
    model = Group

    @user_token_required
    @api.response(200, 'Success')
    def get(self, group_id):
        group = Group.find(group_id)

        cities = get_close_cities(group.latitude, group.longitude)

        return [{'city_id': c.city_id,
                 'name': c.name,
                 'population': int(c.population)} for c in cities]
