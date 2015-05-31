from __future__ import absolute_import

import logging

from time import time, sleep
from datetime import timedelta
from flask import g, request, redirect
from flask.ext.restful import abort
from newrelic import agent
from werkzeug.urls import url_encode
from dateutil.parser import parse

from server.db import wigo_db
from server.models import skey, user_eventmessages_key, AlreadyExistsException, DoesNotExist, user_votes_key
from server.models.event import Event, EventMessage, EventAttendee, EventMessageVote
from server.models.group import get_group_by_city_id, Group
from server.rest import WigoDbListResource, WigoDbResource, WigoResource, api, cache_maker, check_last_modified
from server.security import user_token_required
from utils import epoch

logger = logging.getLogger('wigo.web')


@api.route('/events/')
class EventListResource(WigoDbListResource):
    model = Event

    @user_token_required
    @check_last_modified('group', 'last_event_change')
    @api.response(200, 'Success', model=Event.to_doc_list_model(api))
    def get(self, headers):
        user = g.user
        group = g.group

        query = self.select().group(group)
        query = query.min(epoch(group.get_day_end() - timedelta(days=8)))
        query = query.max(epoch(group.get_day_end() + timedelta(minutes=10)))
        count, page, events = query.execute()

        if count == 0 and group.status == 'initializing':
            tries = int(request.args.get('tries', 0))
            if tries < 5:
                sleep(1)  # clients seem to not obey Retry-After, so artificially delay
                request_arguments = request.args.copy().to_dict()
                request_arguments['tries'] = tries
                response = redirect('%s?%s' % (request.path, url_encode(request_arguments)))
                response.headers.add('Retry-After', 1)
                return response
            else:
                # return without "headers" so this response isn't cached
                return self.serialize_list(self.model, events, count, page), 200

        attending_id = g.user.get_attending_id()
        if attending_id:
            try:
                attending = Event.find(attending_id)
                if attending in events:
                    events.remove(attending)
                if 'page' not in request.args:
                    events.insert(0, attending)
                    attending.current_user_attending = True
            except DoesNotExist:
                logger.warn('Event {} not found'.format(attending_id))

        return self.serialize_list(self.model, events, count, page), 200, headers

    @user_token_required
    @api.expect(Event.to_doc_list_model(api))
    @api.response(200, 'Success', model=Event.to_doc_list_model(api))
    def post(self):
        json = request.get_json()
        if 'city_id' in json:
            group = get_group_by_city_id(json['city_id'])
        elif 'group_id' in json:
            group = Group.find(json['group_id'])
        else:
            group = g.group

        try:
            event = Event({
                'name': json.get('name'),
                'group_id': group.id,
                'owner_id': g.user.id,
                'privacy': json.get('privacy') or 'public'
            })

            if 'date' in json:
                date = parse(json.get('date'))
                event.date = group.get_day_start(date)
                event.expires = group.get_day_end(date)
            else:
                event.date = group.get_day_start()
                event.expires = group.get_day_end()

            event.save()
            return self.serialize_list(Event, [event])
        except AlreadyExistsException, e:
            return self.handle_already_exists_exception(e)

    def handle_already_exists_exception(self, e):
        event = e.instance
        if g.user.can_see_event(event) and not g.user.is_attending(event):
            EventAttendee({
                'user_id': g.user.id,
                'event_id': e.instance.id
            }).save()
        return super(EventListResource, self).handle_already_exists_exception(e)


@api.route('/events/<int:model_id>')
@api.response(403, 'If not invited')
class EventResource(WigoDbResource):
    model = Event

    @user_token_required
    @api.response(200, 'Success', model=Event.to_doc_list_model(api))
    def get(self, model_id):
        return super(EventResource, self).get(model_id)

    def check_get(self, event):
        super(EventResource, self).check_get(event)
        if not g.user.can_see_event(event):
            abort(403, message='Can not see event event')

    @user_token_required
    @api.expect(Event.to_doc_list_model(api))
    @api.response(200, 'Success', model=Event.to_doc_list_model(api))
    def post(self, model_id):
        return super(EventResource, self).post(model_id)

    @api.response(501, 'Not implemented')
    def delete(self, model_id):
        abort(501, message='Not implemented')


@api.route('/users/<user_id>/events/')
class UserEventListResource(WigoResource):
    model = Event

    @user_token_required
    @check_last_modified('user', 'last_event_change')
    @api.response(200, 'Success', model=Event.to_doc_list_model(api))
    def get(self, user_id, headers):
        user = g.user
        group = g.group

        query = self.select().user(user)
        query = query.min(epoch(group.get_day_end() - timedelta(days=8)))
        query = query.max(epoch(group.get_day_end() + timedelta(minutes=10)))

        count, page, instances = query.execute()

        return self.serialize_list(self.model, instances, count, page), 200, headers


@api.route('/events/<int:event_id>/attendees')
@api.response(403, 'If not invited')
class EventAttendeeListResource(WigoResource):
    model = EventAttendee

    @user_token_required
    @check_last_modified('group', 'last_event_change')
    @api.response(200, 'Success', model=EventAttendee.to_doc_list_model(api))
    def get(self, event_id, headers):
        event = Event.find(event_id)
        if not g.user.can_see_event(event):
            abort(403, message='Can not see event')
        count, page, instances = self.select().event(event).secure(g.user).execute()
        return self.serialize_list(self.model, instances, count, page)

    @user_token_required
    @api.expect(EventAttendee.to_doc_list_model(api))
    @api.response(200, 'Success', model=EventAttendee.to_doc_list_model(api))
    def post(self, event_id):
        data = {
            'user_id': g.user.id,
            'event_id': event_id
        }
        attendee = self.create(data)
        return {'success': True}

    @user_token_required
    def delete(self, event_id):
        abort(501, message='Not implemented')


@api.route('/events/<int:event_id>/attendees/<user_id>')
@api.response(200, 'Success')
class DeleteAttendeeResource(WigoResource):
    model = EventAttendee

    @user_token_required
    def delete(self, event_id, user_id):
        EventAttendee({
            'user_id': g.user.id,
            'event_id': event_id
        }).delete()

        return {'success': True}


@api.route('/users/<user_id>/events/<int:event_id>/attendees')
@api.response(403, 'If not invited')
class UserEventAttendeeListResource(WigoResource):
    model = EventAttendee

    @user_token_required
    @check_last_modified('user', 'last_event_change')
    @api.response(200, 'Success', model=EventAttendee.to_doc_list_model(api))
    def get(self, user_id, event_id, headers):
        event = Event.find(event_id)
        if not g.user.can_see_event(event):
            abort(403, message='Can not see event')
        count, page, instances = self.select().user(g.user).event(event).execute()
        return self.serialize_list(self.model, instances, count, page), 200, headers


@api.route('/events/<int:event_id>/messages')
@api.response(403, 'If not invited to event')
class EventMessageListResource(WigoResource):
    model = EventMessage

    @user_token_required
    @check_last_modified('group', 'last_event_change')
    @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
    def get(self, event_id, headers):
        event = Event.find(event_id)
        if not g.user.can_see_event(event):
            abort(403, message='Can not see event')
        query = self.select().event(event).secure(g.user)
        if event.is_expired:
            query = query.by_votes()
        count, page, messages = query.execute()
        return self.serialize_list(self.model, messages, count, page), 200, headers

    @user_token_required
    @api.expect(EventMessage.to_doc_list_model(api))
    @api.response(200, 'Success', model=Event.to_doc_list_model(api))
    def post(self, event_id):
        data = dict(request.get_json())
        data['event_id'] = event_id
        message = self.create(data)
        return self.serialize_list(self.model, [message])


@api.route('/users/<user_id>/events/<int:event_id>/messages')
@api.response(403, 'If not invited to event')
class UserEventMessageListResource(WigoResource):
    model = EventMessage

    @user_token_required
    @check_last_modified('user', 'last_event_change')
    @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
    def get(self, user_id, event_id, headers):
        event = Event.find(event_id)
        if not g.user.can_see_event(event):
            abort(403, message='Can not see event')
        query = self.select().event(event).user(g.user).secure(g.user)
        if event.is_expired:
            query = query.by_votes()
        count, page, messages = query.execute()
        return self.serialize_list(self.model, messages, count, page), 200, headers


@api.route('/events/<int:event_id>/messages/<int:message_id>')
@api.response(403, 'If not invited to event')
class EventMessageResource(WigoResource):
    model = EventMessage

    @user_token_required
    @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
    def get(self, event_id, message_id):
        message = self.model.find(self.get_id(message_id))
        event = message.event
        if not g.user.can_see_event(event):
            abort(403, message='Can not see event')
        return self.serialize_list(self.model, [message])

    @user_token_required
    @api.expect(EventMessage.to_doc_list_model(api))
    @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
    def post(self, event_id, message_id):
        message = self.edit(message_id, request.get_json())
        return self.serialize_list(self.model, [message])

    @user_token_required
    def delete(self, event_id, message_id):
        message = self.model.find(self.get_id(message_id))
        self.check_edit(message)
        message.delete()
        return {'success': True}


@api.route('/events/<int:event_id>/messages/meta')
@api.response(200, 'Meta data about event messages')
class EventMessagesMetaListResource(WigoResource):
    model = EventMessage

    @user_token_required
    @check_last_modified('group', 'last_event_change')
    @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
    def get(self, event_id, headers):
        message_meta = {}

        message_ids = wigo_db.sorted_set_range(skey('event', event_id, 'messages'))
        for message_id in message_ids:
            message_meta[message_id] = {
                'num_votes': wigo_db.get_sorted_set_size(skey('eventmessage', message_id, 'votes')),
                'voted': wigo_db.sorted_set_is_member(skey('eventmessage', message_id, 'votes'), g.user.id)
            }

        return message_meta, 200, headers


@api.route('/users/<user_id>/events/<int:event_id>/messages/meta')
@api.response(200, 'Meta data about event messages')
class UserEventMessagesMetaListResource(WigoResource):
    model = EventMessage

    @user_token_required
    @check_last_modified('user', 'last_event_change')
    @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
    def get(self, user_id, event_id, headers):
        message_meta = {}

        message_ids = wigo_db.sorted_set_range(user_eventmessages_key(g.user, event_id))
        for message_id in message_ids:
            message_meta[message_id] = {
                'num_votes': wigo_db.get_sorted_set_size(user_votes_key(g.user, message_id)),
                'voted': wigo_db.sorted_set_is_member(user_votes_key(g.user, message_id), g.user.id)
            }

        return message_meta, 200, headers


@api.route('/events/<int:event_id>/messages/<int:message_id>/votes')
@api.response(403, 'If not invited')
class EventMessageVoteResource(WigoResource):
    model = EventMessageVote

    @user_token_required
    @check_last_modified('group', 'last_event_change')
    @api.response(200, 'Success')
    def get(self, event_id, message_id, headers):
        from server.models.user import User

        event = Event.find(event_id)
        if not g.user.can_see_event(event):
            abort(403, message='Can not see event')
        message = EventMessage.find(message_id)
        count, page, votes = self.select().eventmessage(message).execute()
        return self.serialize_list(User, votes, count, page), 200, headers

    @user_token_required
    @api.response(200, 'Success')
    def post(self, event_id, message_id):
        EventMessageVote({
            'message_id': message_id,
            'user_id': g.user.id
        }).save()
        return {'success': True}


def get_city_events(group, min, max):
    if max > time():
        return __get_city_events(group, min, max)
    else:
        return get_cached_city_events(group, min, max)


@cache_maker.expiring_lrucache(maxsize=5000, timeout=60)
def get_cached_city_events(group, min, max):
    return __get_city_events(group, min, max)


@agent.function_trace()
def __get_city_events(group, min, max):
    query = Event.select().group(group).limit(1000)
    query = query.min(min).max(max)
    return query.execute()
