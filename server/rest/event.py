from __future__ import absolute_import

import math

from flask import g, request
from flask.ext.restful import abort
from repoze.lru import lru_cache
from server.db import wigo_db
from server.models import skey, user_eventmessages_key, AlreadyExistsException, user_attendees_key
from server.models.event import Event, EventMessage, EventAttendee, EventMessageVote
from server.models.group import get_group_by_city_id, Group, get_close_groups_with_events
from server.models.user import User
from server.rest import WigoDbListResource, WigoDbResource, WigoResource, api
from server.security import user_token_required


@api.route('/events/')
class EventListResource(WigoDbListResource):
    model = Event

    @user_token_required
    @api.response(200, 'Success', model=Event.to_doc_list_model(api))
    def get(self):
        limit = self.get_limit()
        page = self.get_page()

        group = g.group

        current_group_id = int(request.args.get('cg', 0))
        current_group = Group.find(current_group_id) if current_group_id else group

        # if this is the first request, get the event the user is currently attending
        attending = get_global_attending_event() if current_group_id == 0 and page == 1 else None

        # start with the users own group no matter what
        groups = [group]

        # append all of the close groups
        close_groups = get_close_groups_with_events(group.latitude, group.longitude)

        if group in close_groups:
            close_groups.remove(group)

        groups.extend(close_groups)

        remaining_groups = groups[groups.index(current_group):]

        all_events = []

        if attending:
            all_events.append(attending)

        for remaining_group in list(remaining_groups):
            query = Event.select().group(remaining_group).limit(limit).page(page)
            count, events = query.execute()
            pages = int(math.ceil(float(count) / limit))

            while True:
                for event in events:
                    if event != attending:
                        all_events.append(event)

                if page >= pages:
                    remaining_groups.remove(remaining_group)
                    page = 1
                    break

                if len(all_events) >= limit:
                    break

                page += 1
                query = query.page(page)
                count, events = query.execute()

            if len(all_events) >= limit:
                break

        next = {}
        if remaining_groups:
            next = {'page': page, 'cg': remaining_groups[0].id}

        return self.serialize_list(Event, all_events, next=next)

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


# noinspection PyUnresolvedReferences
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


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/events/')
class UserEventListResource(WigoResource):
    model = Event

    @user_token_required
    @api.response(200, 'Success', model=Event.to_doc_list_model(api))
    def get(self, user_id):
        count, instances = self.select().user(g.user).execute()
        return self.serialize_list(self.model, instances, count)


# noinspection PyUnresolvedReferences
@api.route('/events/<int:event_id>/attendees')
@api.response(403, 'If not invited')
class EventAttendeeListResource(WigoResource):
    model = EventAttendee

    @user_token_required
    @api.response(200, 'Success', model=EventAttendee.to_doc_list_model(api))
    def get(self, event_id):
        event = Event.find(event_id)
        if not g.user.can_see_event(event):
            abort(403, message='Can not see event')
        count, instances = self.select().event(event).execute()
        return self.serialize_list(self.model, instances, count)

    @user_token_required
    @api.expect(EventAttendee.to_doc_list_model(api))
    @api.response(200, 'Success', model=EventAttendee.to_doc_list_model(api))
    def post(self, event_id):
        data = dict(request.get_json())
        data['event_id'] = event_id
        attendee = self.create(data)
        return {'success': True}

    @user_token_required
    def delete(self, event_id):
        abort(501, message='Not implemented')


# noinspection PyUnresolvedReferences
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


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/events/<int:event_id>/attendees')
@api.response(403, 'If not invited')
class UserEventAttendeeListResource(WigoResource):
    model = EventAttendee

    @user_token_required
    @api.response(200, 'Success', model=EventAttendee.to_doc_list_model(api))
    def get(self, user_id, event_id):
        event = Event.find(event_id)
        if not g.user.can_see_event(event):
            abort(403, message='Can not see event')
        count, instances = self.select().user(g.user).event(event).execute()
        return self.serialize_list(self.model, instances, count)


# noinspection PyUnresolvedReferences
@api.route('/events/<int:event_id>/messages')
@api.response(403, 'If not invited to event')
class EventMessageListResource(WigoResource):
    model = EventMessage

    @user_token_required
    @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
    def get(self, event_id):
        event = Event.find(event_id)
        if not g.user.can_see_event(event):
            abort(403, message='Can not see event')
        count, messages = self.select().event(event).execute()
        return self.serialize_list(self.model, messages, count)

    @user_token_required
    @api.expect(EventMessage.to_doc_list_model(api))
    @api.response(200, 'Success', model=Event.to_doc_list_model(api))
    def post(self, event_id):
        data = dict(request.get_json())
        data['event_id'] = event_id
        message = self.create(data)
        return self.serialize_list(self.model, [message])


# noinspection PyUnresolvedReferences
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


# noinspection PyUnresolvedReferences
@api.route('/events/<int:event_id>/messages/meta')
@api.response(200, 'Meta data about event messages')
class EventMessagesMetaListResource(WigoResource):
    model = EventMessage

    @user_token_required
    @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
    def get(self, event_id):
        message_meta = {}

        message_ids = wigo_db.sorted_set_range(skey('event', event_id, 'messages'), 0, -1)
        for message_id in message_ids:
            message_meta[message_id] = {
                'num_votes': wigo_db.get_sorted_set_size(skey('message', message_id, 'votes')),
                'voted': wigo_db.sorted_set_is_member(skey('message', message_id, 'votes'), g.user.id)
            }

        return message_meta


# noinspection PyUnresolvedReferences
@api.route('/users/<user_id>/events/<int:event_id>/messages/meta')
@api.response(200, 'Meta data about event messages')
class UserEventMessagesMetaListResource(WigoResource):
    model = EventMessage

    @user_token_required
    @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
    def get(self, user_id, event_id):
        message_meta = {}

        message_ids = wigo_db.sorted_set_range(user_eventmessages_key(g.user, event_id), 0, -1)
        for message_id in message_ids:
            message_meta[message_id] = {
                'num_votes': wigo_db.get_sorted_set_size(skey('message', message_id, 'votes')),
                'voted': wigo_db.sorted_set_is_member(skey('message', message_id, 'votes'), g.user.id)
            }

        return message_meta


@api.route('/events/<int:event_id>/messages/<int:message_id>/votes')
@api.response(403, 'If not invited')
class EventMessageVoteResource(WigoResource):
    model = EventMessageVote

    @user_token_required
    @api.response(200, 'Success')
    def post(self, event_id, message_id):
        EventMessageVote({
            'message_id': message_id,
            'user_id': g.user.id
        }).save()
        return {'success': True}


@lru_cache(1000, timeout=60)
def get_global_attendees(event_id):
    from server.db import wigo_db

    return wigo_db.sorted_set_rrange(skey('event', event_id, 'attendees'), 0, -1)


@lru_cache(1000, timeout=60)
def get_global_messages(event_id):
    from server.db import wigo_db

    return wigo_db.sorted_set_rrange(skey('event', event_id, 'messages'), 0, -1)


def get_global_attending_event():
    """ Get the global view of the event the current user is attending. """

    event_id = g.user.get_attending_id()
    if event_id is None:
        return None

    event = Event.find(event_id)
    attending_ids = get_global_attending_attendees(event)
    message_ids = get_global_attending_messages(event)
    event.num_attending = len(attending_ids)

    alimit = int(request.args.get('attendees_limit', 5))
    mlimit = int(request.args.get('messages_limit', 5))

    if attending_ids:
        event.attendees = (len(attending_ids), User.find(attending_ids[0:alimit]))

    if message_ids:
        event.messages = (len(message_ids), EventMessage.find(message_ids[0:mlimit]))

    return event


def get_global_attending_attendees(event):
    """ Get the global view of the attendees of the event the current user is attending. """

    attending_ids = wigo_db.sorted_set_rrange(user_attendees_key(g.user, event), 0, -1)
    attending_set = set(attending_ids)
    for attendee_id in get_global_attendees(event.id):
        if attendee_id not in attending_set:
            attending_ids.append(attendee_id)
    return attending_ids


def get_global_attending_messages(event):
    """ Get the global view of the messages of the event the current user is attending. """

    # TODO need to combine messages in a smarter way. should be ordered by time
    message_ids = wigo_db.sorted_set_rrange(user_eventmessages_key(g.user, event), 0, -1)
    message_set = set(message_ids)
    for message_id in get_global_messages(event.id):
        if message_id not in message_set:
            message_ids.append(message_id)
    return message_ids
