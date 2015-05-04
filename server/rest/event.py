from __future__ import absolute_import
from datetime import datetime, timedelta

import math

from flask import g, request
from flask.ext.restful import abort
from repoze.lru import CacheMaker
from server.db import wigo_db
from server.models import skey, user_eventmessages_key, AlreadyExistsException
from server.models.event import Event, EventMessage, EventAttendee, EventMessageVote
from server.models.group import get_group_by_city_id, Group, get_close_groups_with_events
from server.rest import WigoDbListResource, WigoDbResource, WigoResource, api
from server.security import user_token_required
from utils import epoch

cache_maker = CacheMaker(maxsize=1000, timeout=60)


@api.route('/events/')
class EventListResource(WigoDbListResource):
    model = Event

    @user_token_required
    @api.response(200, 'Success', model=Event.to_doc_list_model(api))
    def get(self):
        limit = self.get_limit()
        page = self.get_page()

        group = g.group

        current_time = int(request.args.get('t', 1))
        current_group = int(request.args.get('g', 0))

        # start with the users own group no matter what
        groups = [group]

        # append all of the close groups
        close_groups = get_close_groups_with_events(group)

        if group in close_groups:
            close_groups.remove(group)

        groups.extend(close_groups)

        now = datetime.utcnow()
        times = [None] + [now - timedelta(days=i) for i in range(9)]

        all_events = []

        time_index = 0
        group_index = 0

        for time_index, time in enumerate(times[current_time:], current_time):
            for group_index, group in enumerate(groups[current_group:], current_group):
                remaining = limit - len(all_events)

                if remaining <= 0:
                    break

                query = Event.select().group(group).limit(remaining).page(page)

                # if time is None, 1st iteration, max is group day end
                max_time = times[time_index - 1]
                if max_time is None:
                    max_time = group.get_day_end() + timedelta(hours=1)  # add 1 hour to account for sub-score

                query = query.min(epoch(time)).max(epoch(max_time))
                count, page, events = query.execute()

                pages = int(math.ceil(float(count) / remaining))

                while count > 0:
                    for event in events:
                        all_events.append(event)

                    if page >= pages:
                        page = 1
                        current_group += 1
                        break

                    page += 1

                    # if the result set is full, break out
                    if len(all_events) >= limit:
                        break

                    query = query.page(page)
                    count, page, events = query.execute()
                else:
                    page = 1
                    current_group += 1

            if len(all_events) >= limit:
                break
            else:
                # done with all the groups, so go back to group 0, page 1 for the next time
                current_group = 0
                page = 1

        next = {}
        if len(all_events) >= limit and time_index < len(times) and current_group < len(groups):
            next = {'page': page, 't': time_index, 'g': current_group}

        # if this is the first request, get the event the user is currently attending
        if current_group == 0 and page == 1:
            attending_id = g.user.get_attending_id()
            if attending_id:
                attending = Event.find(attending_id)
                if attending in all_events:
                    all_events.remove(attending)
                all_events.insert(0, attending)
                attending.current_user_attending = True

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
    @api.response(200, 'Success', model=Event.to_doc_list_model(api))
    def get(self, user_id):
        count, page, instances = self.select().user(g.user).execute()
        return self.serialize_list(self.model, instances, count, page)


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
    @api.response(200, 'Success', model=EventAttendee.to_doc_list_model(api))
    def get(self, user_id, event_id):
        event = Event.find(event_id)
        if not g.user.can_see_event(event):
            abort(403, message='Can not see event')
        count, page, instances = self.select().user(g.user).event(event).execute()
        return self.serialize_list(self.model, instances, count, page)


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
        count, page, messages = self.select().event(event).secure(g.user).execute()
        return self.serialize_list(self.model, messages, count, page)

    @user_token_required
    @api.expect(EventMessage.to_doc_list_model(api))
    @api.response(200, 'Success', model=Event.to_doc_list_model(api))
    def post(self, event_id):
        data = dict(request.get_json())
        data['event_id'] = event_id
        message = self.create(data)
        return self.serialize_list(self.model, [message])


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
    @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
    def get(self, event_id):
        message_meta = {}

        message_ids = wigo_db.sorted_set_range(skey('event', event_id, 'messages'))
        for message_id in message_ids:
            message_meta[message_id] = {
                'num_votes': wigo_db.get_sorted_set_size(skey('message', message_id, 'votes')),
                'voted': wigo_db.sorted_set_is_member(skey('message', message_id, 'votes'), g.user.id)
            }

        return message_meta


@api.route('/users/<user_id>/events/<int:event_id>/messages/meta')
@api.response(200, 'Meta data about event messages')
class UserEventMessagesMetaListResource(WigoResource):
    model = EventMessage

    @user_token_required
    @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
    def get(self, user_id, event_id):
        message_meta = {}

        message_ids = wigo_db.sorted_set_range(user_eventmessages_key(g.user, event_id))
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
