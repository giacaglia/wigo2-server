from __future__ import absolute_import
from flask import g, request
from flask.ext.restful import abort

from server.models.event import Event, EventMessage, EventAttendee
from server.rest import WigoDbListResource, WigoDbResource, WigoResource
from server.security import user_token_required


# noinspection PyUnresolvedReferences
def setup_event_resources(api):
    @api.route('/api/events/')
    class EventListResource(WigoDbListResource):
        model = Event

        @user_token_required
        @api.response(200, 'Success', model=Event.to_doc_list_model(api))
        def get(self):
            count, instances = self.select().group(g.group).execute()
            return self.serialize_list(self.model, instances, count)

        @user_token_required
        @api.expect(Event.to_doc_list_model(api))
        @api.response(200, 'Success', model=Event.to_doc_list_model(api))
        def post(self):
            return super(EventListResource, self).post()

        def handle_already_exists_exception(self, e):
            event = e.instance
            if g.user.is_invited(event) and not g.user.is_attending(event):
                EventAttendee({
                    'user_id': g.user.id,
                    'event_id': e.instance.id
                }).save()
            return super(EventListResource, self).handle_already_exists_exception(e)


    @api.route('/api/events/<int:model_id>')
    @api.response(403, 'If not invited')
    class EventResource(WigoDbResource):
        model = Event

        @user_token_required
        @api.response(200, 'Success', model=Event.to_doc_list_model(api))
        def get(self, model_id):
            return super(EventResource, self).get(model_id)

        def check_get(self, event):
            super(EventResource, self).check_get(event)
            if not g.user.is_invited(event):
                abort(403, message='Not invited to event')

        @user_token_required
        @api.expect(Event.to_doc_list_model(api))
        @api.response(200, 'Success', model=Event.to_doc_list_model(api))
        def post(self, model_id):
            return super(EventResource, self).post(model_id)

        @api.response(501, 'Not implemented')
        def delete(self, model_id):
            abort(501, message='Not implemented')


    @api.route('/api/users/<user_id>/events/')
    class UserEventListResource(WigoResource):
        model = Event

        @user_token_required
        @api.response(200, 'Success', model=Event.to_doc_list_model(api))
        def get(self, user_id):
            count, instances = self.select().user(g.user).execute()
            return self.serialize_list(self.model, instances, count)


    @api.route('/api/events/<int:event_id>/attendees')
    @api.response(403, 'If not invited')
    class EventAttendeeListResource(WigoResource):
        model = EventAttendee

        @user_token_required
        @api.response(200, 'Success', model=EventAttendee.to_doc_list_model(api))
        def get(self, event_id):
            event = Event.find(event_id)
            if not g.user.is_invited(event):
                abort(403, message='Not invited to event')
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


    @api.route('/api/events/<int:event_id>/attendees/<user_id>')
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


    @api.route('/api/users/<user_id>/events/<int:event_id>/attendees')
    @api.response(403, 'If not invited')
    class UserEventAttendeeListResource(WigoResource):
        model = EventAttendee

        @user_token_required
        @api.response(200, 'Success', model=EventAttendee.to_doc_list_model(api))
        def get(self, user_id, event_id):
            event = Event.find(event_id)
            if not g.user.is_invited(event):
                abort(403, message='Not invited to event')
            count, instances = self.select().user(g.user).event(event).execute()
            return self.serialize_list(self.model, instances, count)


    @api.route('/api/events/<int:event_id>/messages')
    @api.response(403, 'If not invited to event')
    class EventMessageListResource(WigoResource):
        model = EventMessage

        @user_token_required
        @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
        def get(self, event_id):
            event = Event.find(event_id)
            if not g.user.is_invited(event):
                abort(403, message='Not invited to event')
            count, messages = self.select().event(event).execute()
            return self.serialize_list(self.model, messages, count)

        @user_token_required
        @api.expect(EventMessage.to_doc_list_model(api))
        @api.response(200, 'Success', model=Event.to_doc_list_model(api))
        def post(self, event_id):
            data = dict(request.get_json())
            data['event_id'] = event_id
            message = self.create(data)
            return self.serialize_list(self.model, [message], 1)


    @api.route('/api/events/<int:event_id>/messages/<int:message_id>')
    @api.response(403, 'If not invited to event')
    class EventMessageResource(WigoResource):
        model = EventMessage

        @user_token_required
        @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
        def get(self, event_id, message_id):
            message = self.model.find(self.get_id(message_id))
            event = message.event
            if not g.user.is_invited(event):
                abort(403, message='Not invited to event')
            return self.serialize_list(self.model, [message], 1)

        @user_token_required
        @api.expect(EventMessage.to_doc_list_model(api))
        @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
        def post(self, event_id, message_id):
            message = self.edit(message_id, request.get_json())
            return self.serialize_list(self.model, [message], 1)

        @user_token_required
        def delete(self, event_id, message_id):
            message = self.model.find(self.get_id(message_id))
            self.check_edit(message)
            message.delete()
            return {'success': True}
