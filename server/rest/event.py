from __future__ import absolute_import
from flask import g, request
from flask.ext.restful import abort

from server.models import DoesNotExist
from server.models.event import Event, EventMessage, EventAttendee
from server.rest import WigoDbListResource, WigoDbResource, WigoResource
from server.security import wigo_user_token_required


# noinspection PyUnresolvedReferences
def setup_event_resources(api):
    @api.route('/api/events/<int:model_id>')
    @api.response(403, 'If not invited')
    class EventResource(WigoDbResource):
        model = Event

        @api.response(200, 'Success', model=Event.to_doc_list_model(api))
        def get(self, model_id):
            return super(EventResource, self).get(model_id)

        def check_get(self, event):
            super(EventResource, self).check_get(event)
            if not g.user.is_invited(event):
                abort(403, message='Not invited to event')

        @api.expect(Event.to_doc_list_model(api))
        @api.response(200, 'Success', model=Event.to_doc_list_model(api))
        def post(self, model_id):
            return super(EventResource, self).post(model_id)

        def delete(self, model_id):
            abort(501, message='Not implemented')

    @api.route('/api/events/')
    class EventListResource(WigoDbListResource):
        model = Event

        @wigo_user_token_required
        @api.response(200, 'Success', model=Event.to_doc_list_model(api))
        def get(self):
            count, instances = self.select().group(g.group).execute()
            return self.serialize_list(self.model, instances, count)

        @api.expect(Event.to_doc_list_model(api))
        @api.response(200, 'Success', model=Event.to_doc_list_model(api))
        def post(self):
            return super(EventListResource, self).post()

    @api.route('/api/events/<int:event_id>/attendees')
    @api.response(403, 'If not invited')
    class EventAttendeeListResource(WigoResource):
        model = EventAttendee

        @wigo_user_token_required
        @api.expect(EventAttendee.to_doc_list_model(api))
        @api.response(200, 'Success', model=EventAttendee.to_doc_list_model(api))
        def post(self, event_id):
            attendee = EventAttendee({
                'user_id': g.user.id,
                'event_id': event_id
            }).save()

            return {'success': True}

        @wigo_user_token_required
        def delete(self, event_id):
            EventAttendee({
                'user_id': g.user.id,
                'event_id': event_id
            }).delete()

            return {'success': True}

    @api.route('/api/eventmessages/<int:model_id>')
    @api.response(403, 'If not invited to event')
    class EventMessageResource(WigoDbResource):
        model = EventMessage

        @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
        def get(self, model_id):
            return super(EventMessageResource, self).get(model_id)

        @api.expect(EventMessage.to_doc_list_model(api))
        @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
        def post(self, model_id):
            return super(EventMessageResource, self).post(model_id)

        def check_get(self, message):
            super(EventMessageResource, self).check_get(message)
            event = message.event
            if not g.user.is_invited(event):
                abort(403, message='Not invited to event')

    @api.route('/api/eventmessages/')
    @api.response(403, 'If not invited to event')
    class EventMessageListResource(WigoDbListResource):
        model = EventMessage

        @wigo_user_token_required
        @api.response(200, 'Success', model=EventMessage.to_doc_list_model(api))
        def get(self):
            event_id = int(request.args.get('event', 0))
            if event_id:
                event = Event.find(event_id)

                if not g.user.is_invited(event):
                    abort(403, message='Not invited to event')

                count, messages = self.select().event(event).execute()
                return self.serialize_list(self.model, messages, count)
            else:
                raise DoesNotExist()

        @api.expect(EventMessage.to_doc_list_model(api))
        def post(self):
            return super(EventMessageListResource, self).post()

