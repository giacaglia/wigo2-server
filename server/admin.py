from __future__ import absolute_import

import json
from flask.ext.admin import AdminIndexView
from flask.ext.admin.model.ajax import AjaxModelLoader, DEFAULT_PAGE_SIZE
from flask.ext.admin.model.filters import BaseFilter
from flask.ext.bcrypt import generate_password_hash
from markupsafe import Markup
from schematics.types.compound import ListType

from wtforms.widgets import TextArea
from flask import flash, redirect, url_for
from flask.ext.admin.actions import action
from flask.ext.admin.babel import gettext, lazy_gettext
from flask.ext.admin.model import BaseModelView
from flask.ext.admin.model.fields import ListEditableFieldList, AjaxSelectField
from schematics.types import StringType, BooleanType, DateTimeType, NumberType, FloatType
from wtforms import Form, StringField, BooleanField, SelectField, IntegerField, FloatField, TextAreaField
from flask_admin.form.fields import DateTimeField, Select2TagsField
from wtforms.validators import Optional, DataRequired
from server.models import JsonType, DoesNotExist
from server.models.event import Event
from server.models.group import Group
from server.security import check_basic_auth, authenticate


class WigoAdminIndexView(AdminIndexView):
    def is_accessible(self):
        return check_basic_auth()

    def inaccessible_callback(self, name, **kwargs):
        return authenticate()


class WigoAjaxModelLoader(AjaxModelLoader):
    def get_one(self, pk):
        return self.options['model'].find(int(pk))

    def get_list(self, query, offset=0, limit=DEFAULT_PAGE_SIZE):
        model = self.options['model']
        return list(model.select().where(name=query).limit(limit))

    def format(self, model):
        return (model.id, model.name) if model else None


def actions_formatter(view, context, model, name):
    return Markup('Photos | Attendees')


def group_formatter(view, context, model, name):
    try:
        return Markup(model.group.name) if model.group else ""
    except DoesNotExist:
        return ""


class WigoModelView(BaseModelView):
    edit_template = 'admin_overrides/edit.html'

    column_formatters = {
        'actions': actions_formatter,
        'group': group_formatter
    }

    form_ajax_refs = {
        'group': WigoAjaxModelLoader('group', {'model': Group})
    }

    def __init__(self, model, name=None, category=None, endpoint=None, url=None, static_folder=None,
                 menu_class_name=None, menu_icon_type=None, menu_icon_value=None):
        super(WigoModelView, self).__init__(model, name, category, endpoint, url, static_folder, menu_class_name,
                                            menu_icon_type, menu_icon_value)

    def _create_ajax_loader(self, name, options):
        pass

    def is_accessible(self):
        return check_basic_auth()

    def inaccessible_callback(self, name, **kwargs):
        return authenticate()

    def scaffold_list_form(self, custom_fieldlist=ListEditableFieldList, validators=None):
        pass

    def create_model(self, form):
        try:
            instance = self.model()
            form.populate_obj(instance)
            self._on_model_change(form, instance, True)
            instance.save()
        except Exception as ex:
            if not self.handle_view_exception(ex):
                flash(gettext('Failed to create record. %(error)s', error=str(ex)), 'error')
            return False
        else:
            self.after_model_change(form, instance, True)

        return True

    def update_model(self, form, model):
        try:
            form.populate_obj(model)
            self._on_model_change(form, model, False)
            model.save()
        except Exception as ex:
            if not self.handle_view_exception(ex):
                flash(gettext('Failed to update record. %(error)s', error=str(ex)), 'error')
            return False
        else:
            self.after_model_change(form, model, False)

        return True

    def delete_model(self, model):
        try:
            self.on_model_delete(model)
            model.delete()
            return True
        except Exception as ex:
            if not self.handle_view_exception(ex):
                flash(gettext('Failed to delete record. %(error)s', error=str(ex)), 'error')
            return False

    def scaffold_form(self):
        class WigoModelForm(Form):
            pass

        for field in self.model.fields.values():
            validators = [DataRequired() if field.required else Optional()]
            if field.name == 'id':
                continue
            if 'group_id' == field.name:
                setattr(WigoModelForm, 'group', AjaxSelectField(
                    WigoAjaxModelLoader('group', {'model': Group}), 'group', validators=validators))
            elif isinstance(field, DateTimeType):
                setattr(WigoModelForm, field.name,
                        DateTimeField(field.name, default=field.default, validators=validators))
            elif isinstance(field, FloatType):
                setattr(WigoModelForm, field.name, FloatField(field.name, default=field.default, validators=validators))
            elif isinstance(field, NumberType):
                setattr(WigoModelForm, field.name,
                        IntegerField(field.name, default=field.default, validators=validators))
            elif isinstance(field, StringType):
                if field.choices:
                    setattr(WigoModelForm, field.name,
                            SelectField(field.name, choices=[(val, val) for val in field.choices],
                                        default=field.default))
                else:
                    setattr(WigoModelForm, field.name,
                            StringField(field.name, default=field.default, validators=validators))
            elif isinstance(field, BooleanType):
                setattr(WigoModelForm, field.name, BooleanField(field.name))
            elif isinstance(field, JsonType):
                setattr(WigoModelForm, field.name, JSONField(field.name, validators=validators))
            elif isinstance(field, ListType):
                setattr(WigoModelForm, field.name, Select2TagsField(field.name, validators=validators, save_as_list=True))

        return WigoModelForm

    def get_list(self, page, sort_field, sort_desc, search, filters):
        query = self.model.select().limit(self.page_size).page(page + 1)

        # Filters
        if self._filters:
            for flt, flt_name, value in filters:
                f = self._filters[flt]
                query = f.apply(query, flt_name, value)

        count, page, instances = query.execute()
        return count, instances

    def get_one(self, id):
        return self.model.find(id)

    def _get_field_value(self, model, name):
        return getattr(model, name)

    def scaffold_sortable_columns(self):
        return []

    def scaffold_filters(self, name):
        return [WigoEqualsModelFilter(name)]

    def get_pk_value(self, model):
        return model.id

    def scaffold_list_columns(self):
        return self.model.fields.keys()

    def render(self, template, **kwargs):
        if template == self.edit_template:
            if self.model == Event:
                kwargs['event'] = True
            if self.model == Group:
                kwargs['group'] = True
        return super(WigoModelView, self).render(template, **kwargs)

    @action('delete', lazy_gettext('Delete'), lazy_gettext('Are you sure you want to delete selected records?'))
    def action_delete(self, ids):
        for id in ids:
            self.model.find(id).delete()


class WigoEqualsModelFilter(BaseFilter):
    # noinspection PyMethodOverriding
    def apply(self, query, name, value):
        if name == 'group_code':
            try:
                group = Group.find(code=value)
                return query.group(group)
            except DoesNotExist:
                pass
        return query.where(**{name: value})

    def operation(self):
        return gettext('equals')


class UserModelView(WigoModelView):
    column_filters = ('username', 'email', 'facebook_id', 'first_name', 'last_name', 'group_code')

    def scaffold_list_columns(self):
        return ['id', 'group', 'username', 'created']

    def update_model(self, form, model):
        existing_password = model.password
        password = form.password.data
        updated = super(UserModelView, self).update_model(form, model)
        if password != existing_password:
            model.password = generate_password_hash(password) if password else None
            model.save()
        return updated

    @action('email', lazy_gettext('Email'))
    def action_email(self, ids):
        pass

    @action('push', lazy_gettext('Push'), 'Are you sure?')
    def action_push(self, ids):
        return redirect(url_for('.send_push'))


class GroupModelView(WigoModelView):
    column_filters = ('code', 'name', 'city_id')

    def scaffold_list_columns(self):
        return ['id', 'name', 'created']


class EventModelView(WigoModelView):
    column_filters = ('group',)

    def scaffold_list_columns(self):
        return ['group', 'name', 'created']


class NotificationView(WigoModelView):
    def scaffold_list_columns(self):
        return ['id', 'user_id', 'type']

    def is_visible(self):
        return False


class MessageView(WigoModelView):
    def scaffold_list_columns(self):
        return ['id', 'user_id', 'to_user_id']

    def is_visible(self):
        return False


class EventMessageView(WigoModelView):
    column_filters = ('user_id', 'event_id')

    def scaffold_list_columns(self):
        return ['id', 'user_id', 'media_mime_type']

    def is_visible(self):
        return False


class ConfigView(WigoModelView):
    def scaffold_list_columns(self):
        return ['id', 'name', 'created']


class JsonTextArea(TextArea):
    def __call__(self, field, **kwargs):
        kwargs['style'] = 'width:500px'
        kwargs['rows'] = 15
        return super(JsonTextArea, self).__call__(field, **kwargs)


class JSONField(TextAreaField):
    widget = JsonTextArea()

    def _value(self):
        if self.raw_data:
            return self.raw_data[0]
        if self.data:
            return self.to_json(self.data)
        return ""

    def process_formdata(self, valuelist):
        if valuelist:
            value = valuelist[0]
            if not value:
                self.data = None
                return
            try:
                self.data = self.from_json(value)
            except ValueError:
                self.data = None
                raise ValueError(self.gettext('Invalid JSON'))

    def to_json(self, obj):
        return json.dumps(obj, indent=2)

    def from_json(self, data):
        return json.loads(data)
