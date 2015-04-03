from __future__ import absolute_import

import ujson
from flask import flash
from flask.ext.admin.babel import gettext
from flask.ext.admin.model import BaseModelView
from flask.ext.admin.model.fields import ListEditableFieldList
from schematics.types import StringType, BooleanType, DateTimeType, NumberType, FloatType
from wtforms import Form, StringField, BooleanField, SelectField, IntegerField, FloatField, TextAreaField
from flask_admin.form.fields import DateTimeField
from wtforms.validators import Optional
from server.models import JsonType


class RedisModelView(BaseModelView):
    def __init__(self, model, db, name=None, category=None, endpoint=None, url=None, static_folder=None,
                 menu_class_name=None, menu_icon_type=None, menu_icon_value=None):
        super(RedisModelView, self).__init__(model, name, category, endpoint, url, static_folder, menu_class_name,
                                             menu_icon_type, menu_icon_value)
        self.db = db


    def scaffold_list_form(self, custom_fieldlist=ListEditableFieldList, validators=None):
        pass

    def create_model(self, form):
        try:
            instance = self.model()
            form.populate_obj(instance)
            self._on_model_change(form, instance, True)
            self.db.save(instance)
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
            self.db.save(model)
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
            self.db.delete(model)
            return True
        except Exception as ex:
            if not self.handle_view_exception(ex):
                flash(gettext('Failed to delete record. %(error)s', error=str(ex)), 'error')
            return False

    def _create_ajax_loader(self, name, options):
        pass

    def scaffold_form(self):
        class MyForm(Form):
            pass

        for field in self.model.fields.values():
            if field.name == 'id':
                continue
            if isinstance(field, DateTimeType):
                setattr(MyForm, field.name, DateTimeField(field.name, default=field.default))
            elif isinstance(field, FloatType):
                setattr(MyForm, field.name, FloatField(field.name, default=field.default, validators=[Optional()]))
            elif isinstance(field, NumberType):
                setattr(MyForm, field.name, IntegerField(field.name, default=field.default))
            elif isinstance(field, StringType):
                if field.choices:
                    setattr(MyForm, field.name, SelectField(field.name,
                                                            choices=[(val, val) for val in field.choices],
                                                            default=field.default))
                else:
                    setattr(MyForm, field.name, StringField(field.name, default=field.default))
            elif isinstance(field, BooleanType):
                setattr(MyForm, field.name, BooleanField(field.name))
            elif isinstance(field, JsonType):
                setattr(MyForm, field.name, JSONField(field.name))

        return MyForm

    def get_list(self, page, sort_field, sort_desc, search, filters):
        count = self.db.count(self.model)
        return count, self.db.all(self.model, page)

    def get_one(self, id):
        return self.db.get(self.model, id)

    def _get_field_value(self, model, name):
        return getattr(model, name)

    def scaffold_sortable_columns(self):
        return []

    def get_pk_value(self, model):
        return model.id

    def scaffold_list_columns(self):
        return self.model.fields.keys()


class UserModelView(RedisModelView):
    def scaffold_list_columns(self):
        return ['id', 'group_id', 'username', 'created']


class GroupModelView(RedisModelView):
    def scaffold_list_columns(self):
        return ['id', 'name', 'created']


class NotificationView(RedisModelView):
    def scaffold_list_columns(self):
        return ['id', 'user_id', 'type']


class MessageView(RedisModelView):
    def scaffold_list_columns(self):
        return ['id', 'user_id', 'to_user_id']


class ConfigView(RedisModelView):
    def scaffold_list_columns(self):
        return ['id', 'name', 'created']


class JSONField(TextAreaField):
    def _value(self):
        if self.raw_data:
            return self.raw_data[0]
        if self.data:
            return self.data
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
        return ujson.dumps(obj)

    def from_json(self, data):
        return ujson.loads(data)
