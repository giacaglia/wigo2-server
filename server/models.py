from __future__ import absolute_import

from pytz import timezone
from datetime import datetime, tzinfo
from schematics.models import Model
from schematics.types import BaseType, StringType, BooleanType, DateTimeType, EmailType, LongType, FloatType, IntType
from schematics.types.compound import ListType
import ujson


class JsonType(BaseType):
    def _mock(self, context=None):
        return {}

    def to_native(self, value, context=None):
        return ujson.dumps(value)

    def to_primitive(self, value, context=None):
        return value


class WigoModel(Model):
    pass


class WigoPersistentModel(WigoModel):
    id = LongType()
    created = DateTimeType(default=datetime.utcnow)
    modified = DateTimeType(default=datetime.utcnow)


class User(WigoPersistentModel):
    group_id = LongType()
    username = StringType()
    password = StringType()
    timezone = StringType()

    first_name = StringType()
    last_name = StringType()
    bio = StringType()
    birthdate = DateTimeType()
    gender = StringType()
    phone = StringType()

    email = EmailType()
    email_validated = BooleanType()
    email_validated_date = DateTimeType()
    email_validated_status = StringType()

    key = StringType()
    role = StringType()
    status = StringType()
    privacy = StringType(choices=('public', 'private'))

    facebook_id = StringType()
    facebook_token = StringType()

    latitude = FloatType()
    longitude = FloatType()

    properties = JsonType()


class Friend(WigoModel):
    user_id = LongType()
    friend_id = LongType()


class Tap(WigoModel):
    user_id = LongType()
    tapped_id = LongType()


class Invite(WigoModel):
    event_id = LongType()
    inviter_id = LongType()
    invited_id = LongType()


class Group(WigoPersistentModel):
    name = StringType()
    timezone = StringType(default='US/Eastern')

    latitude = FloatType()
    longitude = FloatType()


class Dated(WigoModel):
    date = DateTimeType()
    expires = DateTimeType()

    @classmethod
    def get_expires(cls, tz_arg, current=None):
        date, expires = Dated.get_dates(tz_arg, current)
        return expires

    @classmethod
    def get_today(cls, tz_arg):
        return Dated.get_expires(tz_arg) - timedelta(days=1)

    @classmethod
    def get_dates(cls, tz_arg, current=None):
        if current is None:
            if not tz_arg:
                tz = timezone('US/Eastern')
            elif isinstance(tz_arg, basestring):
                tz = timezone(tz_arg)
            elif isinstance(tz_arg, tzinfo):
                tz = tz_arg
            else:
                raise ValueError('Invalid timezone')

            # get current time in the correct timezone
            current = datetime.now(tz)

        current = current.replace(minute=0, second=0, microsecond=0)

        # if it is < 6am, the date will be 0am, and the expires will be 6am the SAME day
        # if it is > 6am, the date will be 6am, and the expires will be 6am the NEXT day
        if current.hour < 6:
            date = current.replace(hour=0).astimezone(UTC).replace(tzinfo=None)
            expires = current.replace(hour=6).astimezone(UTC).replace(tzinfo=None)
        else:
            date = current.replace(hour=6).astimezone(UTC).replace(tzinfo=None)
            next_day = current + timedelta(days=1)
            expires = next_day.replace(hour=6).astimezone(UTC).replace(tzinfo=None)

        return date, expires


class Event(WigoPersistentModel, Dated):
    group_id = LongType()
    name = StringType()
    privacy = StringType(choices=('public', 'private'))
    tags = ListType(StringType)


class EventAttendee(WigoModel):
    user_id = LongType()
    event_id = LongType()


class EventMessage(WigoPersistentModel):
    event_id = LongType()
    user_id = LongType()
    message = StringType()
    media_mime_type = StringType()
    media = StringType()
    thumbnail = StringType()
    vote_boost = IntType()
    tags = ListType(StringType)


class EventMessageVote(WigoModel):
    message_id = LongType()
    user_id = LongType()


class Notification(WigoPersistentModel):
    user_id = LongType()
    type = StringType()
    from_user_id = LongType()
    properties = JsonType()


class Message(WigoPersistentModel):
    user_id = LongType()
    to_user_id = LongType()
    message = LongType()


class Config(WigoPersistentModel):
    group_id = LongType()
    name = StringType()
    properties = JsonType()
