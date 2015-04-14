from __future__ import absolute_import
from datetime import datetime

from peewee import Model, CompositeKey, TextField, DoubleField, DateTimeField, DoesNotExist, JOIN
from playhouse.db_url import connect
from playhouse.postgres_ext import BinaryJSONField
from config import Configuration


db = connect(Configuration.DATABASE_URL.replace('postgres://', 'postgresqlext://'))


class DataItem(Model):
    value = BinaryJSONField(null=False)

    @classmethod
    def select_non_expired(cls, *selection):
        return cls.select(
            *selection
        ).join(
            DataExpires,
            on=(cls.key == DataExpires.key),
            join_type=JOIN.LEFT_OUTER
        ).where(
            (DataExpires.key.is_null() | (DataExpires.expires > datetime.utcnow()))
        )

    def is_expired(self):
        try:
            expires = DataExpires.get(key=self.key)
            if datetime.utcnow() > expires.expires:
                return True
        except DoesNotExist:
            pass
        return False


class DataStrings(DataItem):
    class Meta:
        database = db
        db_table = 'data_strings'

    key = TextField(primary_key=True)


class DataSets(DataItem):
    class Meta:
        database = db
        db_table = 'data_sets'
        primary_key = CompositeKey('key', 'value')

    key = TextField(index=True)


class DataSortedSets(DataItem):
    class Meta:
        database = db
        db_table = 'data_sorted_sets'
        primary_key = CompositeKey('key', 'value')
        indexes = (
            (('key', 'score'), False),
        )

    key = TextField(null=False, index=True)
    score = DoubleField(null=False)


class DataExpires(Model):
    class Meta:
        database = db
        db_table = 'data_expires'

    key = TextField(primary_key=True, null=False)
    expires = DateTimeField(null=False, index=True)
