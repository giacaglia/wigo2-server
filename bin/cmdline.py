from __future__ import absolute_import

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import logging
import logconfig
import ujson
import click
import geodis

from datetime import timedelta, datetime
from peewee import SQL
from repoze.lru import LRUCache
from collections import Counter
from playhouse.dataset import DataSet
from schematics.exceptions import ModelValidationError

from server.db import wigo_db
from server.models.location import WigoCity
from server.tasks.analytics import import_friend_interactions, import_active_user_ids
from server.models import IntegrityException, DoesNotExist, skey
from server.models.user import User
from config import Configuration

logger = logging.getLogger('wigo.cmdline')


@click.group()
def cli():
    pass


@cli.command()
def deploy():
    from git import Repo

    logconfig.configure('dev')

    repo = Repo('.')
    target = None
    if repo.active_branch.name == 'master':
        target = 'wigo2'
    elif repo.active_branch.name == 'staging':
        target = 'wigo2-stage'
    elif repo.active_branch.name == 'develop':
        target = 'wigo2-dev'
    else:
        logger.error('invalid branch for deployment, %s' % repo.active_branch.name)
        sys.exit(1)

    if target == 'wigo2' and os.system('nosetests -w tests/') != 0:
        logger.error('error running unit tests')
        sys.exit(1)

    remote = next(r for r in repo.remotes if r.name == target)
    if not remote:
        remote = repo.create_remote(target, 'git@heroku.blade:%s.git' % target)

    logger.info('deploying to remote %s, %s' % (remote.name, remote.url))
    os.system('git push %s %s:master' % (target, repo.active_branch.name))


@cli.command()
@click.option('--create_tables', type=bool)
@click.option('--import_cities', type=bool)
def initialize(create_tables=False, import_cities=False):
    logconfig.configure('dev')

    if create_tables:
        from server.rdbms import db, DataStrings, DataSets, DataSortedSets, DataExpires, DataIntSets, DataIntSortedSets

        db.create_tables([DataStrings, DataSets, DataIntSets,
                          DataSortedSets, DataIntSortedSets, DataExpires], safe=True)

        db.execute_sql("""
           CREATE OR REPLACE FUNCTION timestamp_cast(VARCHAR) RETURNS TIMESTAMP
              AS 'select cast($1 as timestamp)'
              LANGUAGE SQL
              IMMUTABLE
              RETURNS NULL ON NULL INPUT;

           CREATE INDEX data_strings_gin ON data_strings USING gin (value);

           CREATE INDEX data_strings_id ON data_strings(
              (value->>'$type'),
              CAST(value->>'id' AS BIGINT) DESC
            );

           CREATE INDEX data_strings_events ON data_strings(
              (value->>'$type'),
              CAST(value->>'expires' AS TIMESTAMP)
            ) WHERE value->>'$type' = 'Event';

           CREATE INDEX data_strings_eventmessages ON data_strings(
              (value->>'$type'),
              CAST(value->>'id' AS BIGINT) DESC
            ) WHERE value->>'$type' = 'EventMessage';

           CREATE INDEX data_strings_eventmessages_event_id ON data_strings(
              (value->>'$type'),
              CAST(value->>'event_id' AS BIGINT) DESC
            ) WHERE value->>'$type' = 'EventMessage';

           CREATE INDEX data_strings_groups ON data_strings(
              (value->>'$type'),
            ) WHERE value->>'$type' = 'Group';

           CREATE INDEX data_strings_first_name ON data_strings(
              (value->>'$type'),
              LOWER(value->>'first_name') varchar_pattern_ops
            ) WHERE value->>'$type' = 'User';

           CREATE INDEX data_strings_last_name ON data_strings(
              (value->>'$type'),
              LOWER(value->>'last_name') varchar_pattern_ops
            ) WHERE value->>'$type' = 'User';

           CREATE INDEX data_int_sorted_sets_attendees_event_id ON data_int_sorted_sets(
              cast(split_part(replace(replace(key, '{', ''), '}', ''), ':', 2) as BIGINT)
            ) WHERE key ~ '\{event:\d+\}:attendees';

           CREATE INDEX data_int_sorted_sets_attendees_user_id ON data_int_sorted_sets(
              value
            ) WHERE key ~ '\{event:\d+\}:attendees';

           CREATE INDEX data_int_sorted_sets_votes_message_id ON data_int_sorted_sets(
              cast(split_part(replace(replace(key, '{', ''), '}', ''), ':', 2) as BIGINT)
            ) WHERE key ~ '\{eventmessage:\d+\}:votes';

           CREATE INDEX data_int_sorted_sets_votes_user_id ON data_int_sorted_sets(
              value
            ) WHERE key ~ '\{eventmessage:\d+\}:votes';

           CREATE INDEX data_int_sorted_sets_taps_user_id ON data_int_sorted_sets(
              cast(split_part(replace(replace(key, '{', ''), '}', ''), ':', 2) as BIGINT)
            ) WHERE key ~ '\{user:\d+\}:tapped';

           CREATE INDEX data_int_sorted_sets_taps_tapped_id ON data_int_sorted_sets(
              value
            ) WHERE key ~ '\{user:\d+\}:tapped';

            CREATE INDEX data_int_sorted_sets_invites_user_id ON data_int_sorted_sets(
              cast(split_part(replace(replace(key, '{', ''), '}', ''), ':', 4) as BIGINT)
            ) WHERE key ~ '\{event:\d+\}:user:\d+:invited';

            CREATE INDEX data_int_sorted_sets_invites_event_id ON data_int_sorted_sets(
              cast(split_part(replace(replace(key, '{', ''), '}', ''), ':', 2) as BIGINT)
            ) WHERE key ~ '\{event:\d+\}:user:\d+:invited';

           CREATE INDEX data_int_sorted_sets_invites_invited_id ON data_int_sorted_sets(
              value
            ) WHERE key ~ '\{event:\d+\}:user:\d+:invited';

           CREATE INDEX data_int_sorted_sets_friends_user_id ON data_int_sorted_sets(
              cast(split_part(replace(replace(key, '{', ''), '}', ''), ':', 2) as BIGINT)
            ) WHERE key ~ '\{user:\d+\}:friends';

           CREATE INDEX data_int_sorted_sets_friends_friend_id ON data_int_sorted_sets(
              value
            ) WHERE key ~ '\{user:\d+\}:friends';

           CREATE OR REPLACE VIEW users AS
              SELECT key, CAST(value->>'id' AS BIGINT) id, CAST(value->>'group_id' AS BIGINT) group_id,
              value->>'first_name' first_name, value->>'last_name' last_name, value->>'gender' gender,
              data_strings.value ->> 'role'::text AS "role", value->>'status' status,
              CAST(value->>'latitude' as float) latitude,
              CAST(value->>'longitude' as float) longitude,
              timestamp_cast(value->>'created') "created"
              FROM data_strings WHERE value->>'$type' = 'User';

           CREATE OR REPLACE VIEW groups AS
              SELECT key, CAST(value->>'id' AS BIGINT) id,
              value->>'name' "name", value->>'code' code, value->>'city_id' city_id,
              value->>'state' state, value->>'country' country,
              CAST(value->>'latitude' as float) latitude, CAST(value->>'longitude' as float) longitude
              FROM data_strings WHERE value->>'$type' = 'Group';

           CREATE OR REPLACE VIEW events AS
                SELECT key, CAST(value->>'id' AS BIGINT) id, CAST(value->>'owner_id' AS BIGINT) owner_id,
                CAST(value->>'group_id' AS BIGINT) group_id, value->>'name' "name",
                timestamp_cast(value->>'expires') "expires",
                (SELECT COUNT(key) FROM data_int_sorted_sets WHERE
                  key = format('{event:%s}:attendees', (data_strings.value->>'id'))) num_attendees,
                timestamp_cast(value->>'date') "date",
                value->>'privacy' "privacy"
                FROM data_strings WHERE value->>'$type' = 'Event';

           CREATE OR REPLACE VIEW taps AS
               SELECT key, cast(split_part(replace(replace(key, '{', ''), '}', ''), ':', 2) as BIGINT)
                user_id, value as tapped_id, to_timestamp(score) as created, modified
                FROM data_int_sorted_sets WHERE key ~ '\{user:\d+\}:tapped';

           CREATE OR REPLACE VIEW friends AS
                SELECT key, cast(split_part(replace(replace(key, '{', ''), '}', ''), ':', 2) as BIGINT)
                user_id, value as friend_id, cast(null as timestamp) as created, modified
                FROM data_int_sorted_sets WHERE key ~ '\{user:\d+\}:friends';

           CREATE OR REPLACE VIEW invites AS
                SELECT key, cast(split_part(replace(replace(key, '{', ''), '}', ''), ':', 2) as BIGINT)
                event_id, cast(split_part(replace(replace(key, '{', ''), '}', ''), ':', 4) as BIGINT)
                user_id, value as invited_id, to_timestamp(score) as created, modified
                FROM data_int_sorted_sets WHERE key ~ '\{event:\d+\}:user:\d+:invited';

            CREATE OR REPLACE VIEW eventmessages AS
                SELECT key, CAST(value->>'id' AS BIGINT) id, CAST(value->>'user_id' AS BIGINT) user_id,
                CAST(value->>'event_id' AS BIGINT) event_id,
                value->>'media' "media", value->>'media_mime_type' "media_mime_type",
                timestamp_cast(value->>'created') "created"
                FROM data_strings WHERE value->>'$type' = 'EventMessage';

            CREATE OR REPLACE VIEW attendees AS
                select key, cast(split_part(replace(replace(key, '{', ''), '}', ''), ':', 2) as BIGINT)
                event_id, value as user_id from data_int_sorted_sets where key ~ '\{event:\d+\}:attendees';

            CREATE OR REPLACE VIEW votes AS
                select key, cast(split_part(replace(replace(key, '{', ''), '}', ''), ':', 2) as BIGINT)
                message_id, value as user_id from data_int_sorted_sets where key ~ '\{eventmessage:\d+\}:votes';

            CREATE OR REPLACE VIEW current_week AS
                select  xdate.*, xdate.start_ts at time zone 'US/Hawaii'
                at time zone 'UTC' as start_ts_utc,
                xdate.end_ts at time zone 'US/Hawaii'
                at time zone 'UTC' as end_ts_utc
                from (  select  xdate.xdate + make_interval(days := (case
                        when xdate.dow <= 3 then 3-xdate.dow-7
                        else 3-xdate.dow end)) as start_ts,
                xdate.xdate - interval '1 microseconds' as end_ts
                from (  select  xdate.xdate,
                        cast(extract(dow from xdate) as int) as dow
                from (  select date(current_timestamp
                                at time zone 'US/Eastern') as xdate)
                                xdate) xdate) xdate;

          """)

    if import_cities:
        from server.db import redis

        redis.delete(WigoCity.getGeohashIndexKey())

        cities_file = os.path.join(geodis.__path__[0], 'data', 'cities1000.json')
        with open(cities_file) as f:
            pipe = redis.pipeline()

            lines = 0
            imported = 0
            skipped = 0
            for line in f:
                try:
                    row = [x.encode('utf-8') for x in ujson.loads(line)]

                    loc = WigoCity(
                        continent_id=row[0],
                        continent=row[1],
                        country_id=row[2],
                        country=row[3],
                        state_id=row[4],
                        state=row[5],
                        city_id=row[6],
                        name=row[7],
                        lat=float(row[8]),
                        lon=float(row[9]),
                        population=int(row[11])
                    )

                    if loc.population > 40000:
                        loc.save(pipe)
                        imported += 1
                    else:
                        skipped += 1

                    lines += 1

                    if (lines % 2000) == 0:
                        logger.info('imported {}, skipped {}'.format(imported, skipped))
                        pipe.execute()

                except Exception, e:
                    logging.exception("Could not import line %s: %s", line, e)
                    return

        pipe.execute()


@cli.command()
def import_interactions():
    import_friend_interactions()


@cli.command()
@click.option('--window', type=int, default=2)
def import_active_users(window=2):
    import_active_user_ids(window)


@cli.command()
def update_facebook_token_expirations():
    from server.services.facebook import Facebook, FacebookTokenExpiredException

    logconfig.configure('dev')

    for u in User.select():
        if u.facebook_token_expires < datetime.utcnow():
            facebook = Facebook(u.facebook_token, u.facebook_token_expires)
            try:
                token_expires = facebook.get_token_expiration()
                if token_expires and token_expires != u.facebook_token_expires:
                    u.facebook_token_expires = token_expires
                    u.save()
                    print 'updated user {}'.format(u.id)
            except FacebookTokenExpiredException:
                pass


@cli.command()
def migrate_top_friends():
    logconfig.configure('dev')

    users = 0
    for user_id, score in wigo_db.sorted_set_iter(skey('user'), count=50):
        for friend_id, score in wigo_db.sorted_set_iter(skey('user', user_id, 'friends'), count=50):
            if not wigo_db.sorted_set_is_member(skey('user', user_id, 'friends', 'top'), friend_id):
                wigo_db.sorted_set_add(skey('user', user_id, 'friends', 'top'), friend_id, 1)

        users += 1
        if (users % 100) == 0:
            logger.info('fixed {} users'.format(users))


if __name__ == '__main__':
    cli()
