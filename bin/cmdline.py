from __future__ import absolute_import
from collections import Counter

import sys
import os
from datetime import timedelta
from peewee import SQL
from repoze.lru import LRUCache

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import logging
import logconfig
import ujson
import click
import geodis

from server.db import wigo_db
from server.models.location import WigoCity
from playhouse.dataset import DataSet
from schematics.exceptions import ModelValidationError
from config import Configuration
from server.models import IntegrityException, DoesNotExist, skey
from server.models.user import User

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
            )

           CREATE INDEX data_strings_events ON data_strings(
              (value->>'$type'),
              cast(value->>'expires' AS TIMESTAMP)
            ) WHERE value->>'$type' = 'Event';

           CREATE INDEX data_strings_groups ON data_strings(
              (value->>'$type',
            ) WHERE value->>'$type' = 'Group';

           CREATE INDEX data_strings_first_name ON data_strings(
              (value->>'$type'),
              LOWER(value->>'first_name') varchar_pattern_ops
            ) WHERE value->>'$type' = 'User';

           CREATE INDEX data_strings_last_name ON data_strings(
              (value->>'$type'),
              LOWER(value->>'last_name') varchar_pattern_ops
            ) WHERE value->>'$type' = 'User';

            CREATE OR REPLACE VIEW users AS
              SELECT key, CAST(value->>'id' AS BIGINT) id, CAST(value->>'group_id' AS BIGINT) group_id,
              value->>'first_name' first_name, value->>'last_name' last_name, value->>'gender' gender,
              value->>'status' status, CAST(value->>'latitude' as float) latitude,
              CAST(value->>'longitude' as float) longitude
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
                  key = format('{event:%s}:attendees', (data_strings.value->>'id'))) num_attendees
                FROM data_strings WHERE value->>'$type' = 'Event';
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
@click.option('--users', type=bool, default=False)
@click.option('--friends', type=bool, default=False)
@click.option('--start', type=int, default=None)
def import_old_db(users=False, friends=False, start=None):
    from server.tasks.predictions import wire_predictions_listeners

    logconfig.configure('dev')
    Configuration.CAPTURE_IMAGES = False

    rdbms = DataSet(Configuration.OLD_DATABASE_URL)

    num_saved = 0
    groups_table = rdbms['group']
    users_table = rdbms['user']
    follow_table = rdbms['follow']
    aa_table = rdbms['accountassociation']

    wire_predictions_listeners()

    num_saved = 0

    schools = {}

    def get_school(school_id):
        school = schools.get(school_id)
        if not school:
            school = groups_table.find_one(id=school_id)
            schools[school_id] = school
        return school

    if users:
        query = users_table.find(email_validated=True, status='active').order_by(SQL('id desc'))
        if start:
            query = query.where(SQL('id < {}'.format(start)))
        for dbuser in query.iterator():
            properties = dbuser.get('properties')
            if isinstance(properties, dict) and 'images' in properties:
                images = properties.get('images')
                if isinstance(images, list):
                    if len(images) > 0 and isinstance(images[0], basestring):
                        images = [{'url': img} for img in images]
                        dbuser['properties']['images'] = images
                else:
                    dbuser['properties']['images'] = []

            try:
                User.find(dbuser['id'])
                continue
            except DoesNotExist:
                pass

            if dbuser['group'] in (1, 2, 1938, 3570):
                continue

            if ' ' in dbuser['email']:
                dbuser['email'] = dbuser['email'].replace(' ', '')

            school = get_school(dbuser['group'])

            assoc = aa_table.find_one(user=dbuser['id'])

            user = User(dbuser)

            try:
                user.facebook_id = dbuser['facebook']
                user.facebook_token = assoc['service_token']
                user.facebook_token_expires = assoc['service_token_expires']
                user.status = 'imported'

                if school:
                    user.education = school['name']

                user.validate()
                user.save()

                num_saved += 1

                if (num_saved % 100) == 0:
                    logger.info('saved {} users, last user_id {}'.format(num_saved, user.id))

            except ModelValidationError, e:
                logger.error('model validation error, {}'.format(e.message))
                logger.error(ujson.dumps(dbuser))
            except IntegrityException, e:
                logger.error('model integrity error, {}'.format(e.message))
                logger.error(ujson.dumps(dbuser))

    num_saved = 0

    if friends:
        results = rdbms.query("""
            select t1.user_id, t1.follow_id, t1.created from follow t1 inner join "user" u1 on t1.user_id = u1.id,
            follow t2 where t1.user_id = t2.follow_id and t1.follow_id = t2.user_id and
            t1.accepted is True and t2.accepted is True and
            u1.group_id not in (1, 2, 1938, 3570)
            order by t1.user_id, t1.follow_id
        """)

        num_recs = Counter()
        users = LRUCache(1000)

        def get_user(user_id):
            u = users.get(user_id)
            if u is None:
                try:
                    u = User.find(user_id)
                    users.put(user_id, u)
                except DoesNotExist, e:
                    users.put(user_id, e)
            if isinstance(u, DoesNotExist):
                raise u
            return u

        for result in results:
            if num_recs[result[0]] > 100:
                continue

            try:
                u1 = get_user(result[0])
                u2 = get_user(result[1])
            except DoesNotExist:
                continue

            try:
                wigo_db.sorted_set_add(skey(u1, 'friend', 'suggestions'), u2.id, .9, replicate=False)
                wigo_db.redis.expire(skey(u1, 'friend', 'suggestions'), timedelta(days=30))

                num_recs[result[0]] += 1

                num_saved += 1

                if (num_saved % 100) == 0:
                    logger.info('saved {} friends'.format(num_saved))

            except Exception, e:
                logger.error('friend import error, {}'.format(e.message))


@cli.command()
def import_predictions():
    import predictionio

    logconfig.configure('dev')

    rdbms = DataSet(Configuration.OLD_DATABASE_URL)
    users_table = rdbms['user']

    client = predictionio.EventClient(
        access_key=Configuration.PREDICTION_IO_ACCESS_KEY,
        url='http://{}:7070'.format(Configuration.PREDICTION_IO_HOST),
        threads=5,
        qsize=500
    )

    def record(user_id, with_user_id, group_id, event):
        r = client.set_user(user_id)

        if r.status not in (200, 201):
            raise Exception('Error returned from prediction io')

        r = client.set_item(with_user_id)

        if r.status not in (200, 201):
            raise Exception('Error returned from prediction io')

        r = client.record_user_action_on_item(event, user_id, with_user_id)
        if r.status not in (200, 201):
            raise Exception('Error returned from prediction io')

    results = rdbms.query("""
        select tap.user_id, "user".group_id, tap.tapped_id from tap inner join "user" on tap.user_id = "user".id
        where tap.created > (now() - interval '30 days') and "user".group_id = 1
    """)

    rows = 0

    for result in results:
        user_id = result[0]
        group_id = result[1]
        with_user_id = result[2]

        try:
            User.find(user_id)
            User.find(with_user_id)
        except DoesNotExist:
            continue

        record(user_id, with_user_id, group_id, 'view')

        rows += 1

        if (rows % 100) == 0:
            logger.info('wrote {} records'.format(rows))


if __name__ == '__main__':
    cli()
