from __future__ import absolute_import

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import logging
import logconfig
import ujson
import geodis
from server.models.location import WigoCity
from playhouse.dataset import DataSet
from schematics.exceptions import ModelValidationError
from config import Configuration
from clize import run, clize
from server.models import IntegrityException, DoesNotExist
from server.models.group import Group
from server.models.user import User, Friend, Tap

logger = logging.getLogger('wigo.cmdline')


@clize
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


@clize
def initialize(create_tables=False, import_cities=False):
    logconfig.configure('dev')

    if create_tables:
        from server.rdbms import db, DataStrings, DataSets, DataSortedSets, DataExpires, DataIntSets, DataIntSortedSets

        db.create_tables([DataStrings, DataSets, DataIntSets,
                          DataSortedSets, DataIntSortedSets, DataExpires], safe=True)

        db.execute_sql("""
          CREATE INDEX data_strings_gin ON data_strings USING gin (value);

          CREATE INDEX data_strings_first_name ON data_strings(
              CAST(value->>'group_id' AS int8),
              (value->>'$type'),
              LOWER(value->>'first_name') varchar_pattern_ops
            ) WHERE value->>'$type' = 'User';

           CREATE INDEX data_strings_last_name ON data_strings(
              CAST(value->>'group_id' AS int8),
              (value->>'$type'),
              LOWER(value->>'last_name') varchar_pattern_ops
            ) WHERE value->>'$type' = 'User';
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


@clize
def import_old_db(groups=False, users=False, friends=False, taps=False):
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

    if groups:
        for dbgroup in groups_table.find():
            group = Group(dbgroup)

            try:
                group.validate()
                group.save()
                num_saved += 1

                if (num_saved % 100) == 0:
                    logger.info('saved {} groups'.format(num_saved))

            except ModelValidationError, e:
                logger.error('model validation error, {}'.format(e.message))
                logger.error(ujson.dumps(dbgroup))

    num_saved = 0

    if users:
        try:
            boston = Group.find(code='boston')
        except DoesNotExist:
            boston = Group({
                'name': 'Boston', 'code': 'boston', 'city_id': 4930956,
                'latitude': 42.3584, 'longitude': -71.0598
            }).save()

        for dbuser in users_table.find(email_validated=True, group=1):
            properties = dbuser.get('properties')
            if isinstance(properties, dict) and 'images' in properties:
                images = properties.get('images')
                if isinstance(images, list):
                    if len(images) > 0 and isinstance(images[0], basestring):
                        images = [{'url': img} for img in images]
                        dbuser['properties']['images'] = images
                else:
                    dbuser['properties']['images'] = []

            assoc = aa_table.find_one(user=dbuser['id'])
            user = User(dbuser)

            try:
                user.group_id = boston.id
                user.facebook_id = dbuser['facebook']
                user.facebook_token = assoc['service_token']
                user.facebook_token_expires = assoc['service_token_expires']
                user.validate()
                user.save()
                num_saved += 1

                if (num_saved % 100) == 0:
                    logger.info('saved {} users'.format(num_saved))

            except ModelValidationError, e:
                logger.error('model validation error, {}'.format(e.message))
                logger.error(ujson.dumps(dbuser))
            except IntegrityException, e:
                logger.error('model integrity error, {}'.format(e.message))
                logger.error(ujson.dumps(dbuser))

    num_saved = 0

    if friends:
        results = rdbms.query("""
            select t1.user_id, t1.follow_id, t1.created from follow t1, follow t2 where t1.user_id = t2.follow_id and t1.follow_id = t2.user_id and
            t1.accepted is True and t2.accepted is True and t1.user_id in (select id from "user" where group_id = 1)
        """)

        for result in results:
            try:
                Friend({
                    'user_id': result[0],
                    'friend_id': result[1],
                    'accepted': True
                }).save()
                num_saved += 1

                if (num_saved % 100) == 0:
                    logger.info('saved {} friends'.format(num_saved))

            except Exception, e:
                logger.error('friend import error, {}'.format(e.message))

    if taps:
        results = rdbms.query("""
            select user_id, tapped_id from tap where user_id in (select id from "user" where group_id = 1)
        """)

        for result in results:
            try:
                Tap({
                    'user_id': result[0],
                    'tapped_id': result[1]
                }).save()

                num_saved += 1

                if (num_saved % 100) == 0:
                    logger.info('saved {} taps'.format(num_saved))

            except Exception, e:
                logger.error('tap import error, {}'.format(e.message))


@clize
def import_predictions():
    import predictionio

    logconfig.configure('dev')

    rdbms = DataSet(Configuration.DATABASE_URL)
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

        r = client.set_item(with_user_id, {'categories': [str(group_id)]})

        if r.status not in (200, 201):
            raise Exception('Error returned from prediction io')

        r = client.record_user_action_on_item(event, user_id, with_user_id)
        if r.status not in (200, 201):
            raise Exception('Error returned from prediction io')

    results = rdbms.query("""
        select tap.user_id, "user".group_id, tap.tapped_id from tap inner join "user" on tap.user_id = "user".id
        where tap.created > (now() - interval '30 days')
    """)

    rows = 0

    for result in results:
        user_id = result[0]
        group_id = result[1]
        with_user_id = result[2]
        record(user_id, with_user_id, group_id, 'view')

        rows += 1

        if (rows % 500) == 0:
            logger.info('wrote {} records'.format(rows))


if __name__ == '__main__':
    run((deploy, initialize, import_old_db, import_predictions))