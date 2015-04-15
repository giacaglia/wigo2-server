from __future__ import absolute_import

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import re
import logging
import logconfig
import ujson
from playhouse.dataset import DataSet
from schematics.exceptions import ModelValidationError
from config import Configuration
from clize import run, clize
from server.models import IntegrityException, DoesNotExist
from server.models.group import Group
from server.models.user import User, Friend

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
    if create_tables:
        from server.rdbms import db, DataStrings, DataSets, DataSortedSets, DataExpires, DataIntSets, DataIntSortedSets
        db.create_tables([DataStrings, DataSets, DataIntSets,
                          DataSortedSets, DataIntSortedSets, DataExpires], safe=True)

    if import_cities:
        from server.db import redis
        from geodis.city import City

        redis.delete(City.getGeohashIndexKey())
        with open('data/wigo_cities.json') as f:
            pipe = redis.pipeline()
            for line in f:
                try:
                    row = [x.encode('utf-8') for x in ujson.loads(line)]
                    print 'importing {}'.format(row[7])

                    loc = City(
                        continentId=row[0],
                        continent=row[1],
                        countryId=row[2],
                        country=row[3],
                        stateId=row[4],
                        state=row[5],
                        cityId=row[6],
                        name=row[7],
                        lat=float(row[8]),
                        lon=float(row[9]),
                        aliases=row[10],
                        population=int(row[11])
                    )

                    loc.save(pipe)

                    try:
                        Group.find(city_id=loc.cityId)
                    except DoesNotExist:
                        Group({
                            'name': loc.name,
                            'code': re.sub(r'([^\s\w]|_)+', '_', loc.name.lower()),
                            'latitude': loc.lat,
                            'longitude': loc.lon,
                            'city_id': loc.cityId,
                            'verified': True
                        }).save()

                except Exception, e:
                    logging.exception("Could not import line %s: %s", line, e)
                    return

        pipe.execute()



@clize
def import_old_db(groups=False, users=False, friends=False):
    logconfig.configure('dev')
    Configuration.CAPTURE_IMAGES = False

    rdbms = DataSet(Configuration.DATABASE_URL)

    num_saved = 0
    groups_table = rdbms['group']
    users_table = rdbms['user']
    follow_table = rdbms['follow']
    aa_table = rdbms['accountassociation']

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
        for dbuser in users_table.find(email_validated=True):
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
                user.group_id = dbuser['group']
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
            select t1.user_id, t1.follow_id from follow t1, follow t2 where t1.user_id = t2.follow_id and t1.follow_id = t2.user_id and
            t1.accepted is True and t2.accepted is True
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


if __name__ == '__main__':
    run((deploy, initialize, import_old_db))