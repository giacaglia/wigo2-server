from __future__ import absolute_import

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import logging
import logconfig
import ujson
from playhouse.dataset import DataSet
from schematics.exceptions import ModelValidationError
from config import Configuration
from clize import run, clize
from server.models import IntegrityException
from server.models.group import Group
from server.models.user import User, Friend

logger = logging.getLogger('wigo.cmdline')


@clize
def deploy():
    pass


@clize
def create_tables():
    from server.rdbms import db, DataStrings, DataSets, DataSortedSets, DataExpires, DataIntSets, DataIntSortedSets

    db.create_tables([DataStrings, DataSets, DataIntSets,
                      DataSortedSets, DataIntSortedSets, DataExpires], safe=True)


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
    run((deploy, create_tables, import_old_db))