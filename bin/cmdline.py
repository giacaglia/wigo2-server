from __future__ import absolute_import

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import logging
import logconfig
import ujson
from schematics.exceptions import ModelValidationError
from config import Configuration
from clize import run, clize
from server.models import IntegrityException
from server.models.group import Group
from server.models.user import User

logger = logging.getLogger('wigo.cmdline')


@clize
def deploy():
    pass


@clize
def import_old_db():
    logconfig.configure('dev')
    Configuration.CAPTURE_IMAGES = False

    from server.rdbms import rdbms

    num_saved = 0
    groups = rdbms['group']
    users = rdbms['user']
    aa = rdbms['accountassociation']

    for dbgroup in groups.find():
        group = Group(dbgroup)

        try:
            group.validate()
            group.save()

            if (num_saved % 100) == 0:
                logger.info('saved {} groups'.format(num_saved))
            num_saved += 1
        except ModelValidationError, e:
            logger.error('model validation error, {}'.format(e.message))
            logger.error(ujson.dumps(dbgroup))

    num_saved = 0

    for dbuser in users.find(email_validated=True):
        properties = dbuser.get('properties')
        if isinstance(properties, dict) and 'images' in properties:
            images = properties.get('images')
            if isinstance(images, list):
                if len(images) > 0 and isinstance(images[0], basestring):
                    images = [{'url': img} for img in images]
                    dbuser['properties']['images'] = images
            else:
                dbuser['properties']['images'] = []

        assoc = aa.find_one(user=dbuser['id'])
        user = User(dbuser)

        try:
            user.group_id = dbuser['group']
            user.facebook_id = assoc['service']
            user.facebook_token = assoc['service_token']
            user.facebook_token_expires = assoc['service_token_expires']

            user.validate()

            user.save()

            if (num_saved % 100) == 0:
                logger.info('saved {} users'.format(num_saved))

            num_saved += 1
        except ModelValidationError, e:
            logger.error('model validation error, {}'.format(e.message))
            logger.error(ujson.dumps(dbuser))
        except IntegrityException, e:
            logger.error('model integrity error, {}'.format(e.message))
            logger.error(ujson.dumps(dbuser))


if __name__ == '__main__':
    run((deploy, import_old_db))