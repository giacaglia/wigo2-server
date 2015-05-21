from __future__ import absolute_import
import click

import logconfig
from config import Configuration

logconfig.configure(Configuration.ENVIRONMENT)

import logging
import cPickle

from redis import ReadOnlyError
from time import sleep

from server.db import redis, wigo_rdbms
from server.rdbms import db
from utils import BreakHandler


@click.command()
@click.option('--debug', type=bool)
def start(debug=False):
    logger = logging.getLogger('wigo.rdbms.sync')

    bh = BreakHandler()
    bh.enable()
    logger.info('draining command queue')
    num_run = 0

    while True:
        try:
            if bh.trapped:
                print 'stopping at user request (keyboard interrupt)...'
                break

            found_something = False

            try:
                command = redis.lindex('db:queue:commands', -1)
            except ReadOnlyError:
                logger.warn('error popping item from redis queue, redis in readonly mode, retrying')
                sleep(5)
                continue
            except:
                logger.exception('error popping item from redis queue, retrying')
                sleep(1)
                continue
            else:
                if command:
                    found_something = True
                    parsed = cPickle.loads(command)
                    fn = getattr(wigo_rdbms, parsed[0])
                    with db.transaction():
                        fn(*parsed[1:])
                    redis.rpop('db:queue:commands')
                    num_run += 1

                    if (num_run % 500) == 0:
                        logger.info('{} sync commands completed'.format(num_run))

            if not found_something:
                if num_run > 0:
                    logger.info('{} sync commands completed, resetting count'.format(num_run))
                    num_run = 0
                sleep(5)

        except Exception, e:
            logger.exception('error processing data queue')
            sleep(2)


if __name__ == '__main__':
    start()
