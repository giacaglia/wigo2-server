from __future__ import absolute_import

import logging
import cPickle
import logconfig

from redis import ReadOnlyError
from time import sleep
from clize import clize, run
from config import Configuration

from server.db import redis, wigo_rdbms
from server.rdbms import db
from utils import BreakHandler


@clize
def start(debug=False):
    logconfig.configure(Configuration.ENVIRONMENT)
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
                        logger.debug('{} sync commands completed'.format(num_run))

            if not found_something:
                logger.debug('{} sync commands completed, resetting count'.format(num_run))
                num_run = 0
                sleep(5)

        except Exception, e:
            logger.exception('error processing data queue')
            sleep(2)


if __name__ == '__main__':
    run(start)
