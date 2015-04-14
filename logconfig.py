from __future__ import absolute_import

import logging
import logging.config


def notify(self, message, *args, **kws):
    self.info('NOTIFY: %s' % message, *args, **kws)


def alert(self, message, *args, **kws):
    self.info('ALERT: %s' % message, *args, **kws)


def ops_alert(self, message, *args, **kws):
    self.info('OPS_ALERT: %s' % message, *args, **kws)


def configure(env):
    logging.Logger.notify = notify
    logging.Logger.alert = alert
    logging.Logger.ops_alert = ops_alert

    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,

        'formatters': {
            'standard': {
                'format': '%(name)s - %(levelname)s - %(message)s'
            },
        },
        'handlers': {
            'default': {
                'class': 'logging.StreamHandler',
                'formatter': 'standard'
            },
        },
        'root': {
            'handlers': ['default'],
            'level': 'WARN',
        },
        'loggers': {
            'wigo': {
                'level': 'DEBUG' if env == 'dev' else 'INFO',
            },
            'blade': {
                'level': 'DEBUG' if env == 'dev' else 'INFO',
            },
            'peewee': {
                'level': 'DEBUG' if env == 'dev' else 'INFO',
            }
        }
    })
