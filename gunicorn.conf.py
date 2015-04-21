import os

bind = '0.0.0.0:%s' % os.environ.get('PORT', 5200)
timeout = 15
graceful_timeout = 10
backlog = 100
max_requests = 0
workers = int(os.environ.get('WEB_NUM_PROCS', 10))
loglevel = 'warn'
preload_app = False  # for safety ALWAYS leave this FALSE, esp if gevent is on
