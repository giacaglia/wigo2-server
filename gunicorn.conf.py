import os

bind = 'unix:/tmp/nginx.socket'
timeout = 15
graceful_timeout = 10
backlog = 100
max_requests = 0
workers = int(os.environ.get('WEB_NUM_PROCS', 10))
loglevel = 'warn'
preload_app = False  # for safety ALWAYS leave this FALSE, esp if gevent is on


def touch(path):
    import os, time

    now = time.time()
    try:
        # assume it's there
        os.utime(path, (now, now))
    except os.error:
        open(path, "w").close()
        os.utime(path, (now, now))


def post_fork(server, worker):
    touch('/tmp/app-initialized')


def post_request(worker, req, environ, resp):
    from server.dao import db
    import psycopg2.extensions as _ext

    if not db.is_closed():
        status = db.get_conn().get_transaction_status()
        if status != _ext.TRANSACTION_STATUS_IDLE:
            db.rollback()
        db.close()

    for slave in db.slaves:
        if not slave.is_closed():
            status = slave.get_conn().get_transaction_status()
            if status != _ext.TRANSACTION_STATUS_IDLE:
                slave.rollback()
            slave.close()


def worker_exit(server, worker):
    from blade.services.bus import shutdown
    shutdown.send()