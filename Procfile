web: bin/start-pgbouncer-stunnel bin/start-nginx newrelic-admin run-program gunicorn -c gunicorn.conf.py web:app
worker: rqgeventworker -q --url $REDIS_URL email images notifications
sync: python rdbms_sync.py