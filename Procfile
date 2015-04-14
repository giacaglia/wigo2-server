web: bin/start-pgbouncer-stunnel bin/start-nginx newrelic-admin run-program gunicorn -c gunicorn.conf.py web:app
worker: celery -A worker worker --loglevel=info
sync: python rdbms_sync.py