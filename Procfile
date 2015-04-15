web: bin/start-pgbouncer-stunnel bin/start-nginx newrelic-admin run-program gunicorn -c gunicorn.conf.py web:app
sync: python rdbms_sync.py