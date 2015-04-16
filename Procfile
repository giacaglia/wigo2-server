web: bin/start-pgbouncer-stunnel bin/start-nginx newrelic-admin run-program gunicorn -c gunicorn.conf.py web:app
worker: rqgeventworker -q --results-ttl 0 -c worker
sync: python rdbms_sync.py