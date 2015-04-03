web: bin/start-pgbouncer-stunnel bin/start-nginx newrelic-admin run-program gunicorn -c gunicorn.conf.py web:app
worker: bin/start-pgbouncer-stunnel newrelic-admin run-program python worker.py