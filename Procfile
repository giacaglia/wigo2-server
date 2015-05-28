web: newrelic-admin run-program gunicorn -c gunicorn.conf.py web:app
worker: newrelic-admin run-program python start_worker.py -c worker
sync: python rdbms_sync.py