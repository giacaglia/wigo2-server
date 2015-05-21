web: newrelic-admin run-program gunicorn -c gunicorn.conf.py web:app
worker: python start_worker -c worker
sync: python rdbms_sync.py