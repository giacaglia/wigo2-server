web: bin/start-nginx newrelic-admin run-program gunicorn -c gunicorn.conf.py web:app
high_worker: newrelic-admin run-program python start_worker.py -c worker data data-priority scheduled
medium_worker: newrelic-admin run-program python start_worker.py -c worker notifications push predictions
low_worker: newrelic-admin run-program python start_worker.py -c worker parse images email
sync: python rdbms_sync.py