web: bin/start-nginx newrelic-admin run-program gunicorn -c gunicorn.conf.py web:app
worker: newrelic-admin run-program python start_worker.py -c worker data data-priority scheduled
low_priority_worker: newrelic-admin run-program python start_worker.py -c worker notifications push predictions
very_low_priority_worker: newrelic-admin run-program python start_worker.py -c worker parse images email
sync: python rdbms_sync.py