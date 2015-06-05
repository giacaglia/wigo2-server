web: bin/start-nginx newrelic-admin run-program gunicorn -c gunicorn.conf.py web:app
high_worker: newrelic-admin run-program python start_worker.py -c worker data data-priority scheduled
medium_worker: newrelic-admin run-program python start_worker.py -c worker notifications push
low_worker: newrelic-admin run-program python start_worker.py -c worker images email
predictions_worker: newrelic-admin run-program python start_worker.py -c worker predictions
sync: python rdbms_sync.py