web: bin/start-nginx newrelic-admin run-program gunicorn -c gunicorn.conf.py web:app
worker: newrelic-admin run-program python start_worker.py -c worker data data-priority
low_priority_worker: newrelic-admin run-program python start_worker.py -c worker email images notifications push parse predictions scheduled notifications
sync: python rdbms_sync.py