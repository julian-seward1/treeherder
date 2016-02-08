web: newrelic-admin run-program gunicorn treeherder.config.wsgi:application --log-file - --timeout 29 --max-requests 2000
worker_beat: newrelic-admin run-program celery -A treeherder beat
worker_pushlog: newrelic-admin run-program celery -A treeherder worker -Q pushlog,fetch_missing_push_logs --maxtasksperchild=500 --concurrency=5
worker_buildapi_pending: newrelic-admin run-program celery -A treeherder worker -Q buildapi_pending --maxtasksperchild=20 --concurrency=5
worker_buildapi_running: newrelic-admin run-program celery -A treeherder worker -Q buildapi_running --maxtasksperchild=20 --concurrency=5
worker_buildapi_4hr: newrelic-admin run-program celery -A treeherder worker -Q buildapi_4hr --maxtasksperchild=20 --concurrency=1
worker_default: newrelic-admin run-program celery -A treeherder worker -Q default,cycle_data,calculate_durations,fetch_bugs,autoclassify,detect_intermittents,fetch_allthethings,generate_perf_alerts --maxtasksperchild=50 --concurrency=3
worker_hp: newrelic-admin run-program celery -A treeherder worker -Q classification_mirroring,publish_to_pulse --maxtasksperchild=50 --concurrency=1
worker_log_parser: newrelic-admin run-program celery -A treeherder worker -Q log_parser_fail,log_parser,log_parser_json,error_summary,after_logs_parsed,store_error_summary, crossreference_error_lines,log_parser_hp,log_parser_json_hp,error_summary_hp,after_logs_parsed_hp,store_error_summary_hp,crossreference_error_lines_hp --maxtasksperchild=50 --concurrency=5
