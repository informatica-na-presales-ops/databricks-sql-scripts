import apscheduler.schedulers.blocking
import dbx.cnx
import common
import datetime
import notch
import os
import pg
import signal
import sys
import time

log = notch.make_log('databricks_sql_scripts.get_iics_user_logs')


def main_job(repeat_interval_hours: int = None):
    start = time.monotonic()
    log.info('Running the main job')

    dbx_cnx = dbx.cnx.get_connection(os.getenv('DBX_HOSTNAME'), os.getenv('DBX_HTTP_PATH'), os.getenv('DBX_TOKEN'))
    pg_cnx = pg.cnx.get_connection(os.getenv('PGSQL_DSN'))

    key_start = pg.data_lake_postgres.get_iics_user_logs_max_user_log_key(pg_cnx)
    if key_start is None:
        key_start = 0

    records = []
    total = 0

    for row in dbx.cnx.get_iics_user_logs(dbx_cnx, key_start):
        total += 1
        records.append(row)
        if len(records) > 999:
            pg.data_lake_postgres.batch_insert_iics_user_logs(pg_cnx, records)
            records = []

    if len(records) > 0:
        pg.data_lake_postgres.batch_insert_iics_user_logs(pg_cnx, records)
        pass

    log.info(f'Total records: {total}')

    if repeat_interval_hours:
        plural = 's'
        if repeat_interval_hours == 1:
            plural = ''
        repeat_message = f'see you again in {repeat_interval_hours} hour{plural}'
    else:
        repeat_message = 'quitting'
    duration = int(time.monotonic() - start)
    log.info(f'Main job complete in {common.human_duration(duration)}, {repeat_message}')


def main():
    repeat = os.getenv('REPEAT', 'false').lower() in ('1', 'on', 'true', 'yes')
    if repeat:
        repeat_interval_hours = int(os.getenv('REPEAT_INTERVAL_HOURS', '1'))
        log.info(f'This job will repeat every {repeat_interval_hours} hours')
        log.info('Change this value by setting the REPEAT_INTERVAL_HOURS environment variable')
        scheduler = apscheduler.schedulers.blocking.BlockingScheduler()
        scheduler.add_job(main_job, 'interval', args=[repeat_interval_hours], hours=repeat_interval_hours)
        scheduler.add_job(main_job, args=[repeat_interval_hours])
        scheduler.start()
    else:
        main_job()


def handle_sigterm(_signal, _frame):
    sys.exit()


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
