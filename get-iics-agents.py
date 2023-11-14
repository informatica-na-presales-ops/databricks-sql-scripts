import apscheduler.schedulers.blocking
import datime
import dbx.cnx
import datetime
import notch
import os
import pg
import signal
import sys
import time

log = notch.make_log('databricks_sql_scripts.get_iics_agents')


def main_job(repeat_interval_hours: int = None):
    start = time.monotonic()
    log.info('Running the main job')

    dbx_cnx = dbx.cnx.get_connection(os.getenv('DBX_HOSTNAME'), os.getenv('DBX_HTTP_PATH'), os.getenv('DBX_TOKEN'))
    pg_cnx = pg.cnx.get_connection(os.getenv('PGSQL_DSN'))

    updated_at_start = pg.data_lake_postgres.get_iics_agents_max_record_updated_at(pg_cnx)
    if updated_at_start is None:
        updated_at_start = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)

    records = []
    total = 0

    for row in dbx.cnx.get_iics_agents(dbx_cnx, updated_at_start, close_cnx=False):
        total += 1
        records.append(row)
        if len(records) > 999:
            pg.data_lake_postgres.batch_upsert_iics_agents(pg_cnx, records)
            records = []

    if len(records) > 0:
        pg.data_lake_postgres.batch_upsert_iics_agents(pg_cnx, records)
        pass

    log.info(f'Total records: {total}')

    adv_configs = list(dbx.cnx.get_iics_advanced_cluster_configs(dbx_cnx, close_cnx=False))
    pg.data_lake_postgres.batch_upsert_iics_advanced_cluster_configs(pg_cnx, adv_configs)

    adv_instances = list(dbx.cnx.get_iics_advanced_cluster_instances(dbx_cnx, close_cnx=False))
    pg.data_lake_postgres.batch_upsert_iics_advanced_cluster_instances(pg_cnx, adv_instances)

    serverless = list(dbx.cnx.get_iics_serverless_environments(dbx_cnx))
    pg.data_lake_postgres.batch_upsert_iics_serverless_environments(pg_cnx, serverless)

    if repeat_interval_hours:
        plural = 's'
        if repeat_interval_hours == 1:
            plural = ''
        repeat_message = f'see you again in {repeat_interval_hours} hour{plural}'
    else:
        repeat_message = 'quitting'
    duration = int(time.monotonic() - start)
    log.info(f'Main job complete in {datime.pretty_duration_short(duration)}, {repeat_message}')


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
