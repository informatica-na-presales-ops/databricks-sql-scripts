import logging
import os
import signal
import sys
import time

import apscheduler.schedulers.blocking
import datime
import notch

import dbx.cnx
import pg

notch.configure()
log = logging.getLogger(__name__)


def main_job(repeat_interval_hours: int | None = None) -> None:
    start = time.monotonic()
    log.info("Running the main job")

    dbx_cnx = dbx.cnx.get_connection(
        os.getenv("DBX_HOSTNAME"), os.getenv("DBX_HTTP_PATH"), os.getenv("DBX_TOKEN")
    )
    pg_cnx = pg.cnx.get_connection(os.getenv("PGSQL_DSN"))

    records = []
    total = 0

    for row in dbx.cnx.get_iics_user_weekly_logins(dbx_cnx):
        total += 1
        records.append(row)
        if len(records) > 999:
            pg.data_lake_postgres.batch_upsert_iics_weekly_logins(pg_cnx, records)
            records = []

    if len(records) > 0:
        pg.data_lake_postgres.batch_upsert_iics_weekly_logins(pg_cnx, records)

    log.info(f"Total records: {total}")

    if repeat_interval_hours:
        plural = "s"
        if repeat_interval_hours == 1:
            plural = ""
        repeat_message = f"see you again in {repeat_interval_hours} hour{plural}"
    else:
        repeat_message = "quitting"
    duration = int(time.monotonic() - start)
    duration_text = datime.pretty_duration_short(duration)
    log.info("Main job complete in %s, %s", duration_text, repeat_message)


def main() -> None:
    repeat = os.getenv("REPEAT", "false").lower() in ("1", "on", "true", "yes")
    if repeat:
        repeat_interval_hours = int(os.getenv("REPEAT_INTERVAL_HOURS", "1"))
        log.info(f"This job will repeat every {repeat_interval_hours} hours")
        log.info(
            "Change this value by setting the REPEAT_INTERVAL_HOURS "
            "environment variable"
        )
        scheduler = apscheduler.schedulers.blocking.BlockingScheduler()
        scheduler.add_job(
            main_job,
            "interval",
            args=[repeat_interval_hours],
            hours=repeat_interval_hours,
        )
        scheduler.add_job(main_job, args=[repeat_interval_hours])
        scheduler.start()
    else:
        main_job()


def handle_sigterm(_signal: int, _frame: object) -> None:
    sys.exit()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
