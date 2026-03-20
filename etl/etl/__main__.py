import os
import time

from hn_etl import main


if __name__ == "__main__":
    # Controls the re-runs for the ETL loop
    sleep_minutes = int(os.getenv("ETL_SLEEP_MINUTES", "300"))
    sleep_seconds = int(os.getenv("ETL_SLEEP_SECONDS", "300"))

    if sleep_minutes is not None:
        sleep_seconds = int(sleep_minutes) * 60

    run_once = os.getenv("ETL_RUN_ONCE", "false").lower() in ("1", "true", "yes")

    while True:
        # Start one ETL run (fetch + upsert cycle)
        main()
        if run_once:
            # exit after a single run when ETL_RUN_ONCE is true
            break
        if sleep_seconds > 0:
            # wait before the next upsert run
            time.sleep(sleep_seconds)
