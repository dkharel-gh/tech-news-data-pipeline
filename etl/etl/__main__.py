import os
import time

from hn_etl import main


if __name__ == "__main__":
    sleep_seconds = int(os.getenv("ETL_SLEEP_SECONDS", "300"))
    run_once = os.getenv("ETL_RUN_ONCE", "false").lower() in ("1", "true", "yes")

    while True:
        main()
        if run_once:
            break
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
