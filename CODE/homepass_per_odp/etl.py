###############################################################################
### run_pipeline.py
### Orchestrator : jalankan extract.py lalu load.py secara berurutan
### Scheduled   : daily 08:00 (setelah hadoop dump jam 06:00)
###############################################################################

import os
import sys
import logging
import platform
from datetime import datetime, timedelta

import extract
import transform
import load

###############################################################################
# CONFIG
###############################################################################
SCRIPT_NAME    = "homepass_per_odp_daily"
SCRIPT_VERSION = "2.0.0"

HOMEDIR = os.getenv("HOMEDIR")
LOGDIR             = os.path.join(HOMEDIR, "LOGS", "homepass_per_odp")
LOG_RETENTION_DAYS = 30

###############################################################################
# SETUP
###############################################################################
datefiltername = (datetime.now() - timedelta(days=1)).strftime("%Y_%m_%d")

os.makedirs(LOGDIR, exist_ok=True)
LOGFILE = os.path.join(LOGDIR, f"{SCRIPT_NAME}_{datefiltername}.log")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOGFILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger()

###############################################################################
# FUNGSI : cleanup log lama
###############################################################################
def validate_env(log):
    required = ["HOMEDIR"]
    missing  = [v for v in required if not os.getenv(v)]
    if missing:
        log.critical(f"[ENV] Missing required variable(s): {', '.join(missing)}")
        sys.exit(1)
    log.info("[ENV] All required variables loaded OK.")


def cleanup_old_logs():
    log.info(f"[CLEANUP] Checking logs older than {LOG_RETENTION_DAYS} days in: {LOGDIR}")
    cutoff  = datetime.now() - timedelta(days=LOG_RETENTION_DAYS)
    deleted = 0
    for fname in os.listdir(LOGDIR):
        if not fname.startswith(SCRIPT_NAME):
            continue
        fpath = os.path.join(LOGDIR, fname)
        if os.path.isfile(fpath) and fpath != LOGFILE:
            fmtime = datetime.fromtimestamp(os.path.getmtime(fpath))
            if fmtime < cutoff:
                os.remove(fpath)
                deleted += 1
                log.info(f"[CLEANUP] Deleted: {fname}")
    log.info(f"[CLEANUP] Done. {deleted} file(s) deleted.")

###############################################################################
# MAIN
###############################################################################
def main():
    start_time = datetime.now()

    log.info("=" * 70)
    log.info(f"  SCRIPT      : {SCRIPT_NAME} v{SCRIPT_VERSION}")
    log.info(f"  HOSTNAME    : {platform.node()}")
    log.info(f"  PYTHON      : {sys.version.split()[0]}")
    log.info(f"  DATE FILTER : {datefiltername}")
    log.info(f"  LOGFILE     : {LOGFILE}")
    log.info("=" * 70)

    cleanup_old_logs()

    # ------------------------------------------------------------------
    # STEP 1 : EXTRACT - download dari VM1
    # ------------------------------------------------------------------
    log.info("[PIPELINE] Step 1/3 : EXTRACT - download from VM1")
    try:
        local_path = extract.run(datefiltername, log)
        log.info(f"[PIPELINE] Extract OK. Local file: {local_path}")
    except Exception as e:
        log.error(f"[PIPELINE] Extract FAILED: {e}")
        log.error("[PIPELINE] Pipeline aborted.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # STEP 2 : TRANSFORM - join dengan ref STO, output Excel
    # ------------------------------------------------------------------
    log.info("[PIPELINE] Step 2/3 : TRANSFORM - join & write Excel")
    try:
        output_path = transform.run(datefiltername, log)
        log.info(f"[PIPELINE] Transform OK. Output file: {output_path}")
    except Exception as e:
        log.error(f"[PIPELINE] Transform FAILED: {e}")
        log.error("[PIPELINE] Pipeline aborted. Load step skipped.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # STEP 3 : LOAD - copy Excel ke OneDrive
    # ------------------------------------------------------------------
    log.info("[PIPELINE] Step 3/3 : LOAD - copy to OneDrive")
    try:
        dest_path = load.run(datefiltername, log)
        log.info(f"[PIPELINE] Load OK. Dest file: {dest_path}")
    except Exception as e:
        log.error(f"[PIPELINE] Load FAILED: {e}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # SUMMARY
    # ------------------------------------------------------------------
    elapsed = datetime.now() - start_time
    log.info("=" * 70)
    log.info(f"  RESULT  : SUCCESS")
    log.info(f"  ELAPSED : {str(elapsed).split('.')[0]}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()