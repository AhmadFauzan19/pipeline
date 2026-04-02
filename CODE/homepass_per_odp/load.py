###############################################################################
### load.py
### Copy file Excel hasil transform dari local OUTPUTDIR ke OneDrive synced folder
###############################################################################

import os
import sys
import shutil
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

###############################################################################
# LOAD .env
###############################################################################
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

###############################################################################
# CONFIG
###############################################################################
HOMEDIR = os.getenv("HOMEDIR")
ONEDRIVE = os.getenv("ONEDRIVE")
OUTPUTDIR = os.path.join(HOMEDIR, "OUTPUT", "homepass_per_odp")

FILE_PREFIX    = "homepass_per_odp_"
FILE_EXT       = ".xlsx"
DATE_FORMAT    = "%Y_%m_%d"
RETENTION_DAYS = 7   # file dengan tanggal lebih dari ini (dari hari ini) akan dihapus

###############################################################################
# FUNGSI BANTU
###############################################################################
def validate_env(log):
    required = ["HOMEDIR", "ONEDRIVE"]
    missing  = [v for v in required if not os.getenv(v)]
    if missing:
        log.critical(f"[ENV] Missing required variable(s): {', '.join(missing)}")
        sys.exit(1)
    log.info("[ENV] All required variables loaded OK.")


def cleanup_old_files(folder, prefix, ext, date_fmt, retention_days, log):
    """
    Hapus file di folder yang tanggalnya (dari nama file) lebih dari retention_days
    dihitung dari hari ini. Nama file format: {prefix}{YYYY_MM_DD}{ext}
    """
    today   = datetime.now().date()
    cutoff  = today - timedelta(days=retention_days)
    deleted = 0

    log.info(f"[CLEANUP] Checking files older than {retention_days} days in: {folder}")
    log.info(f"[CLEANUP] Cutoff date: {cutoff} (file tanggal <= ini akan dihapus)")

    for fname in os.listdir(folder):
        if not (fname.startswith(prefix) and fname.endswith(ext)):
            continue

        date_str = fname[len(prefix):-len(ext)]
        try:
            file_date = datetime.strptime(date_str, date_fmt).date()
        except ValueError:
            log.warning(f"[CLEANUP] Skip '{fname}': format tanggal tidak dikenali")
            continue

        if file_date <= cutoff:
            fpath = os.path.join(folder, fname)
            try:
                os.remove(fpath)
                deleted += 1
                log.info(f"[CLEANUP] Deleted: {fname} (date: {file_date})")
            except Exception as e:
                log.warning(f"[CLEANUP] Gagal hapus '{fname}': {e}")

    log.info(f"[CLEANUP] Done. {deleted} file(s) deleted.")


###############################################################################
# FUNGSI UTAMA
###############################################################################
def run(datefiltername, log):
    """
    Copy file Excel hasil transform dari OUTPUTDIR ke OneDrive folder.
    Sebelum copy, hapus dulu file lama (>31 hari) di OneDrive folder.
    Return: dest_path jika sukses, raise Exception jika gagal.
    """
    filename   = f"{FILE_PREFIX}{datefiltername}{FILE_EXT}"
    local_path = os.path.join(OUTPUTDIR, filename)

    log.info(f"[LOAD] Target file   : {filename}")
    log.info(f"[LOAD] Source        : {local_path}")
    log.info(f"[LOAD] Destination   : {ONEDRIVE}")

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Source file not found: {local_path}")

    file_size = os.path.getsize(local_path)
    if file_size == 0:
        raise ValueError(f"Source file is empty: {local_path}")

    log.info(f"[LOAD] Source file OK. Size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")

    if not os.path.exists(ONEDRIVE):
        raise FileNotFoundError(f"OneDrive folder not found: {ONEDRIVE}")

    # Cleanup file lama di OneDrive sebelum upload
    cleanup_old_files(
        folder         = ONEDRIVE,
        prefix         = FILE_PREFIX,
        ext            = FILE_EXT,
        date_fmt       = DATE_FORMAT,
        retention_days = RETENTION_DAYS,
        log            = log,
    )

    dest_path = os.path.join(ONEDRIVE, filename)
    shutil.copy2(local_path, dest_path)

    log.info(f"[LOAD] Copy complete. File will sync automatically.")
    log.info(f"[LOAD] Dest path: {dest_path}")
    return dest_path


###############################################################################
# STANDALONE RUN (opsional, bisa dirun langsung)
###############################################################################
if __name__ == "__main__":
    datefiltername = (datetime.now() - timedelta(days=1)).strftime("%Y_%m_%d")

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    log = logging.getLogger()

    try:
        result = run(datefiltername, log)
        log.info(f"[LOAD] Done. File at: {result}")
    except Exception as e:
        log.error(f"[LOAD] Failed: {e}")
        sys.exit(1)