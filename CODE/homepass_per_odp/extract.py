###############################################################################
### extract.py
### Download CSV dari VM1 via SCP ke local DOWNLOADDIR
###############################################################################

import os
import sys
import logging
import paramiko
from scp import SCPClient
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
VM1_HOST      = "10.53.176.18"
VM1_PORT      = 22
VM1_USERNAME  = os.getenv("VM1_USERNAME")
VM1_PASSWORD  = os.getenv("VM1_PASSWORD")
VM1_OUTPUTDIR = "/data/gd_sls_stgy/ahmadfan/OUTPUT/homepass_per_odp"

HOMEDIR = os.getenv("HOMEDIR")
DOWNLOADDIR = os.path.join(HOMEDIR, "DOWNLOAD", "homepass_per_odp")

FILE_PREFIX    = "homepass_per_odp_"
FILE_EXT       = ".csv"
DATE_FORMAT    = "%Y_%m_%d"
RETENTION_DAYS = 31

###############################################################################
# FUNGSI BANTU
###############################################################################
def validate_env(log):
    required = ["VM1_USERNAME", "VM1_PASSWORD", "HOMEDIR"]
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
    today  = datetime.now().date()
    cutoff = today - timedelta(days=retention_days)
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
    Download file CSV dari VM1 via SCP.
    Sebelum download, hapus dulu file lama (>31 hari) di DOWNLOADDIR.
    Return: local_path jika sukses, raise Exception jika gagal.
    """
    validate_env(log)
    os.makedirs(DOWNLOADDIR, exist_ok=True)

    filename    = f"{FILE_PREFIX}{datefiltername}{FILE_EXT}"
    remote_path = f"{VM1_OUTPUTDIR}/{filename}"
    local_path  = os.path.join(DOWNLOADDIR, filename)

    log.info(f"[EXTRACT] Target file    : {filename}")
    log.info(f"[EXTRACT] Remote path    : {remote_path}")
    log.info(f"[EXTRACT] Local dest     : {local_path}")
    log.info(f"[EXTRACT] Connecting to  : {VM1_HOST}:{VM1_PORT} (user: {VM1_USERNAME})")

    # Cleanup file lama di DOWNLOADDIR sebelum download
    cleanup_old_files(
        folder         = DOWNLOADDIR,
        prefix         = FILE_PREFIX,
        ext            = FILE_EXT,
        date_fmt       = DATE_FORMAT,
        retention_days = RETENTION_DAYS,
        log            = log,
    )

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=VM1_HOST,
        port=VM1_PORT,
        username=VM1_USERNAME,
        password=VM1_PASSWORD
    )
    log.info("[EXTRACT] Connected. Starting download...")

    with SCPClient(ssh.get_transport()) as scp:
        scp.get(remote_path, local_path)

    ssh.close()
    log.info("[EXTRACT] Download complete. Connection closed.")

    # Validasi hasil download
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File not found after download: {local_path}")

    file_size = os.path.getsize(local_path)
    if file_size == 0:
        raise ValueError(f"File is empty after download: {local_path}")

    log.info(f"[EXTRACT] File OK. Size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
    return local_path


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
        log.info(f"[EXTRACT] Done. File at: {result}")
    except Exception as e:
        log.error(f"[EXTRACT] Failed: {e}")
        sys.exit(1)