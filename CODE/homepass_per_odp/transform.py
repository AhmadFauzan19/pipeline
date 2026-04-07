###############################################################################
### transform.py
### Join CSV hasil extract dengan referensi STO, output Excel split per AREA
###############################################################################

import os
import sys
import pandas as pd
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
DOWNLOADDIR = os.path.join(HOMEDIR, "DOWNLOAD", "homepass_per_odp")
OUTPUTDIR   = os.path.join(HOMEDIR, "OUTPUT",   "homepass_per_odp")
FILE_RIGHT   = os.path.join(HOMEDIR, "REF",   "Final Ref STO & Class WOK NGPP Vol 2 (154 WOK) v3.2 -per Desember 2025.xlsx")

SHEET_RIGHT = "Ref STO"
COLS_RIGHT  = ["STO", "AREA ", "REGIONAL New", "BRANCH 2025", "WOK Vol 2 (2025)"]

JOIN_ON   = {"left": "sto", "right": "STO"}
JOIN_TYPE = "left"

DROP_COLS = ["STO"]

RENAME_COLS = {
    "sto"              : "STO",
    "AREA "            : "AREA",
    "REGIONAL New"     : "REGIONAL",
    "BRANCH 2025"      : "BRANCH",
    "WOK Vol 2 (2025)" : "WOK",
    "odp_name"         : "ODP NAME",
    "total_homepass"   : "TOTAL HOMEPASS",
}

OUTPUT_COL_ORDER = ["AREA", "REGIONAL", "BRANCH", "WOK", "STO", "ODP NAME", "TOTAL HOMEPASS"]

SHEET_COL      = "AREA"
SHEET_EMPTY    = "NO AREA"
MAX_ROWS_SHEET = 1_000_000

FILE_PREFIX    = "homepass_per_odp_"
FILE_EXT_OUT   = ".xlsx"
DATE_FORMAT    = "%Y_%m_%d"
RETENTION_DAYS = 7

###############################################################################
# FUNGSI BANTU
###############################################################################
def validate_env(log):
    required = ["HOMEDIR"]
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


def clean_csv(path, log):
    """
    Baca CSV baris per baris, buang baris yang jumlah field-nya
    tidak sama dengan header. Return path file CSV yang sudah bersih.
    """
    clean_path = path.replace(".csv", "_cleaned.csv")
    removed    = 0

    with open(path, "r", encoding="utf-8", errors="replace") as fin, \
         open(clean_path, "w", encoding="utf-8") as fout:

        header   = fin.readline()
        fout.write(header)
        expected = header.count(",") + 1

        for i, line in enumerate(fin, start=2):
            actual = line.count(",") + 1
            if actual == expected:
                fout.write(line)
            else:
                removed += 1
                log.warning(
                    f"[CLEAN] Line {i} dibuang: expected {expected} fields, "
                    f"got {actual} | {line.rstrip()[:120]}"
                )

    log.info(f"[CLEAN] Selesai. {removed} baris dibuang. File bersih: {clean_path}")
    return clean_path


def read_file(path, sheet, cols, log):
    ext = path.rsplit(".", 1)[-1].lower()
    if ext == "csv":
        clean_path = clean_csv(path, log)
        df = pd.read_csv(clean_path)
    else:
        df = pd.read_excel(path, sheet_name=sheet)

    df.columns = df.columns.str.strip()

    if cols:
        cols = [c.strip() for c in cols]
        df = df[cols]

    return df


def sanitize_sheet_name(name, max_len=31):
    for ch in r'\/?*[]:':
        name = name.replace(ch, "_")
    return name[:max_len]


def write_excel_by_area(df, path, sheet_col, empty_sheet_name, max_rows, log):
    mask_empty = df[sheet_col].isna() | (df[sheet_col].astype(str).str.strip() == "")
    df_empty   = df[mask_empty]
    df_filled  = df[~mask_empty]

    areas = sorted(df_filled[sheet_col].unique())

    log.info(f"[TRANSFORM] Area ditemukan    : {len(areas)}")
    log.info(f"[TRANSFORM] Baris tanpa area  : {len(df_empty)}")

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for area in areas:
            chunk      = df_filled[df_filled[sheet_col] == area].reset_index(drop=True)
            sheet_name = sanitize_sheet_name(str(area))

            if len(chunk) <= max_rows:
                chunk.to_excel(writer, sheet_name=sheet_name, index=False)
                log.info(f"[TRANSFORM] Sheet '{sheet_name}': {len(chunk)} baris")
            else:
                for part_idx, start in enumerate(range(0, len(chunk), max_rows), start=1):
                    part_name = sanitize_sheet_name(f"{sheet_name}_{part_idx}")
                    part_df   = chunk.iloc[start:start + max_rows]
                    part_df.to_excel(writer, sheet_name=part_name, index=False)
                    log.info(f"[TRANSFORM] Sheet '{part_name}': {len(part_df)} baris")

        if not df_empty.empty:
            empty_name = sanitize_sheet_name(empty_sheet_name)
            df_empty.reset_index(drop=True).to_excel(writer, sheet_name=empty_name, index=False)
            log.info(f"[TRANSFORM] Sheet '{empty_name}': {len(df_empty)} baris")

    log.info(f"[TRANSFORM] Output saved to: {path}")
    return path

###############################################################################
# FUNGSI UTAMA
###############################################################################
def run(datefiltername, log):
    """
    Join CSV hasil extract dengan ref STO, output Excel per AREA.
    Return: output_path jika sukses, raise Exception jika gagal.
    """
    os.makedirs(OUTPUTDIR, exist_ok=True)

    file_left   = os.path.join(DOWNLOADDIR, f"homepass_per_odp_{datefiltername}.csv")
    output_file = os.path.join(OUTPUTDIR,   f"homepass_per_odp_{datefiltername}.xlsx")

    log.info(f"[TRANSFORM] Input CSV  : {file_left}")
    log.info(f"[TRANSFORM] Input REF  : {FILE_RIGHT}")
    log.info(f"[TRANSFORM] Output     : {output_file}")

    # Validasi input files
    if not os.path.exists(file_left):
        raise FileNotFoundError(f"CSV input not found: {file_left}")
    if not os.path.exists(FILE_RIGHT):
        raise FileNotFoundError(f"REF file not found: {FILE_RIGHT}")

    # Cleanup file Excel lama di OUTPUTDIR
    cleanup_old_files(
        folder         = OUTPUTDIR,
        prefix         = FILE_PREFIX,
        ext            = FILE_EXT_OUT,
        date_fmt       = DATE_FORMAT,
        retention_days = RETENTION_DAYS,
        log            = log,
    )

    # Read
    log.info(f"[TRANSFORM] Reading CSV ...")
    df_left = read_file(file_left, 0, None, log)
    log.info(f"[TRANSFORM] CSV rows: {len(df_left)}, cols: {list(df_left.columns)}")

    log.info(f"[TRANSFORM] Reading REF STO ...")
    df_right = read_file(FILE_RIGHT, SHEET_RIGHT, COLS_RIGHT, log)
    log.info(f"[TRANSFORM] REF rows: {len(df_right)}, cols: {list(df_right.columns)}")

    # Cast join keys ke string dan strip
    col_left  = JOIN_ON["left"]
    col_right = JOIN_ON["right"]
    df_left[col_left]   = df_left[col_left].astype(str).str.strip()
    df_right[col_right] = df_right[col_right].astype(str).str.strip()
    log.info(f"[TRANSFORM] Join key cast OK. LEFT: '{col_left}', RIGHT: '{col_right}'")

    # Join
    log.info(f"[TRANSFORM] Performing {JOIN_TYPE.upper()} JOIN ...")
    df_result = pd.merge(df_left, df_right, how=JOIN_TYPE,
                         left_on=col_left, right_on=col_right)
    log.info(f"[TRANSFORM] Join result: {len(df_result)} rows")

    # Drop
    df_result = df_result.drop(columns=[c for c in DROP_COLS if c in df_result.columns])

    # Rename
    df_result = df_result.rename(columns=RENAME_COLS)

    # Reorder kolom
    ordered   = [c for c in OUTPUT_COL_ORDER if c in df_result.columns]
    rest      = [c for c in df_result.columns if c not in ordered]
    df_result = df_result[ordered + rest]
    log.info(f"[TRANSFORM] Output cols: {list(df_result.columns)}")

    # Write Excel
    output_path = write_excel_by_area(
        df_result, output_file, SHEET_COL, SHEET_EMPTY, MAX_ROWS_SHEET, log
    )

    return output_path


###############################################################################
# STANDALONE RUN (opsional, bisa dirun langsung)
###############################################################################
if __name__ == "__main__":
    import logging
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
        log.info(f"[TRANSFORM] Done. File at: {result}")
    except Exception as e:
        log.error(f"[TRANSFORM] Failed: {e}")
        sys.exit(1)