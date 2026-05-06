"""CSV storage helpers for source and generated pricing data."""

import csv
import io
import json
import os
import tempfile

from schema import COLUMNS


BASELINE_STATE_FILENAME = "_baseline_state.json"


def baseline_state_path(data_dir: str) -> str:
    return os.path.join(data_dir, BASELINE_STATE_FILENAME)


def load_baseline_state(data_dir: str, logger=None) -> dict:
    path = baseline_state_path(data_dir)
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        if logger:
            logger.warning(f"Failed to load baseline state from {path}: {e}")
        return {}


def cleanup_baseline_state(data_dir: str, logger=None) -> None:
    path = baseline_state_path(data_dir)
    if os.path.isfile(path):
        try:
            os.remove(path)
        except OSError:
            if logger:
                logger.warning(f"Failed to remove baseline state file: {path}")


def row_key(row: dict) -> tuple:
    return tuple(row.get(col, "") for col in COLUMNS)


def dedupe_rows(rows: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for row in rows:
        key = row_key(row)
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def read_appended_rows(path: str, baseline_size: int) -> list[dict]:
    if not os.path.isfile(path):
        return []

    current_size = os.path.getsize(path)
    if current_size <= baseline_size:
        return []

    with open(path, "rb") as f:
        f.seek(baseline_size)
        chunk = f.read()

    if not chunk.strip():
        return []

    text = chunk.decode("utf-8")
    if baseline_size == 0:
        reader = csv.DictReader(io.StringIO(text))
    else:
        reader = csv.DictReader(io.StringIO(text), fieldnames=COLUMNS)
    return [row for row in reader if any(str(v).strip() for v in row.values())]


def replace_appended_rows(path: str, baseline_size: int, rows: list[dict]) -> None:
    fd, tmp_path = tempfile.mkstemp(prefix="collect_tail_", suffix=".csv", dir=os.path.dirname(path) or None)
    os.close(fd)
    try:
        if baseline_size > 0 and os.path.isfile(path):
            with open(path, "rb") as src, open(tmp_path, "wb") as dst:
                dst.write(src.read(baseline_size))
            mode = "a"
            write_header = False
        else:
            mode = "w"
            write_header = True

        with open(tmp_path, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            for row in rows:
                writer.writerow(row)

        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def append_rows(path: str, rows: list[dict]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    file_exists = os.path.isfile(path) and os.path.getsize(path) > 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def load_source_rows_for_dates(data_dir: str, snapshot_dates: set[str]) -> list[dict]:
    rows = []
    if not snapshot_dates:
        return rows

    for fname in sorted(os.listdir(data_dir)):
        if not fname.endswith(".csv") or fname.startswith("_"):
            continue
        path = os.path.join(data_dir, fname)
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("snapshot_date", "") in snapshot_dates:
                    row["_source_file"] = fname.replace(".csv", "")
                    rows.append(row)
    return rows


def first_snapshot_date(path: str) -> str:
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return ""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            return row.get("snapshot_date", "")
    return ""


def rewrite_csv_excluding_dates_and_cutoff(path: str, cutoff: str, snapshot_dates: set[str]) -> int:
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return 0

    fd, tmp_path = tempfile.mkstemp(prefix="collect_replace_", suffix=".csv", dir=os.path.dirname(path) or None)
    os.close(fd)
    removed = 0
    try:
        with open(path, newline="", encoding="utf-8") as src, open(tmp_path, "w", newline="", encoding="utf-8") as dst:
            reader = csv.DictReader(src)
            writer = csv.DictWriter(dst, fieldnames=COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for row in reader:
                snapshot_date = row.get("snapshot_date", "")
                if snapshot_date < cutoff or snapshot_date in snapshot_dates:
                    removed += 1
                    continue
                writer.writerow(row)

        os.replace(tmp_path, path)
        return removed
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
