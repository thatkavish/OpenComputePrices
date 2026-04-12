#!/usr/bin/env python3
"""
Safe helpers for working with release data archives.

Usage:
    python release_data.py extract /tmp/data.tar.gz data
"""

import argparse
import csv
import gzip
import os
import shutil
import tarfile
from pathlib import Path


def _is_within_directory(target_dir: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(target_dir.resolve())
        return True
    except ValueError:
        return False


def safe_extract_tar_gz(archive_path: str, dest_dir: str) -> None:
    """Extract regular files from a tar.gz archive into dest_dir safely."""
    target_dir = Path(dest_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    with tarfile.open(archive_path, "r:gz") as tar:
        members = tar.getmembers()
        for member in members:
            member_path = target_dir / member.name
            if not _is_within_directory(target_dir, member_path):
                raise ValueError(f"Archive member escapes target directory: {member.name}")
            if member.issym() or member.islnk():
                raise ValueError(f"Archive member uses links, which are not allowed: {member.name}")
            if member.isdev():
                raise ValueError(f"Archive member is a device file, which is not allowed: {member.name}")

        for member in members:
            member_path = target_dir / member.name
            if member.isdir():
                member_path.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile():
                continue

            member_path.parent.mkdir(parents=True, exist_ok=True)
            extracted = tar.extractfile(member)
            if extracted is None:
                raise ValueError(f"Could not extract archive member: {member.name}")
            with extracted, open(member_path, "wb") as output:
                shutil.copyfileobj(extracted, output)


def _snapshot_month(row: dict) -> str:
    snapshot_date = row.get("snapshot_date", "")
    month = snapshot_date[:7]
    if len(month) == 7 and month[4] == "-" and month[:4].isdigit() and month[5:].isdigit():
        return month
    return "unknown"


def write_monthly_archives(expired_csv_path: str, output_dir: str) -> list[Path]:
    """Split expired rows into gzip archives named by each row's snapshot month."""
    source_path = Path(expired_csv_path)
    if not source_path.is_file() or source_path.stat().st_size == 0:
        return []

    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    handles = {}
    writers = {}
    paths = {}
    try:
        with open(source_path, newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            fieldnames = reader.fieldnames
            if not fieldnames:
                return []

            for row in reader:
                month = _snapshot_month(row)
                if month not in writers:
                    archive_path = target_dir / f"archive_{month}.csv.gz"
                    handle = gzip.open(archive_path, "wt", newline="", encoding="utf-8")
                    writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
                    writer.writeheader()
                    handles[month] = handle
                    writers[month] = writer
                    paths[month] = archive_path
                writers[month].writerow(row)
    finally:
        for handle in handles.values():
            handle.close()

    return [paths[month] for month in sorted(paths)]


def _gzip_csv_fieldnames(path: Path) -> list[str]:
    if not path.is_file() or path.stat().st_size == 0:
        return []
    with gzip.open(path, "rt", newline="", encoding="utf-8") as source:
        return csv.DictReader(source).fieldnames or []


def merge_csv_gz_archives(existing_archive_path: str, new_archive_path: str, output_archive_path: str) -> int:
    """Merge gzip CSV archives, preserving existing rows and removing exact duplicates."""
    existing_path = Path(existing_archive_path)
    new_path = Path(new_archive_path)
    output_path = Path(output_archive_path)

    fieldnames = _gzip_csv_fieldnames(new_path) or _gzip_csv_fieldnames(existing_path)
    if not fieldnames:
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(f"{output_path.name}.tmp")
    seen = set()
    count = 0

    def write_rows(writer: csv.DictWriter, path: Path) -> None:
        nonlocal count
        if not path.is_file() or path.stat().st_size == 0:
            return
        with gzip.open(path, "rt", newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            for row in reader:
                key = tuple(row.get(field, "") for field in fieldnames)
                if key in seen:
                    continue
                seen.add(key)
                writer.writerow(row)
                count += 1

    try:
        with gzip.open(tmp_path, "wt", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            write_rows(writer, existing_path)
            write_rows(writer, new_path)
        os.replace(tmp_path, output_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Safe release archive helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser("extract", help="Safely extract a tar.gz archive")
    extract_parser.add_argument("archive_path", help="Path to the input .tar.gz archive")
    extract_parser.add_argument("dest_dir", help="Directory to extract into")

    split_parser = subparsers.add_parser("split-expired", help="Split expired CSV rows into monthly archives")
    split_parser.add_argument("expired_csv_path", help="Path to data/_expired.csv")
    split_parser.add_argument("output_dir", help="Directory to write archive_YYYY-MM.csv.gz files")

    merge_parser = subparsers.add_parser("merge-archive", help="Merge two gzip CSV archives")
    merge_parser.add_argument("existing_archive_path", help="Path to the existing archive, if present")
    merge_parser.add_argument("new_archive_path", help="Path to the newly generated archive")
    merge_parser.add_argument("output_archive_path", help="Path to write the merged archive")

    args = parser.parse_args()

    if args.command == "extract":
        safe_extract_tar_gz(args.archive_path, args.dest_dir)
    elif args.command == "split-expired":
        for archive_path in write_monthly_archives(args.expired_csv_path, args.output_dir):
            print(archive_path)
    elif args.command == "merge-archive":
        row_count = merge_csv_gz_archives(
            args.existing_archive_path,
            args.new_archive_path,
            args.output_archive_path,
        )
        print(row_count)


if __name__ == "__main__":
    main()
