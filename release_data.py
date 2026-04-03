#!/usr/bin/env python3
"""
Safe helpers for working with release data archives.

Usage:
    python release_data.py extract /tmp/data.tar.gz data
"""

import argparse
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Safe release archive helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser("extract", help="Safely extract a tar.gz archive")
    extract_parser.add_argument("archive_path", help="Path to the input .tar.gz archive")
    extract_parser.add_argument("dest_dir", help="Directory to extract into")

    args = parser.parse_args()

    if args.command == "extract":
        safe_extract_tar_gz(args.archive_path, args.dest_dir)


if __name__ == "__main__":
    main()
