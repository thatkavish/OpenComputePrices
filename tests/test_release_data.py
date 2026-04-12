import csv
import gzip
import io
import os
import tarfile
import tempfile
import unittest

from release_data import merge_csv_gz_archives, safe_extract_tar_gz, write_monthly_archives


class ReleaseDataTests(unittest.TestCase):
    def _write_csv(self, path, rows):
        fieldnames = ["snapshot_date", "source", "provider", "price_per_hour"]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def _write_gzip_csv(self, path, rows):
        fieldnames = ["snapshot_date", "source", "provider", "price_per_hour"]
        with gzip.open(path, "wt", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def _read_gzip_csv(self, path):
        with gzip.open(path, "rt", newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    def test_safe_extract_tar_gz_extracts_regular_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            archive_path = os.path.join(temp_dir, "data.tar.gz")
            output_dir = os.path.join(temp_dir, "out")

            with tarfile.open(archive_path, "w:gz") as tar:
                payload = b"hello"
                info = tarfile.TarInfo("nested/file.txt")
                info.size = len(payload)
                tar.addfile(info, io.BytesIO(payload))

            safe_extract_tar_gz(archive_path, output_dir)

            with open(os.path.join(output_dir, "nested", "file.txt"), "rb") as f:
                self.assertEqual(f.read(), b"hello")

    def test_safe_extract_tar_gz_rejects_path_traversal(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            archive_path = os.path.join(temp_dir, "data.tar.gz")
            output_dir = os.path.join(temp_dir, "out")

            with tarfile.open(archive_path, "w:gz") as tar:
                payload = b"oops"
                info = tarfile.TarInfo("../escape.txt")
                info.size = len(payload)
                tar.addfile(info, io.BytesIO(payload))

            with self.assertRaises(ValueError):
                safe_extract_tar_gz(archive_path, output_dir)

    def test_write_monthly_archives_uses_snapshot_month(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            expired_path = os.path.join(temp_dir, "_expired.csv")
            output_dir = os.path.join(temp_dir, "archives")
            self._write_csv(expired_path, [
                {"snapshot_date": "2026-01-08", "source": "skypilot", "provider": "aws", "price_per_hour": "1"},
                {"snapshot_date": "2026-01-09", "source": "aws", "provider": "aws", "price_per_hour": "2"},
                {"snapshot_date": "2026-02-01", "source": "azure", "provider": "azure", "price_per_hour": "3"},
            ])

            archives = write_monthly_archives(expired_path, output_dir)

            self.assertEqual([path.name for path in archives], ["archive_2026-01.csv.gz", "archive_2026-02.csv.gz"])
            jan_rows = self._read_gzip_csv(os.path.join(output_dir, "archive_2026-01.csv.gz"))
            feb_rows = self._read_gzip_csv(os.path.join(output_dir, "archive_2026-02.csv.gz"))
            self.assertEqual([row["snapshot_date"] for row in jan_rows], ["2026-01-08", "2026-01-09"])
            self.assertEqual([row["snapshot_date"] for row in feb_rows], ["2026-02-01"])

    def test_merge_csv_gz_archives_preserves_existing_rows_and_dedupes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            existing_path = os.path.join(temp_dir, "existing.csv.gz")
            new_path = os.path.join(temp_dir, "archive_2026-01.csv.gz")
            self._write_gzip_csv(existing_path, [
                {"snapshot_date": "2026-01-08", "source": "skypilot", "provider": "aws", "price_per_hour": "1"},
                {"snapshot_date": "2026-01-09", "source": "aws", "provider": "aws", "price_per_hour": "2"},
            ])
            self._write_gzip_csv(new_path, [
                {"snapshot_date": "2026-01-09", "source": "aws", "provider": "aws", "price_per_hour": "2"},
                {"snapshot_date": "2026-01-10", "source": "azure", "provider": "azure", "price_per_hour": "3"},
            ])

            row_count = merge_csv_gz_archives(existing_path, new_path, new_path)

            rows = self._read_gzip_csv(new_path)
            self.assertEqual(row_count, 3)
            self.assertEqual([row["snapshot_date"] for row in rows], ["2026-01-08", "2026-01-09", "2026-01-10"])
