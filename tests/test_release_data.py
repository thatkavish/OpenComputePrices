import io
import os
import tarfile
import tempfile
import unittest

from release_data import safe_extract_tar_gz


class ReleaseDataTests(unittest.TestCase):
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
