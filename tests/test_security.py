import unittest
from pathlib import Path

from uas_pipeline.security import SecurityError, validate_folder_path


class TestSecurity(unittest.TestCase):
    def test_validate_folder_path_allows_subdir_of_cwd(self):
        test_path = Path.cwd() / "test_data"
        test_path.mkdir(parents=True, exist_ok=True)
        try:
            validate_folder_path(test_path)
        finally:
            # best-effort cleanup
            if test_path.exists():
                try:
                    test_path.rmdir()
                except OSError:
                    pass

    def test_validate_folder_path_rejects_outside_allowed(self):
        # Use a path at the filesystem root to avoid being under home/cwd
        root_path = Path(Path.home().anchor) / "not_allowed_test_dir"
        with self.assertRaises(SecurityError):
            validate_folder_path(root_path)


if __name__ == "__main__":
    unittest.main()
