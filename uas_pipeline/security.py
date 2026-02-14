"""Security utilities (path validation, permissions)."""
from __future__ import annotations

import os
import platform
import stat
from pathlib import Path
import logging

class SecurityError(Exception):
    """Raised when security validation fails."""
    pass

ALLOWED_BASE_DIRS = [
    str(Path.home()),
    os.getcwd(),
    str(Path.home() / "FAA_UAS_Sightings"),
    str(Path.home() / "Documents" / "FAA_UAS_Sightings"),
    str(Path("C:/Documents/FAA_UAS_Sightings")),
]

logger = logging.getLogger(__name__)

def validate_folder_path(folder_path: Path) -> None:
    resolved_path = Path(folder_path).resolve()
    for allowed_base in ALLOWED_BASE_DIRS:
        allowed_resolved = Path(allowed_base).resolve()
        try:
            resolved_path.relative_to(allowed_resolved)
            logger.info("Path validation passed: %s", folder_path)
            return
        except ValueError:
            continue
    raise SecurityError(
        f"Folder path '{folder_path}' is outside allowed directories: {ALLOWED_BASE_DIRS}"
    )


def secure_cache_permissions(file_path: Path) -> None:
    if not file_path.exists():
        return

    system = platform.system()
    try:
        if system == "Windows":
            try:
                import win32security
                import ntsecuritycon as con
                import win32api

                user_sid = win32security.LookupAccountName("", win32api.GetUserName())[0]
                dacl = win32security.ACL()
                dacl.AddAccessAllowedAce(win32security.ACL_REVISION, con.FILE_ALL_ACCESS, user_sid)
                sd = win32security.SECURITY_DESCRIPTOR()
                sd.SetSecurityDescriptorDacl(1, dacl, 0)
                win32security.SetFileSecurity(
                    str(file_path),
                    win32security.DACL_SECURITY_INFORMATION,
                    sd
                )
                logger.debug("Set secure permissions (Windows ACL) on %s", file_path)
            except ImportError:
                logger.warning(
                    "pywin32 not installed. Install with 'pip install pywin32' for proper Windows file permissions."
                )
                os.chmod(file_path, stat.S_IREAD | stat.S_IWRITE)
                logger.debug("Set basic permissions on %s (limited on Windows)", file_path)
        else:
            os.chmod(file_path, stat.S_IRUSR | stat.S_IWUSR)
            logger.debug("Set secure permissions (0600) on %s", file_path)
    except Exception as exc:
        logger.warning("Could not set secure permissions on %s: %s", file_path, exc)
