import os
import shutil
from datetime import datetime, timezone, timedelta

# WIB (Western Indonesia Time) = UTC+7
_LOCAL_TZ = timezone(timedelta(hours=7))

def rotate_backups(target_path, backup_dir, prefix="staging", extension=".db", max_versions=3):
    """
    Rotates backups: current -> v1, v1 -> v2, v2 -> v3.
    """
    if not os.path.exists(target_path):
        return

    os.makedirs(backup_dir, exist_ok=True)

    # v(N-1) -> vN
    for i in range(max_versions, 1, -1):
        prev_v = os.path.join(backup_dir, f"{prefix}_v{i-1}{extension}")
        curr_v = os.path.join(backup_dir, f"{prefix}_v{i}{extension}")
        if os.path.exists(prev_v):
            if os.path.exists(curr_v):
                os.remove(curr_v)
            os.rename(prev_v, curr_v)

    # current -> v1
    v1 = os.path.join(backup_dir, f"{prefix}_v1{extension}")
    shutil.copy2(target_path, v1)

def get_available_backups(backup_dir, prefix="staging", extension=".db", max_versions=3):
    """
    Returns a list of available backup files with their last modified timestamps.
    """
    backups = []
    if not os.path.exists(backup_dir):
        return backups

    for i in range(1, max_versions + 1):
        path = os.path.join(backup_dir, f"{prefix}_v{i}{extension}")
        if os.path.exists(path):
            mtime = os.path.getmtime(path)
            dt = datetime.fromtimestamp(mtime, tz=_LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
            backups.append({"version": i, "path": path, "timestamp": dt})
    return backups

def restore_backup(backup_path, target_path):
    """
    Restores a backup to the target path.
    """
    if os.path.exists(backup_path):
        shutil.copy2(backup_path, target_path)
        return True
    return False
