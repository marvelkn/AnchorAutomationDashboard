import os
import shutil
from datetime import datetime

def rotate_backups(db_path, backup_dir, max_versions=3):
    """
    Rotates backups: current -> v1, v1 -> v2, v2 -> v3.
    """
    if not os.path.exists(db_path):
        return

    os.makedirs(backup_dir, exist_ok=True)

    # v2 -> v3
    v2 = os.path.join(backup_dir, "staging_v2.db")
    v3 = os.path.join(backup_dir, "staging_v3.db")
    if os.path.exists(v2):
        if os.path.exists(v3):
            os.remove(v3)
        os.rename(v2, v3)

    # v1 -> v2
    v1 = os.path.join(backup_dir, "staging_v1.db")
    if os.path.exists(v1):
        os.rename(v1, v2)

    # current -> v1
    shutil.copy2(db_path, v1)

def get_available_backups(backup_dir):
    """
    Returns a list of available backup files with their last modified timestamps.
    """
    backups = []
    for i in range(1, 4):
        path = os.path.join(backup_dir, f"staging_v{i}.db")
        if os.path.exists(path):
            mtime = os.path.getmtime(path)
            dt = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
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
