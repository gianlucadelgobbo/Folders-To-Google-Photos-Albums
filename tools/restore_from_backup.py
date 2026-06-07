#!/usr/bin/env python3
"""
Restore 0-byte files from a backup drive.

For each file in _TOOSMALL/<folder>/<file>:
  1. Look for it in BACKUP_ROOT/<folder>/<file>
  2. If found with size > 0 → copy it to LPM_ROOT/<folder>/<file>
     (restoring the original location so it can be uploaded)

Usage:
    python3 tools/restore_from_backup.py \
        --toosmall  "/path/to/_TOOSMALL" \
        --backup    "/Volumes/ARCHIVE_01_BKP/_PHOTOS-VIDEOS/LPM" \
        --dest      "/path/to/LPM" \
        [--dry-run]
"""

import argparse
import shutil
import sys
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--toosmall', required=True, help='Path to _TOOSMALL folder')
parser.add_argument('--backup',   required=True, help='Backup HD root (e.g. /Volumes/ARCHIVE_01_BKP/_PHOTOS-VIDEOS/LPM)')
parser.add_argument('--dest',     required=True, help='Destination LPM root to restore files into')
parser.add_argument('--dry-run',  action='store_true', help='Show what would be done without copying')
args = parser.parse_args()

TOOSMALL = Path(args.toosmall)
BACKUP   = Path(args.backup)
DEST     = Path(args.dest)
DRY_RUN  = args.dry_run

for p in (TOOSMALL, BACKUP, DEST):
    if not p.is_dir():
        print(f'[ERROR] Not a directory: {p}', file=sys.stderr)
        sys.exit(1)

if DRY_RUN:
    print('[DRY-RUN] No files will be copied.\n')

total_found = total_missing = total_restored = 0

for folder in sorted(TOOSMALL.iterdir()):
    if not folder.is_dir():
        continue
    for file in sorted(folder.iterdir()):
        if not file.is_file() or file.name.startswith('.'):
            continue

        total_found += 1
        backup_file = BACKUP / folder.name / file.name

        if not backup_file.exists():
            total_missing += 1
            print(f'  [NOT FOUND] {folder.name}/{file.name}')
            continue

        backup_size = backup_file.stat().st_size
        if backup_size == 0:
            total_missing += 1
            print(f'  [EMPTY IN BACKUP] {folder.name}/{file.name}')
            continue

        dest_folder = DEST / folder.name
        dest_file   = dest_folder / file.name

        def fmt(n):
            for u in ['B','KB','MB','GB']:
                if n < 1024: return f'{n:.1f} {u}'
                n /= 1024
            return f'{n:.1f} TB'

        print(f'  [RESTORE] {folder.name}/{file.name}  ({fmt(backup_size)})')
        print(f'    FROM  {backup_file}')
        print(f'    TO    {dest_file}')

        if DRY_RUN:
            print(f'    → DRY-RUN: not copied')
        else:
            dest_folder.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copy2(str(backup_file), str(dest_file))
                print(f'    → COPIED OK')
                total_restored += 1
            except Exception as e:
                print(f'    → ERROR: {e}', file=sys.stderr)

print(f'\n[DONE] scanned={total_found}  restored={total_restored}  not_found={total_missing}')
