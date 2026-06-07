#!/usr/bin/env python3
"""
Restore 0-byte files from a backup drive by searching filename across the entire backup.

For each file in _TOOSMALL/<folder>/<file>:
  1. Search recursively in BACKUP for a file with the same name and size > 0
  2. If found → copy it to DEST/<folder>/<file> (original LPM location)

The backup folder structure does NOT need to match — search is by filename only.

Usage:
    python3 tools/restore_from_backup.py \
        --toosmall "/path/to/_TOOSMALL" \
        --backup   "/Volumes/ARCHIVE_01_BKP/_PHOTOS-VIDEOS/LPM" \
        --dest     "/path/to/LPM" \
        [--dry-run]
"""

import argparse
import shutil
import sys
from collections import defaultdict
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--toosmall', required=True, help='Path to _TOOSMALL folder')
parser.add_argument('--backup',   required=True, help='Backup HD root to search recursively')
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

def fmt(n):
    for u in ['B', 'KB', 'MB', 'GB']:
        if n < 1024: return f'{n:.1f} {u}'
        n /= 1024
    return f'{n:.1f} TB'


# Build index: filename.lower() → [list of paths with size > 0]
print('[INDEX] Building backup index (this may take a while)...')
backup_index: dict[str, list[Path]] = defaultdict(list)
indexed = 0
for f in BACKUP.rglob('*'):
    if f.is_file() and not f.name.startswith('.'):
        try:
            if f.stat().st_size > 0:
                backup_index[f.name.lower()].append(f)
                indexed += 1
        except OSError:
            pass
print(f'[INDEX] {indexed} files indexed from backup.\n')

if DRY_RUN:
    print('[DRY-RUN] No files will be copied.\n')

total_found = total_restored = total_missing = total_ambiguous = 0

for folder in sorted(TOOSMALL.iterdir()):
    if not folder.is_dir():
        continue
    for file in sorted(folder.iterdir()):
        if not file.is_file() or file.name.startswith('.'):
            continue

        total_found += 1
        candidates = backup_index.get(file.name.lower(), [])

        if not candidates:
            total_missing += 1
            print(f'  [NOT FOUND] {folder.name}/{file.name}')
            continue

        if len(candidates) > 1:
            total_ambiguous += 1
            print(f'  [AMBIGUOUS] {folder.name}/{file.name} — {len(candidates)} matches in backup:')
            for c in candidates:
                print(f'    {c}  ({fmt(c.stat().st_size)})')
            continue

        backup_file = candidates[0]
        backup_size = backup_file.stat().st_size
        dest_folder = DEST / folder.name
        dest_file   = dest_folder / file.name

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

print(f'\n[DONE] scanned={total_found}  restored={total_restored}  not_found={total_missing}  ambiguous={total_ambiguous}')
