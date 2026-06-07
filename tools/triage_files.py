#!/usr/bin/env python3
"""
Move files that can't be uploaded to Google Photos into dedicated folders.

Rules (applied in order):
  1. Unsupported format (.flv .f4v .swf)  → _UNSUPPORTED/<folder>/
  2. Empty file (0 bytes)                 → _TOOSMALL/<folder>/
  3. Too large (> 10 GB)                  → _TOOLARGE/<folder>/

Usage:
    python3 tools/triage_files.py --path /path/to/photos [--dry-run]
"""

import argparse
import mimetypes
import shutil
import sys
from pathlib import Path

GPHOTOS_UNSUPPORTED_EXTS = {'.flv', '.f4v', '.swf'}
MAX_SIZE_BYTES = 10 * 1024 ** 3  # 10 GB

SUPPORTED_MEDIA_EXTS = {
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp',
    '.heic', '.heif', '.raw', '.cr2', '.nef', '.rw2', '.orf', '.dng', '.tif', '.tiff',
    '.mp4', '.mov', '.avi', '.mkv', '.wmv', '.webm',
    '.m4v', '.3gp', '.3g2', '.mts', '.m2ts', '.wm',
    '.flv', '.f4v', '.swf',  # included so they get caught as unsupported
}

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--path', required=True, help='Root photos folder to scan')
parser.add_argument('--dry-run', action='store_true', help='Show what would happen without moving anything')
parser.add_argument('--verbose', action='store_true', help='Print every file scanned (for debugging)')
args = parser.parse_args()

ROOT = Path(args.path)
DRY_RUN = args.dry_run
VERBOSE = args.verbose

if not ROOT.is_dir():
    print(f'[ERROR] Not a directory: {ROOT}', file=sys.stderr)
    sys.exit(1)


def is_media(file: Path) -> bool:
    ext = file.suffix.lower()
    if ext in SUPPORTED_MEDIA_EXTS:
        return True
    mime, _ = mimetypes.guess_type(str(file))
    return bool(mime and (mime.startswith('image/') or mime.startswith('video/')))


def fmt_size(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f'{size:.1f} {unit}'
        size /= 1024
    return f'{size:.1f} TB'


def has_non_empty_duplicate(file: Path) -> bool:
    """Return True if a numbered variant (e.g. 'DSCN2927 2.jpg') exists with size > 0."""
    stem = file.stem   # e.g. 'DSCN2927'
    ext = file.suffix  # e.g. '.jpg'
    for n in range(2, 20):
        candidate = file.parent / f'{stem} {n}{ext}'
        if candidate.exists() and candidate.stat().st_size > 0:
            return True
    return False


def move(file: Path, target_dir_name: str, tag: str, reason: str):
    dest_folder = ROOT / target_dir_name / file.parent.name
    dest = dest_folder / file.name
    size = file.stat().st_size
    print(f'  [{tag}] {reason}')
    print(f'    FROM  {file}')
    print(f'    TO    {dest}')
    print(f'    SIZE  {fmt_size(size)}')
    if DRY_RUN:
        return
    dest_folder.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(file), str(dest))
    except Exception as e:
        print(f'  [ERROR] Could not move {file.name}: {e}', file=sys.stderr)


if DRY_RUN:
    print('[DRY-RUN] No files will be moved.\n')

folders = sorted(f for f in ROOT.iterdir() if f.is_dir() and f.name not in {'_UNSUPPORTED', '_TOOSMALL', '_TOOLARGE'})
total = {'unsupported': 0, 'empty': 0, 'deleted': 0, 'toolarge': 0, 'skipped': 0}

for folder in folders:
    files = sorted(f for f in folder.iterdir() if f.is_file() and not f.name.startswith('.'))
    if not files:
        continue

    folder_total = {'unsupported': 0, 'empty': 0, 'toolarge': 0}

    for file in files:
        ext = file.suffix.lower()

        if ext in GPHOTOS_UNSUPPORTED_EXTS:
            if not folder_total['unsupported']:
                print(f'\n[FOLDER] {folder.name}')
            move(file, '_UNSUPPORTED', 'UNSUPPORTED', f'unsupported format ({ext})')
            folder_total['unsupported'] += 1
            total['unsupported'] += 1
            continue

        if not is_media(file):
            if not any(folder_total.values()):
                print(f'\n[FOLDER] {folder.name}')
            print(f'  [SKIP] {file.name}  ext={ext or "(none)"}')
            total['skipped'] += 1
            continue

        if VERBOSE:
            print(f'  [SCAN] {file.name}')

        try:
            size = file.stat().st_size
        except OSError as e:
            print(f'\n[FOLDER] {folder.name}')
            print(f'  [WARN] Cannot stat {file.name}: {e}')
            continue

        if size == 0:
            if not any(folder_total.values()):
                print(f'\n[FOLDER] {folder.name}')
            if has_non_empty_duplicate(file):
                print(f'  [DELETE] duplicate ghost (0 bytes, numbered copy exists)')
                print(f'    FILE  {file}')
                if not DRY_RUN:
                    file.unlink()
                total['deleted'] += 1
            else:
                move(file, '_TOOSMALL', 'EMPTY', 'empty file (0 bytes)')
                folder_total['empty'] += 1
                total['empty'] += 1
            continue

        if size > MAX_SIZE_BYTES:
            if not any(folder_total.values()):
                print(f'\n[FOLDER] {folder.name}')
            move(file, '_TOOLARGE', 'TOOLARGE', f'too large ({fmt_size(size)} > 10 GB)')
            folder_total['toolarge'] += 1
            total['toolarge'] += 1
            continue

print(f'\n[DONE] unsupported={total["unsupported"]}  empty={total["empty"]}  deleted={total["deleted"]}  toolarge={total["toolarge"]}  skipped(non-media)={total["skipped"]}')
