#!/usr/bin/env python3
"""
Compare local folders against upload_state.json and mark missing files for retry.

For each local media file in each subfolder of --path:
  - Already in upload_state.json  → OK, skip
  - Not in upload_state.json      → add to failed_uploads.json (UploadError)

Files skipped by design (handled by triage_files.py):
  - Unsupported format (.flv .f4v .swf)
  - Empty (0 bytes)
  - Too large (> 10 GB)
  - Non-media files

No API calls, no OAuth needed. Run triage_files.py first, then this.

Usage:
    python3 tools/verify_state.py --path /path/to/photos [--dry-run]
"""

import argparse
import json
import mimetypes
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
STATE_FILE   = PROJECT_ROOT / 'upload_state.json'
FAILED_FILE  = PROJECT_ROOT / 'failed_uploads.json'

GPHOTOS_UNSUPPORTED_EXTS = {'.flv', '.f4v', '.swf'}
MAX_SIZE_BYTES = 10 * 1024 ** 3  # 10 GB

SUPPORTED_MEDIA_EXTS = {
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp',
    '.heic', '.heif', '.raw', '.cr2', '.nef', '.rw2', '.orf', '.dng', '.tif', '.tiff',
    '.mp4', '.mov', '.avi', '.mkv', '.wmv', '.webm',
    '.m4v', '.3gp', '.3g2', '.mts', '.m2ts', '.wm',
}

TRIAGE_DIRS = {'_UNSUPPORTED', '_TOOSMALL', '_TOOLARGE', '_NONMEDIA'}

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--path', required=True, help='Root photos folder to scan')
parser.add_argument('--dry-run', action='store_true', help='Show what would be added without writing failed_uploads.json')
args = parser.parse_args()

ROOT    = Path(args.path)
DRY_RUN = args.dry_run

if not ROOT.is_dir():
    print(f'[ERROR] Not a directory: {ROOT}', file=sys.stderr)
    sys.exit(1)


def log(msg):   print(msg, flush=True)
def err(msg):   print(msg, file=sys.stderr, flush=True)


def load_json(path, default):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def is_uploadable(file: Path) -> bool:
    ext = file.suffix.lower()
    if ext in GPHOTOS_UNSUPPORTED_EXTS:
        return False
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


# Load state
state   = load_json(STATE_FILE, {})
failures = load_json(FAILED_FILE, {
    'UploadError': {}, 'AddToAlbumError': {}, 'TooLarge': {},
    'ExifErrors': {}, 'UnsupportedFormat': {},
})

if DRY_RUN:
    log('[DRY-RUN] failed_uploads.json will NOT be updated.\n')

folders = sorted(f for f in ROOT.iterdir() if f.is_dir() and f.name not in TRIAGE_DIRS)
log(f'[INIT] Scanning {len(folders)} folder(s) in {ROOT}')
log(f'[INIT] upload_state.json has {len(state)} folder(s)\n')

total_ok = 0
total_missing = 0
total_skipped = 0

for folder in folders:
    folder_name = folder.name
    state_info  = state.get(folder_name, {})
    album_id    = state_info.get('album_id', '')
    uploaded    = {f.lower() for f in state_info.get('files', [])}

    local_files = sorted(
        f for f in folder.iterdir()
        if f.is_file() and not f.name.startswith('.')
    )
    if not local_files:
        continue

    folder_missing = []

    for file in local_files:
        if not is_uploadable(file):
            total_skipped += 1
            continue

        try:
            size = file.stat().st_size
        except OSError:
            total_skipped += 1
            continue

        if size == 0 or size > MAX_SIZE_BYTES:
            total_skipped += 1
            continue

        if file.name.lower() in uploaded:
            total_ok += 1
            continue

        folder_missing.append(file)
        total_missing += 1

    if folder_missing:
        log(f'[FOLDER] {folder_name}  ({len(folder_missing)} missing)')
        for file in folder_missing:
            size = file.stat().st_size
            log(f'  [MISSING] {file.name}  ({fmt_size(size)})')

        if not DRY_RUN:
            if 'UploadError' not in failures:
                failures['UploadError'] = {}
            if folder_name not in failures['UploadError']:
                failures['UploadError'][folder_name] = {
                    'path': str(folder.resolve()),
                    'files': [],
                }
                if album_id:
                    failures['UploadError'][folder_name]['album_id'] = album_id
            existing = set(failures['UploadError'][folder_name]['files'])
            for file in folder_missing:
                if file.name not in existing:
                    failures['UploadError'][folder_name]['files'].append(file.name)

already_in_failed = sum(
    len(info.get('files', []))
    for info in failures.get('UploadError', {}).values()
)

if not DRY_RUN and total_missing > 0:
    save_json(FAILED_FILE, failures)
    log(f'\n[SAVED] failed_uploads.json updated')

if DRY_RUN:
    log(f'\n[DRY-RUN] Would add {total_missing} file(s) to failed_uploads.json (UploadError)')
    log(f'          Already in failed_uploads.json (UploadError): {already_in_failed}')
    log(f'          Total after update would be: {already_in_failed + total_missing}')

log(f'\n[DONE] ok={total_ok}  missing={total_missing}  skipped(triage/nonmedia)={total_skipped}')
