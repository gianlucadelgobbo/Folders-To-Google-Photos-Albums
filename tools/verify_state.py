#!/usr/bin/env python3
"""
Check every local file against Google Photos and repair discrepancies.

Usage (run from project root):
    python3 tools/verify_state.py --path /path/to/photos [--dry-run]

For each local file in each subfolder of --path:
  1. Check if the file exists anywhere in Google Photos (via galbum_cache.json).
     Note: the Google Photos API does not support filename search, so the cache
     is the only way to do a global "does this file exist" lookup.
  2a. File IS in Google Photos → skip.
  2b. File is NOT in Google Photos:
        → unsupported format (.flv / .f4v / .swf)  → move to _UNSUPPORTED
        → empty file (0 bytes)                      → move to _TOOSMALL
        → otherwise                                 → upload and add to album
        → upload failure                            → track in failed_uploads.json
"""

import argparse
import asyncio
import hashlib
import json
import mimetypes
import os
import shutil
import sys
import tempfile
import unicodedata
import urllib.parse
import warnings
from pathlib import Path
from typing import Dict, Optional, Set

warnings.filterwarnings('ignore', category=Warning)

from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from tenacity import retry, stop_after_attempt, wait_exponential

# === CONFIG ===
SCOPES = [
    'https://www.googleapis.com/auth/photoslibrary.appendonly',
    'https://www.googleapis.com/auth/photoslibrary.readonly',
]
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'
STATE_FILE = 'upload_state.json'
FAILED_FILE = 'failed_uploads.json'
ALBUM_CACHE_FILE = 'galbum_cache.json'
GPHOTOS_UNSUPPORTED_EXTS = {'.flv', '.f4v', '.swf'}
TOOSMALL_DIR_NAME = '_TOOSMALL'
UNSUPPORTED_DIR_NAME = '_UNSUPPORTED'

# === CLI ===
parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--path', type=str, required=True, help='Root photos folder to check')
parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making any changes')
args = parser.parse_args()

PHOTO_ROOT_DIR = args.path
DRY_RUN = args.dry_run

# === LOGGING ===
def log(msg): print(msg, flush=True)
def err(msg): print(msg, file=sys.stderr, flush=True)

# === JSON ===
def load_json(path, default):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return default

def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def format_size(size):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f'{size:.2f}{unit}'
        size /= 1024
    return f'{size:.2f}TB'

# === MEDIA TYPE ===
def is_supported_media(file_path: Path) -> bool:
    ext = file_path.suffix.lower()
    if ext in GPHOTOS_UNSUPPORTED_EXTS:
        return False
    mime_type, _ = mimetypes.guess_type(str(file_path))
    if mime_type:
        return mime_type.startswith('image/') or mime_type.startswith('video/')
    supported_exts = {
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp',
        '.heic', '.heif', '.raw', '.cr2', '.nef', '.rw2',
        '.mp4', '.mov', '.avi', '.mkv', '.wmv', '.webm',
        '.m4v', '.3gp', '.3g2', '.mts', '.m2ts', '.wm',
    }
    return ext in supported_exts

# === MOVE HELPERS ===
def _move_to_dir(file: Path, folder_name: str, target: str, tag: str):
    dest_folder = Path(PHOTO_ROOT_DIR).parent / target / folder_name
    dest_folder.mkdir(parents=True, exist_ok=True)
    dest = dest_folder / file.name
    try:
        shutil.move(str(file), str(dest))
        log(f'  [{tag}] Moved → {dest}')
    except Exception as e:
        err(f'  [{tag}] Failed to move {file.name}: {e}')

def move_to_toosmall(file: Path, folder_name: str):
    _move_to_dir(file, folder_name, TOOSMALL_DIR_NAME, 'TOOSMALL')

def move_to_unsupported(file: Path, folder_name: str):
    _move_to_dir(file, folder_name, UNSUPPORTED_DIR_NAME, 'UNSUPPORTED')

# === FAILURE TRACKING ===
failures = load_json(FAILED_FILE, {
    'UploadError': {}, 'AddToAlbumError': {}, 'TooLarge': {},
    'ExifErrors': {}, 'UnsupportedFormat': {},
})

def add_failure(error_type: str, folder_name: str, file_name: str, folder_path: Path, album_id: str = ''):
    if error_type not in failures:
        failures[error_type] = {}
    if folder_name not in failures[error_type]:
        failures[error_type][folder_name] = {'path': str(folder_path.resolve()), 'files': []}
        if album_id:
            failures[error_type][folder_name]['album_id'] = album_id
    if file_name not in failures[error_type][folder_name]['files']:
        failures[error_type][folder_name]['files'].append(file_name)
    if not DRY_RUN:
        save_json(FAILED_FILE, failures)

# === GLOBAL FILENAME INDEX (built from API at startup) ===
def build_global_index():
    """Fetch all albums and their media items from the API.

    Returns:
        global_filenames: set of filename.lower() for every file in Google Photos
        album_map: dict title → album_id
    The Google Photos API has no filename search endpoint, so we enumerate
    all albums and all their contents to build the index.
    """
    log('[INDEX] Fetching all albums from Google Photos API...')

    # Step 1: fetch all albums → build both id→title and title→id maps
    id_to_title: Dict[str, str] = {}
    album_map: Dict[str, str] = {}  # title → album_id
    page_token = None
    while True:
        params = {'pageSize': 50}
        if page_token:
            params['pageToken'] = page_token
        r = session.get('https://photoslibrary.googleapis.com/v1/albums', params=params, timeout=(10, 30))
        if r.status_code != 200:
            err(f'[INDEX] Error fetching albums: {r.status_code} {r.text[:200]}')
            break
        data = r.json()
        for album in data.get('albums', []):
            aid = album.get('id', '')
            title = album.get('title', '')
            if aid and title:
                id_to_title[aid] = title
                album_map[title] = aid
        page_token = data.get('nextPageToken')
        if not page_token:
            break
    log(f'[INDEX] {len(album_map)} album(s) found')

    # Step 2: for each album, fetch all media items
    global_filenames: Set[str] = set()
    for i, (album_id, title) in enumerate(id_to_title.items(), 1):
        log(f'[INDEX] [{i}/{len(id_to_title)}] Listing "{title}"...')
        page_token = None
        album_count = 0
        while True:
            body: dict = {'albumId': album_id, 'pageSize': 100}
            if page_token:
                body['pageToken'] = page_token
            r = session.post(
                'https://photoslibrary.googleapis.com/v1/mediaItems:search',
                json=body, timeout=(10, 60),
            )
            if r.status_code != 200:
                err(f'  [INDEX] Error listing "{title}": {r.status_code} {r.text[:200]}')
                break
            data = r.json()
            for item in data.get('mediaItems', []):
                fn = item.get('filename', '')
                if fn:
                    global_filenames.add(fn.lower())
                    album_count += 1
            page_token = data.get('nextPageToken')
            if not page_token:
                break
        log(f'  → {album_count} item(s)')

    log(f'[INDEX] Total: {len(global_filenames)} unique filename(s) in Google Photos')
    return global_filenames, album_map

# === AUTH ===
def _scopes_ok(creds, required_scopes):
    granted = set(creds.scopes or [])
    return set(required_scopes).issubset(granted)

def authenticate() -> AuthorizedSession:
    creds = None
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            if creds and creds.scopes and not _scopes_ok(creds, SCOPES):
                log('[AUTH] Token scopes insufficient, forcing re-auth')
                creds = None
        except Exception:
            creds = None
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            if creds and creds.scopes and not _scopes_ok(creds, SCOPES):
                log('[AUTH] Token scopes insufficient after refresh, forcing re-auth')
                creds = None
        except Exception:
            creds = None
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
        creds = flow.run_local_server(port=0, prompt='consent')
        with open(TOKEN_FILE, 'w') as f:
            f.write(creds.to_json())
    log(f'[AUTH] Granted scopes: {creds.scopes}')
    return AuthorizedSession(creds)

# === CLOUD STAGING ===
def stage_local_copy_if_cloud(path: Path) -> Path:
    if '/Library/CloudStorage/' not in str(path):
        return path
    path_sig = hashlib.sha1(str(path).encode()).hexdigest()[:8]
    dst = Path(tempfile.gettempdir()) / 'gphotos_stage' / f'{path_sig}_{path.name}'
    dst.parent.mkdir(parents=True, exist_ok=True)
    st = os.stat(path)
    with open(path, 'rb') as src, open(dst, 'wb') as out:
        shutil.copyfileobj(src, out, length=1024 * 1024)
    os.utime(dst, (st.st_atime, st.st_mtime))
    return dst

def cleanup_staged(staged: Path, original: Path):
    if staged != original:
        try:
            staged.unlink()
        except Exception:
            pass

# === SESSION (set in main) ===
session: Optional[AuthorizedSession] = None

# === GOOGLE PHOTOS API ===
_album_map: Dict[str, str] = {}  # title → album_id, populated at startup by build_global_index

def get_or_create_album(folder_name: str) -> str:
    if folder_name in _album_map:
        return _album_map[folder_name]
    r = session.post(
        'https://photoslibrary.googleapis.com/v1/albums',
        json={'album': {'title': folder_name[:100]}},
        timeout=(10, 30),
    )
    r.raise_for_status()
    aid = r.json()['id']
    _album_map[folder_name] = aid
    log(f'  [ALBUM] Created: {folder_name} (id: {aid})')
    return aid


@retry(wait=wait_exponential(multiplier=2, min=5, max=300), stop=stop_after_attempt(7))
async def upload_file(file_path: str) -> str:
    file_name = Path(file_path).name
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        raise Exception(f'File is 0 bytes: {file_name}')
    headers = {
        'Content-Type': 'application/octet-stream',
        'X-Goog-Upload-File-Name': urllib.parse.quote(unicodedata.normalize('NFC', file_name)),
        'X-Goog-Upload-Protocol': 'raw',
    }
    read_timeout = max(1200, (file_size / (1024 ** 3)) * 60 + 300)
    with open(file_path, 'rb') as f:
        r = session.post(
            'https://photoslibrary.googleapis.com/v1/uploads',
            data=f, headers=headers, timeout=(30, read_timeout),
        )
    if r.status_code != 200:
        raise Exception(f'Upload failed: {r.status_code} {r.text[:200]}')
    token = r.text.strip()
    if not token:
        raise Exception('Empty upload token')
    return token


async def add_to_album(upload_token: str, album_id: str, file_name: str):
    body = {
        'albumId': album_id,
        'newMediaItems': [{'description': file_name, 'simpleMediaItem': {'uploadToken': upload_token}}],
    }
    r = session.post(
        'https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate',
        json=body, timeout=(10, 30),
    )
    if r.status_code != 200:
        raise Exception(f'batchCreate failed: {r.status_code} {r.text[:200]}')
    for item in r.json().get('newMediaItemResults', []):
        status = item.get('status', {})
        if status.get('code', -1) == 0 or status.get('message') == 'Success':
            return
        raise Exception(f"batchCreate error: {status.get('message', 'unknown')}")


# === FOLDER PROCESSING ===

async def process_folder(folder_path: Path, global_filenames: Set[str], state: dict):
    folder_name = folder_path.name
    log(f'\n{"=" * 80}')
    log(f'[FOLDER] {folder_name}')

    local_files = [
        f for f in folder_path.iterdir()
        if f.is_file() and not f.name.startswith('.')
    ]
    if not local_files:
        log('  (no files)')
        return

    album_id: Optional[str] = None  # resolved lazily, only when needed for upload

    for file in sorted(local_files):
        # ── already in Google Photos ──────────────────────────────────────
        if file.name.lower() in global_filenames:
            continue

        log(f'\n  [MISSING] {file.name}')

        # ── unsupported format ────────────────────────────────────────────
        if not is_supported_media(file):
            log(f'    → Unsupported format')
            if DRY_RUN:
                log(f'    [DRY-RUN] Would move to _UNSUPPORTED')
            else:
                move_to_unsupported(file, folder_name)
            continue

        # ── empty file ────────────────────────────────────────────────────
        if file.stat().st_size == 0:
            log(f'    → Empty file (0 bytes)')
            if DRY_RUN:
                log(f'    [DRY-RUN] Would move to _TOOSMALL')
            else:
                move_to_toosmall(file, folder_name)
            continue

        # ── upload ────────────────────────────────────────────────────────
        file_size = file.stat().st_size
        log(f'    → Not in Google Photos — uploading ({format_size(file_size)})')
        if DRY_RUN:
            log(f'    [DRY-RUN] Would upload to album "{folder_name}"')
            continue

        if album_id is None:
            try:
                album_id = get_or_create_album(folder_name)
            except Exception as e:
                err(f'  [ALBUM] Failed to create album: {e}')
                add_failure('UploadError', folder_name, file.name, folder_path)
                continue

        staged = stage_local_copy_if_cloud(file)
        try:
            token = await upload_file(str(staged))
            await add_to_album(token, album_id, file.name)
            if folder_name not in state:
                state[folder_name] = {'album_id': album_id, 'path': str(folder_path.resolve()), 'files': []}
            state[folder_name].setdefault('files', [])
            if file.name not in state[folder_name]['files']:
                state[folder_name]['files'].append(file.name)
            state[folder_name]['album_id'] = album_id
            save_json(STATE_FILE, state)
            log(f'    ✅ Uploaded and added to album')
        except Exception as e:
            err(f'    ❌ Upload failed: {e}')
            add_failure('UploadError', folder_name, file.name, folder_path, album_id)
        finally:
            cleanup_staged(staged, file)


# === MAIN ===

async def main():
    global session, _album_map
    log('[INIT] Authenticating with Google Photos...')
    session = authenticate()
    log('[INIT] Authenticated')
    if DRY_RUN:
        log('[INIT] DRY-RUN mode — no changes will be made')

    root = Path(PHOTO_ROOT_DIR)
    if not root.is_dir():
        err(f'[ERROR] Path not found: {PHOTO_ROOT_DIR}')
        sys.exit(1)

    # Build global index from API: fetches all albums + their contents
    global_filenames, _album_map = build_global_index()

    state = load_json(STATE_FILE, {})
    log(f'[INIT] Loaded upload_state.json ({len(state)} entries)')

    folders = sorted(f for f in root.iterdir() if f.is_dir())
    log(f'[INIT] {len(folders)} folder(s) to process under {root}')

    for folder in folders:
        await process_folder(folder, global_filenames, state)

    save_json(FAILED_FILE, failures)
    log('\n[DONE] Verification complete.')


if __name__ == '__main__':
    asyncio.run(main())
