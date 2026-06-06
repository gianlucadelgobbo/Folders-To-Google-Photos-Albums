#!/usr/bin/env python3
"""
Verify upload_state.json against Google Photos and local files, then repair.

Usage (run from project root):
    python3 utilities/verify_state.py --path /path/to/photos [--dry-run]

For each file tracked in upload_state.json it checks:
  1. The file is present in the corresponding Google Photos album.
  2. If not in the album, whether the local file still exists.

Repair logic when a file is missing from the album:
  - Local file exists + unsupported format  → move to _UNSUPPORTED, remove from state
  - Local file exists + 0 bytes             → move to _TOOSMALL, remove from state
  - Local file exists + ok                  → re-upload and add to album
  - Album missing + local file ok           → create album, re-upload
  - Local file doesn't exist either         → log as unrecoverable, remove from state

Note: listing each album's contents requires one paginated API call per album.
For large libraries this can take a while.
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
from typing import Optional, Set

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
GPHOTOS_UNSUPPORTED_EXTS = {'.flv', '.f4v', '.swf'}
TOOSMALL_DIR_NAME = '_TOOSMALL'
UNSUPPORTED_DIR_NAME = '_UNSUPPORTED'

# === CLI ===
parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--path', type=str, required=True, help='Root photos folder (only folders under this path are verified)')
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

# === FORMAT ===
def format_size(size):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.2f}{unit}"
        size /= 1024
    return f"{size:.2f}TB"

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
def _move_to_dir(file: Path, folder_name: str, target_dir_name: str, tag: str):
    dest_folder = Path(PHOTO_ROOT_DIR).parent / target_dir_name / folder_name
    dest_folder.mkdir(parents=True, exist_ok=True)
    dest = dest_folder / file.name
    try:
        shutil.move(str(file), str(dest))
        log(f'[{tag}] Moved {file.name} → {dest}')
    except Exception as e:
        err(f'[{tag}] Failed to move {file.name}: {e}')

def move_to_toosmall(file: Path, folder_name: str):
    _move_to_dir(file, folder_name, TOOSMALL_DIR_NAME, 'TOOSMALL')

def move_to_unsupported(file: Path, folder_name: str):
    _move_to_dir(file, folder_name, UNSUPPORTED_DIR_NAME, 'UNSUPPORTED')

# === FAILURE TRACKING ===
failures = load_json(FAILED_FILE, {
    'UploadError': {}, 'AddToAlbumError': {}, 'TooLarge': {},
    'ExifErrors': {}, 'UnsupportedFormat': {},
})

def add_failure(error_type: str, folder_name: str, file_name: str, folder_path, album_id=None):
    if error_type not in failures:
        failures[error_type] = {}
    if folder_name not in failures[error_type]:
        failures[error_type][folder_name] = {'path': str(Path(folder_path).resolve()), 'files': []}
        if album_id:
            failures[error_type][folder_name]['album_id'] = album_id
    if file_name not in failures[error_type][folder_name]['files']:
        failures[error_type][folder_name]['files'].append(file_name)
    if not DRY_RUN:
        save_json(FAILED_FILE, failures)

# === AUTH ===
def authenticate() -> AuthorizedSession:
    creds = None
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception:
            creds = None
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception:
            creds = None
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
        creds = flow.run_local_server(port=0, prompt='consent')
        with open(TOKEN_FILE, 'w') as f:
            f.write(creds.to_json())
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

def cleanup_staged(path: Path, original: Path):
    if path != original:
        try:
            path.unlink()
        except Exception:
            pass

# === GOOGLE PHOTOS API ===
session: Optional[AuthorizedSession] = None

def list_album_filenames(album_id: str) -> Set[str]:
    """Return set of filenames (lowercased) currently in the album via API."""
    filenames: Set[str] = set()
    page_token = None
    while True:
        body: dict = {'albumId': album_id, 'pageSize': 100}
        if page_token:
            body['pageToken'] = page_token
        r = session.post(
            'https://photoslibrary.googleapis.com/v1/mediaItems:search',
            json=body, timeout=(10, 60),
        )
        if r.status_code != 200:
            err(f'[API] Error listing album {album_id}: {r.status_code} {r.text[:200]}')
            break
        data = r.json()
        for item in data.get('mediaItems', []):
            fn = item.get('filename')
            if fn:
                filenames.add(fn.lower())
        page_token = data.get('nextPageToken')
        if not page_token:
            break
    return filenames

def check_album_exists(album_id: str) -> bool:
    if not album_id:
        return False
    r = session.get(
        f'https://photoslibrary.googleapis.com/v1/albums/{album_id}',
        timeout=(10, 30),
    )
    return r.status_code == 200

def api_create_album(title: str) -> str:
    r = session.post(
        'https://photoslibrary.googleapis.com/v1/albums',
        json={'album': {'title': title[:100]}}, timeout=(10, 30),
    )
    r.raise_for_status()
    return r.json()['id']

@retry(wait=wait_exponential(multiplier=2, min=5, max=300), stop=stop_after_attempt(7))
async def upload_file(file_path: str) -> str:
    file_name = Path(file_path).name
    file_size = os.path.getsize(file_path)
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
        raise Exception(f'Add to album failed: {r.status_code} {r.text[:200]}')
    for item in r.json().get('newMediaItemResults', []):
        status = item.get('status', {})
        if status.get('code', -1) == 0 or status.get('message') == 'Success':
            return
        raise Exception(f"Add to album: {status.get('message', 'Unknown error')}")
    raise Exception('No results in batchCreate response')

# === VERIFY FOLDER ===
async def verify_folder(folder_name: str, folder_state: dict, state: dict):
    folder_path = Path(folder_state.get('path', ''))
    album_id: str = folder_state.get('album_id', '')
    files_in_state: list = list(folder_state.get('files', []))

    if not files_in_state:
        return

    log(f'\n{"=" * 80}')
    log(f'[VERIFY] {folder_name}  ({len(files_in_state)} files in state)')

    # Check album and list its contents
    if check_album_exists(album_id):
        log(f'[VERIFY] Listing album contents via API...')
        album_filenames = list_album_filenames(album_id)
        log(f'[VERIFY] Album has {len(album_filenames)} item(s)')
    else:
        log(f'[VERIFY] Album not found (id={album_id!r}) — will recreate if needed')
        album_filenames = set()
        album_id = ''

    files_to_remove: list = []

    for file_name in files_in_state:
        # Case-insensitive comparison (Google Photos may normalise casing)
        if file_name.lower() in album_filenames:
            continue  # ✓ already in album

        log(f'  [MISSING] {file_name}')
        local_path = folder_path / file_name

        # --- local file missing ---
        if not local_path.exists():
            log(f'    → Not found locally either — removing from state')
            files_to_remove.append(file_name)
            continue

        # --- unsupported format ---
        if not is_supported_media(local_path):
            log(f'    → Unsupported format — moving to _UNSUPPORTED')
            if not DRY_RUN:
                move_to_unsupported(local_path, folder_name)
            files_to_remove.append(file_name)
            continue

        # --- empty file ---
        file_size = local_path.stat().st_size
        if file_size == 0:
            log(f'    → Empty file (0 bytes) — moving to _TOOSMALL')
            if not DRY_RUN:
                move_to_toosmall(local_path, folder_name)
            files_to_remove.append(file_name)
            continue

        # --- re-upload ---
        log(f'    → Exists locally ({format_size(file_size)}) — re-uploading')
        if DRY_RUN:
            log(f'    [DRY-RUN] Would re-upload to album "{folder_name}"')
            continue

        # Create album if missing
        if not album_id:
            try:
                album_id = api_create_album(folder_name)
                state[folder_name]['album_id'] = album_id
                log(f'    [ALBUM] Created: {folder_name} (id: {album_id})')
            except Exception as e:
                err(f'    [ALBUM] Failed to create album: {e}')
                add_failure('UploadError', folder_name, file_name, folder_path, album_id)
                files_to_remove.append(file_name)
                continue

        local_file = stage_local_copy_if_cloud(local_path)
        try:
            token = await upload_file(str(local_file))
            await add_to_album(token, album_id, file_name)
            log(f'    ✅ Re-uploaded successfully')
        except Exception as e:
            err(f'    ❌ Re-upload failed: {e}')
            add_failure('UploadError', folder_name, file_name, folder_path, album_id)
            files_to_remove.append(file_name)
        finally:
            cleanup_staged(local_file, local_path)

    # Apply state removals
    if files_to_remove and not DRY_RUN:
        folder_state['files'] = [f for f in folder_state.get('files', []) if f not in files_to_remove]
        log(f'  [STATE] Removed {len(files_to_remove)} file(s) from state entry')

# === MAIN ===
async def main():
    global session
    log('[INIT] Authenticating with Google Photos...')
    session = authenticate()
    log('[INIT] Authenticated')

    state = load_json(STATE_FILE, {})
    root = str(Path(PHOTO_ROOT_DIR).resolve())

    # Filter to folders under the given root path
    folders = {
        name: info for name, info in state.items()
        if str(Path(info.get('path', '')).resolve()).startswith(root)
    }
    log(f'[INIT] {len(folders)} folder(s) to verify under {root}')
    if DRY_RUN:
        log('[INIT] DRY-RUN mode — no changes will be made\n')

    for folder_name, folder_state in sorted(folders.items()):
        await verify_folder(folder_name, folder_state, state)

    if not DRY_RUN:
        save_json(STATE_FILE, state)
        log('\n[DONE] upload_state.json updated.')
    else:
        log('\n[DRY-RUN] No changes made.')

if __name__ == '__main__':
    asyncio.run(main())
