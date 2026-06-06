# usage: python3 gphotos_uploader.py --path "/your/folder"
# usage: python3 gphotos_uploader.py --path "/your/folder" --retry-failed

import os
import json
import time
import logging
import sys
import warnings
from tqdm import tqdm
from pathlib import Path
from tenacity import retry, wait_fixed, stop_after_attempt, wait_exponential, retry_if_exception
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2.credentials import Credentials
import argparse
import subprocess
import re
from datetime import datetime
import gc
from typing import Tuple, Optional
import urllib.parse
import asyncio
from requests.exceptions import ReadTimeout, ConnectTimeout, Timeout, ConnectionError as RequestsConnectionError
import shutil
import signal
import tempfile
import hashlib
import mimetypes
import unicodedata

# Suppress urllib3 SSL warnings
warnings.filterwarnings('ignore', category=Warning)

class NonRetryableError(RuntimeError):
    """Raised for deterministic failures that should not be retried."""
    pass

class EmptyCloudFileError(NonRetryableError):
    """Raised when a cloud file is still 0 bytes after waiting for Drive to download it."""
    pass

# Custom retry predicate to NOT retry on KeyboardInterrupt or NonRetryableError
def retry_if_not_keyboard_interrupt(exception):
    if isinstance(exception, (KeyboardInterrupt, NonRetryableError)):
        raise exception
    return True

def log_init(msg):
    print(msg, flush=True)
    sys.stdout.flush()

log_init("[INIT] Script starting...")

SUPPORTED_EXIF_EXT = ('.jpg', '.jpeg', '.heic', '.heif', '.cr2', '.tif', '.tiff', '.mov', '.mp4', '.nef', '.flv', '.avi', '.m4v', '.mgg', '.rw2')

# Formats that produce a valid video/* MIME type but are NOT accepted by Google Photos
GPHOTOS_UNSUPPORTED_EXTS = {'.flv', '.f4v', '.swf'}

def is_supported_media(file_path: Path) -> bool:
    """Check if file is a supported Google Photos media type (image or video)

    Tries MIME type first, then falls back to extension if MIME is unreliable.
    """
    ext = file_path.suffix.lower()

    # Reject formats not accepted by Google Photos even if they have a valid MIME type
    if ext in GPHOTOS_UNSUPPORTED_EXTS:
        return False

    mime_type, _ = mimetypes.guess_type(str(file_path))

    # Primary: MIME type detection
    if mime_type:
        return mime_type.startswith('image/') or mime_type.startswith('video/')

    # Fallback: extension whitelist (for cases where MIME is missing or unreliable)
    supported_exts = {
        # Images
        ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp",
        ".heic", ".heif", ".raw", ".cr2", ".nef", ".rw2",
        # Video
        ".mp4", ".mov", ".avi", ".mkv", ".wmv", ".webm",
        ".m4v", ".3gp", ".3g2", ".mts", ".m2ts", ".wm"
    }
    return ext in supported_exts

# === CLI ===
log_init("[INIT] Setting up argument parser...")
parser = argparse.ArgumentParser(description="Uploader per Google Photos")
parser.add_argument("--path", type=str, required=True, help="Absolute path to the folder to process")
parser.add_argument("--retry-failed", action="store_true", help="Retry files listed in failed_uploads.json")
parser.add_argument("--fix-dates", action="store_true", help="Check and fix EXIF and system dates using folder names")
parser.add_argument("--dry-run", action="store_true", help="Simulate all actions without uploading or modifying anything")
parser.add_argument("--debug", action="store_true", help="Enable detailed debug logging of API responses")

log_init("[INIT] Parsing arguments...")
try:
    args = parser.parse_args()
    log_init(f"[INIT] Arguments parsed: path={args.path}, retry_failed={args.retry_failed}, fix_dates={args.fix_dates}, dry_run={args.dry_run}, debug={args.debug}")
except Exception as e:
    print(f"Error parsing arguments: {e}", file=sys.stderr)
    sys.exit(1)

PHOTO_ROOT_DIR = args.path
RETRY_FAILED = args.retry_failed
FIX_DATES = args.fix_dates
DRY_RUN = args.dry_run
DEBUG_MODE = args.debug

# === CONFIG ===
log_init("[INIT] Loading configuration...")
SCOPES = [
    'https://www.googleapis.com/auth/photoslibrary.appendonly',
    'https://www.googleapis.com/auth/photoslibrary.readonly'
]
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'
LOG_FILE = 'upload.log'
STATE_FILE = 'upload_state.json'
FAILED_FILE = 'failed_uploads.json'
CHUNK_SIZE_BYTES = 32768  # 32KB read size to keep socket active
TIMEOUT_COUNTS = {}  # per-file timeout counters for adaptive cooldown
SAFETY_BUFFER_BYTES = 500 * 1024 * 1024  # 500 MB safety buffer
TOOSMALL_DIR_NAME = "_TOOSMALL"
UNSUPPORTED_DIR_NAME = "_UNSUPPORTED"

def format_size(size):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.2f}{unit}"
        size /= 1024
    return f"{size:.2f}TB"

def _move_to_dir(file: Path, folder_name: str, target_dir_name: str, tag: str):
    dest_folder = Path(PHOTO_ROOT_DIR).parent / target_dir_name / folder_name
    dest_folder.mkdir(parents=True, exist_ok=True)
    dest = dest_folder / file.name
    try:
        shutil.move(str(file), str(dest))
        log_warn(f"[{tag}] Moved {file.name} → {dest}")
    except Exception as e:
        log_error(f"[{tag}] Failed to move {file.name}: {e}")

def move_to_toosmall(file: Path, folder_name: str):
    _move_to_dir(file, folder_name, TOOSMALL_DIR_NAME, "TOOSMALL")

def move_to_unsupported(file: Path, folder_name: str):
    _move_to_dir(file, folder_name, UNSUPPORTED_DIR_NAME, "UNSUPPORTED")

# === LOGGING ===
log_init("[INIT] Setting up logging...")
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_warn(msg):
    print(msg, flush=True)
    logging.warning(msg)
    sys.stdout.flush()
    
def log_error(msg, exc_info=False):
    print(msg, file=sys.stderr, flush=True)
    if exc_info:
        logging.error(msg, exc_info=True)
    else:
        logging.error(msg)
    sys.stderr.flush()

def log_debug(msg, data=None):
    if DEBUG_MODE:
        if data:
            print(f"[DEBUG] {msg}\n{json.dumps(data, indent=2)}", flush=True)
            logging.debug(f"{msg}\n{json.dumps(data, indent=2)}")
        else:
            print(f"[DEBUG] {msg}", flush=True)
            logging.debug(msg)
        sys.stdout.flush()

log_init("[INIT] Checking root directory...")
if not os.path.isdir(PHOTO_ROOT_DIR):
    log_error(f"❌ Invalid folder: {PHOTO_ROOT_DIR}")
    sys.exit(1)

log_init("[INIT] Script initialization complete")

# === STATE HANDLING ===
def load_json(path, default):
    if os.path.exists(path):
        with open(path, 'r') as f:
            data = json.load(f)
            log_warn(f"[STATE] Loaded state from {path}: {len(data)} entries")
            return data
    log_warn(f"[STATE] No state file found at {path}, using default")
    return default

def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    if path == FAILED_FILE:
        total = sum(len(e.get('files', [])) for cat in data.values() for e in cat.values())
        log_warn(f"[STATE] Saved {path}: {total} failed file(s) across {sum(len(c) for c in data.values())} folder(s)")
    else:
        log_warn(f"[STATE] Saved state to {path}: {len(data)} entries")

state = load_json(STATE_FILE, {})
# Clean up state entries that point to non-existent folders
state = {
    k: v for k, v in state.items()
    if os.path.isdir(v.get("path", ""))
}
log_warn(f"[STATE] Cleaned state: {len(state)} valid entries")

failures = load_json(FAILED_FILE, {
    "UploadError": {},
    "AddToAlbumError": {},
    "TooLarge": {},
    "ExifErrors": {},
    "UnsupportedFormat": {}
})
# Cache for albums and folders
album_cache = {}

# === AUTH ===
def _scopes_ok(creds, required_scopes):
    granted = set(creds.scopes or [])
    return set(required_scopes).issubset(granted)

def authenticate():
    """
    OAuth authentication with token persistence.
    First run: browser login + save token.json
    Subsequent runs: load token.json, auto-refresh if expired
    """
    creds = None

    try:
        # 1) credentials.json exists?
        if not os.path.exists(CREDENTIALS_FILE):
            log_error(f"[AUTH] Credentials file not found: {CREDENTIALS_FILE}")
            raise FileNotFoundError(f"Credentials file not found: {CREDENTIALS_FILE}")

        # 2) load token if present
        if os.path.exists(TOKEN_FILE):
            log_warn("[AUTH] Loading existing token.json")
            try:
                creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
                log_warn("[AUTH] Token loaded successfully")

                # scope check (forces re-consent if token was created with fewer scopes)
                if creds and creds.scopes and not _scopes_ok(creds, SCOPES):
                    log_warn(f"[AUTH] Token scopes insufficient: {creds.scopes} -> forcing re-auth")
                    creds = None
            except Exception as e:
                log_warn(f"[AUTH] Failed to load token.json: {e}, will re-authenticate")
                creds = None

        # 3) refresh if possible
        if creds and creds.expired and creds.refresh_token:
            log_warn("[AUTH] Token expired, refreshing...")
            try:
                creds.refresh(Request())
                log_warn("[AUTH] Token refreshed successfully")

                if creds and creds.scopes and not _scopes_ok(creds, SCOPES):
                    log_warn(f"[AUTH] Token scopes insufficient after refresh: {creds.scopes} -> forcing re-auth")
                    creds = None
            except Exception as e:
                log_warn(f"[AUTH] Refresh failed: {e}, re-authenticating")
                creds = None

        # 4) full OAuth if still missing/invalid
        if not creds or not creds.valid:
            log_warn("[AUTH] Starting OAuth flow (browser will open)")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0, prompt="consent")
            log_warn("[AUTH] Successfully obtained credentials via OAuth")

        # 5) save token only if creds OK
        if creds and creds.valid:
            with open(TOKEN_FILE, "w") as token:
                token.write(creds.to_json())
            log_warn("[AUTH] Saved token.json for future runs")

        log_warn(f"[AUTH] Granted scopes: {creds.scopes}")
        return AuthorizedSession(creds)

    except Exception as e:
        log_error(f"[AUTH] Authentication failed: {str(e)}", exc_info=True)
        raise
try:
    log_warn("[INIT] Initializing Google Photos session...")
    session = authenticate()
    log_warn("[INIT] Successfully initialized session")
except Exception as e:
    log_error(f"[INIT] Failed to initialize session: {str(e)}", exc_info=True)
    exit(1)

# === API WRAPPERS ===

def is_album_id_valid(album_id: str) -> bool:
    """Validate album ID by checking if it exists and is accessible"""
    if not album_id:
        return False
    try:
        response = session.get(
            f"https://photoslibrary.googleapis.com/v1/albums/{album_id}",
            timeout=(10, 30)
        )
        if response.status_code == 200:
            return True
        elif response.status_code == 403:
            # 403 = insufficient scopes OR album is inaccessible/deleted. Treat as INVALID to force recreation
            log_warn(f"[ALBUM] Album id {album_id} returned 403 → treating as INVALID (will recreate)")
            return False
        else:
            log_warn(f"[ALBUM] Album ID validation failed: {album_id} (status {response.status_code})")
            return False
    except Exception as e:
        log_warn(f"[ALBUM] Album ID validation error: {album_id} - {str(e)}")
        return False

@retry(wait=wait_fixed(5), stop=stop_after_attempt(5), retry=retry_if_exception(retry_if_not_keyboard_interrupt))
def create_album(title):
    log_warn(f"[ALBUM] Creating album: {title}")
    
    # SAFETY CHECK: Before creating, do a final search to avoid duplicates
    # (in case another process created it between our search and now)
    existing_id = search_album_by_name(title)
    if existing_id:
        log_warn(f"[ALBUM] Album '{title}' was already created, using existing (id: {existing_id})")
        return existing_id
    
    body = {"album": {"title": title[:100]}}
    try:
        log_warn(f"[ALBUM] Sending request to create album: {title}")
        response = session.post(
            "https://photoslibrary.googleapis.com/v1/albums",
            json=body,
            timeout=(10, 30)
        )
        if response.status_code != 200:
            log_error(f"[ALBUM] Error creating album: {response.status_code} - {response.text}")
            raise Exception(f"Errore creazione album: {response.text}")
        album_id = response.json()["id"]
        log_warn(f"[ALBUM] Successfully created album: {title} (id: {album_id})")
        return album_id
    except Exception as e:
        log_error(f"[ALBUM] Failed to create album {title}: {str(e)}")
        raise

@retry(wait=wait_fixed(5), stop=stop_after_attempt(5), retry=retry_if_exception(retry_if_not_keyboard_interrupt))
def search_album_by_name(title):
    global album_cache
    
    # 1. First check upload_state.json - but VALIDATE the ID
    if title in state:
        album_id = state[title].get('album_id')
        if album_id and is_album_id_valid(album_id):
            log_warn(f"[ALBUM] Found album '{title}' in upload_state.json (id: {album_id})")
            return album_id
        elif album_id:
            log_warn(f"[ALBUM] Album id in state is invalid for '{title}': {album_id} (will re-search/create)")
    
    # 2. Then check cache
    if title in album_cache:
        album_id = album_cache[title]
        if is_album_id_valid(album_id):
            log_warn(f"[ALBUM] Found album '{title}' in cache (id: {album_id})")
            return album_id
        else:
            log_warn(f"[ALBUM] Album id in cache is invalid for '{title}', removing from cache")
            del album_cache[title]
    
    # 3. If not found, create cache and search
    log_warn(f"[ALBUM] Building album cache...")
    try:
        page_token = None
        while True:
            url = "https://photoslibrary.googleapis.com/v1/albums"
            if page_token:
                url += f"?pageToken={page_token}"
                
            response = session.get(url, timeout=(10, 30))
            if response.status_code != 200:
                log_error(f"[ALBUM] Error searching albums: {response.status_code} - {response.text}")
                return None
                
            data = response.json()
            albums = data.get('albums', [])
            
            # Update cache with all albums
            for album in albums:
                album_title = album.get('title')
                album_id = album.get('id')
                if album_title and album_id:
                    album_cache[album_title] = album_id
                    if album_title == title:
                        log_warn(f"[ALBUM] Found existing album: {title} (id: {album_id})")
                        return album_id
            
            page_token = data.get('nextPageToken')
            if not page_token:
                break
                
            log_warn(f"[ALBUM] Checking next page of albums...")
            
        log_warn(f"[ALBUM] No existing album found with title: '{title}'")
        return None
    except Exception as e:
        log_error(f"[ALBUM] Failed to search for album: {str(e)}")
        return None

@retry(wait=wait_exponential(multiplier=2, min=5, max=300), stop=stop_after_attempt(7), retry=retry_if_exception(retry_if_not_keyboard_interrupt))
async def upload_file(file_path):
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        error_msg = "Empty staged file (0 bytes). Cloud hydration failed."
        log_error(f"[UPLOAD] {error_msg}")
        raise NonRetryableError(error_msg)
    max_size = 10 * 1024 * 1024 * 1024  # 10 GB

    folder_name = Path(file_path).parent.name
    file_name = Path(file_path).name

    log_warn(f"[UPLOAD] Starting upload of {file_name}")
    # Disk space check: require file size + 500MB free
    try:
        usage = shutil.disk_usage(str(Path(file_path).parent))
        free_space = usage.free
        required = file_size + SAFETY_BUFFER_BYTES
        if free_space < required:
            log_error(f"❌ Insufficient disk space. Required: {required} bytes, Free: {free_space} bytes. Need at least 500MB over file size.")
            sys.exit(1)
    except Exception as e:
        log_warn(f"[DISK] Could not check free space: {e}")
    log_warn(f"[UPLOAD] File size: {format_size(file_size)} (max allowed: {format_size(max_size)})")
    log_debug("Size details:", {
        "bytes": file_size,
        "max_bytes": max_size,
        "percentage_of_max": (file_size / max_size) * 100
    })

    # Remove size check - now done in process_file() before calling upload_file()

    headers = {
        'Content-Type': 'application/octet-stream',
        'X-Goog-Upload-File-Name': urllib.parse.quote(unicodedata.normalize('NFC', file_name)),
        'X-Goog-Upload-Protocol': 'raw',
    }
    
    log_debug("Upload headers:", headers)

    try:
        log_warn(f"[UPLOAD] Opening file for reading: {file_path}")
        with open(file_path, 'rb') as f:
            log_warn(f"[UPLOAD] Sending file to Google Photos API...")
            try:
                # Calcola timeout in base alla dimensione del file
                # Connect timeout: 30s, Read timeout: max(20 min, 1 min per GB + 5 min base)
                read_timeout = max(1200, (file_size / (1024 * 1024 * 1024)) * 60 + 300)
                connect_timeout = 30
                log_debug("Calculated timeout:", {
                    "connect_seconds": connect_timeout,
                    "read_seconds": read_timeout,
                    "based_on_size": format_size(file_size)
                })

                # Crea una progress bar
                pbar = tqdm(
                    total=file_size,
                    unit='B',
                    unit_scale=True,
                    desc=f"Uploading {file_name}",
                    ncols=100
                )

                # Leggi il file a blocchi, aggiorna la barra e accumula i dati
                # (per file grandi, meglio inviare direttamente il file object)
                class FileWithProgress:
                    def __init__(self, file, pbar):
                        self.file = file
                        self.pbar = pbar
                    def read(self, size=CHUNK_SIZE_BYTES):
                        chunk = self.file.read(size)
                        if chunk:
                            self.pbar.update(len(chunk))
                        return chunk
                    def __getattr__(self, attr):
                        return getattr(self.file, attr)

                file_with_progress = FileWithProgress(f, pbar)
                response = session.post(
                    "https://photoslibrary.googleapis.com/v1/uploads",
                    data=file_with_progress,
                    headers=headers,
                    timeout=(connect_timeout, read_timeout)
                )
                pbar.close()

                # Log detailed response information
                log_debug("Upload response status:", response.status_code)
                log_debug("Upload response headers:", {k: v for k, v in response.headers.items()})
                log_debug("Upload response content:", response.text)
                
                # Handle rate limiting with exponential backoff
                if response.status_code == 429:
                    # Default to Retry-After header if present, else check body for RESOURCE_EXHAUSTED
                    retry_after = response.headers.get('Retry-After')
                    if retry_after is not None:
                        retry_after = int(retry_after)
                    else:
                        # If RESOURCE_EXHAUSTED, pause for 4 hours as requested
                        try:
                            err = response.json().get('error', {})
                            status = err.get('status')
                            message = err.get('message', '')
                        except Exception:
                            status = None
                            message = response.text or ''
                        if status == 'RESOURCE_EXHAUSTED' or 'Quota exceeded' in message:
                            retry_after = 4 * 60 * 60  # 4 hours
                        else:
                            retry_after = 120  # fallback
                    log_warn(f"[UPLOAD] Rate limit/RESOURCE_EXHAUSTED. Pausing {retry_after} seconds before retry...")
                    log_debug("Rate limit response:", {
                        "status": response.status_code,
                        "headers": dict(response.headers),
                        "body": response.text
                    })
                    await interruptible_sleep(retry_after)
                    raise Exception(f"Rate limit/RESOURCE_EXHAUSTED, retrying after {retry_after}s delay")

                if response.status_code != 200:
                    error_msg = f"[UPLOAD] Error response from API: {response.status_code} - {response.text}"
                    log_error(error_msg)
                    log_debug("Upload error details:", {
                        "status": response.status_code,
                        "headers": dict(response.headers),
                        "body": response.text
                    })
                    try:
                        raw_bytes = response.content[:500]
                        log_warn(f"[UPLOAD] Raw response bytes (truncated): {raw_bytes}")
                    except Exception:
                        pass
                    raise Exception(error_msg)
                
                # Validate upload token
                upload_token = response.text.strip()
                if not upload_token or len(upload_token) < 10:  # Basic validation
                    error_msg = f"[UPLOAD] Invalid upload token received: {upload_token}"
                    log_error(error_msg)
                    raise Exception(error_msg)
                
                log_warn(f"[UPLOAD] Successfully uploaded {file_name}")
                TIMEOUT_COUNTS.pop(str(file_path), None)  # reset on success
                # Add delay after successful upload to avoid quota issues
                if shutdown_handler.check():
                    raise KeyboardInterrupt()
                await interruptible_sleep(30)  # 30 second delay between uploads
                log_debug("Upload token:", upload_token)
                return upload_token
                
            except Exception as e:
                log_error(f"[UPLOAD] Error during API request: {str(e)}", exc_info=True)
                log_warn(f"[UPLOAD] Raw exception: {repr(e)}")
                # Targeted cooldown on timeouts/connection errors (likely quota/network stalls)
                if isinstance(e, (ReadTimeout, ConnectTimeout, Timeout, RequestsConnectionError)):
                    key = str(file_path)
                    TIMEOUT_COUNTS[key] = TIMEOUT_COUNTS.get(key, 0) + 1
                    steps = [180, 300, 600, 1200]
                    cooldown = steps[min(TIMEOUT_COUNTS[key]-1, len(steps)-1)]
                    log_warn(f"[UPLOAD] Timeout/Connection error detected (count={TIMEOUT_COUNTS[key]}). Cooling down for {cooldown}s before retry...")
                    # Check shutdown before starting cooldown
                    if shutdown_handler.check():
                        raise KeyboardInterrupt()
                    await interruptible_sleep(cooldown)
                raise
    except KeyboardInterrupt:
        log_warn(f"[UPLOAD] Upload interrupted by user")
        raise
    except Exception as e:
        log_error(f"[UPLOAD] Failed to upload {file_name}: {str(e)}", exc_info=True)
        log_warn(f"[UPLOAD] Raw exception: {repr(e)}")
        raise

async def add_to_album(upload_token, album_id, description, folder_name):
    log_warn(f"[ALBUM] Adding photo to album {album_id}: {folder_name}")
    
    # Validate upload token
    if not upload_token or len(upload_token) < 10:
        error_msg = f"[ALBUM] Invalid upload token provided: {upload_token}"
        log_error(error_msg)
        raise Exception(error_msg)
    
    body = {
        'albumId': album_id,
        'newMediaItems': [{
            'description': description,
            'simpleMediaItem': {'uploadToken': upload_token}
        }]
    }
    try:
        log_warn(f"[ALBUM] Sending request to add photo to album")
        log_debug("Request body:", body)
        response = session.post(
            "https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate",
            json=body,
            timeout=(10, 30)
        )
        
        # Log detailed response information
        log_debug("Response status:", response.status_code)
        log_debug("Response headers:", {k: v for k, v in response.headers.items()})
        log_debug("Response content:", response.text)
        
        # Handle rate limiting with exponential backoff
        if response.status_code == 429:
            retry_after = response.headers.get('Retry-After')
            if retry_after is not None:
                retry_after = int(retry_after)
            else:
                try:
                    err = response.json().get('error', {})
                    status = err.get('status')
                    message = err.get('message', '')
                except Exception:
                    status = None
                    message = response.text or ''
                if status == 'RESOURCE_EXHAUSTED' or 'Quota exceeded' in message:
                    retry_after = 4 * 60 * 60  # 4 hours
                else:
                    retry_after = 120
            log_warn(f"[ALBUM] Rate limit/RESOURCE_EXHAUSTED. Pausing {retry_after} seconds before retry...")
            log_debug("Rate limit response:", {
                "status": response.status_code,
                "headers": dict(response.headers),
                "body": response.text
            })
            if shutdown_handler.check():
                raise KeyboardInterrupt()
            await interruptible_sleep(retry_after)
            raise Exception(f"Rate limit/RESOURCE_EXHAUSTED, retrying after {retry_after}s delay")
        
        # Handle invalid album ID (400) - album was deleted or is no longer accessible
        if response.status_code == 400 and "Invalid album ID" in response.text:
            log_warn(f"[ALBUM] Album {album_id} has invalid ID, recreating for {folder_name}")
            # CRITICAL: Delete the zombie ID from state and cache to prevent infinite loop
            if folder_name in state:
                del state[folder_name]
                save_json(STATE_FILE, state)
            if folder_name in album_cache:
                del album_cache[folder_name]
            # Now search for existing or create new album (without the poison ID)
            new_album_id = search_album_by_name(folder_name) or create_album(folder_name)
            state[folder_name] = {'album_id': new_album_id, 'path': str(Path(PHOTO_ROOT_DIR) / folder_name), 'files': []}
            save_json(STATE_FILE, state)
            body['albumId'] = new_album_id
            log_debug("Retrying with new album ID")
            response = session.post(
                "https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate",
                json=body,
                timeout=(10, 30)
            )
            
        if response.status_code == 404 and "The provided ID does not match any albums" in response.text:
            # Album no longer exists, remove it from state and create a new one
            log_warn(f"[ALBUM] Album {album_id} no longer exists, removing from state and creating new album for {folder_name}")
            
            # Remove old album from state
            if folder_name in state:
                del state[folder_name]
                save_json(STATE_FILE, state)
            
            # Create new album
            # First try to reuse an existing album with the same title to avoid duplicates
            new_album_id = search_album_by_name(folder_name) or create_album(folder_name)
            # Update state with new album ID
            # Preserve existing path and files
            resolved_path = state.get(folder_name, {}).get('path', str(Path(PHOTO_ROOT_DIR) / folder_name))
            existing_files = state.get(folder_name, {}).get('files', [])
            state[folder_name] = {
                'album_id': new_album_id,
                'path': resolved_path,
                'files': existing_files
            }
            save_json(STATE_FILE, state)
            # Retry with new album ID
            body['albumId'] = new_album_id
            log_debug("Retrying with new album ID. Request body:", body)
            response = session.post(
                "https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate",
                json=body,
                timeout=(10, 30)
            )
            
        if response.status_code != 200:
            log_error(f"[ALBUM] Error response from API: {response.status_code} - {response.text}")
            log_debug("Error response body:", response.json() if response.text else None)
            try:
                raw_bytes = response.content[:500]
                log_warn(f"[ALBUM] Raw response bytes (truncated): {raw_bytes}")
            except Exception:
                pass
            raise Exception(f"Error adding to album: {response.text}")
            
        # Parse response
        result = response.json()
        log_debug("API Response:", result)
        
        if 'newMediaItemResults' not in result:
            log_error(f"[ALBUM] Unexpected API response format: {result}")
            raise Exception("Unexpected API response format")
            
        for item in result['newMediaItemResults']:
            if 'status' not in item:
                log_error(f"[ALBUM] Missing status in response: {item}")
                raise Exception("Missing status in response")
                
            status = item['status']
            if status.get('code') == 0 or status.get('message') == 'Success':
                log_warn(f"✅ ✅ [ALBUM] Successfully added photo to album: {description}")
                photo_id = item.get('mediaItem', {}).get('id')
                return photo_id, body['albumId']  # Return both photo_id and the effective album_id used
            else:
                error_msg = status.get('message', 'Unknown error')
                log_error(f"[ALBUM] Failed to add media item: {error_msg}")
                raise Exception(f"Failed to add media item: {error_msg}")
                
        # If we get here, something went wrong
        log_error(f"[ALBUM] No success status found in response: {result}")
        raise Exception("No success status found in response")
        
    except Exception as e:
        log_error(f"[ALBUM] Failed to add photo to album: {str(e)}")
        log_warn(f"[ALBUM] Raw exception: {repr(e)}")
        raise

async def add_existing_media_to_album(media_item_id, album_id, folder_name):
    """Add an existing media item to an album using its ID (no re-upload needed)"""
    log_warn(f"[ALBUM] Adding existing media item {media_item_id} to album {album_id}: {folder_name}")
    
    body = {
        'albumId': album_id,
        'mediaItemIds': [media_item_id]
    }
    
    try:
        log_warn(f"[ALBUM] Sending request to add existing media item to album")
        log_debug("Request body:", body)
        response = session.post("https://photoslibrary.googleapis.com/v1/albums/{albumId}:batchAddMediaItems".format(albumId=album_id), json=body, timeout=(10, 30))

        # Log detailed response information
        log_debug("Response status:", response.status_code)
        log_debug("Response headers:", {k: v for k, v in response.headers.items()})
        log_debug("Response content:", response.text)

        # Handle rate limiting with exponential backoff
        if response.status_code == 429:
            retry_after = response.headers.get('Retry-After')
            if retry_after is not None:
                retry_after = int(retry_after)
            else:
                try:
                    err = response.json().get('error', {})
                    status = err.get('status')
                    message = err.get('message', '')
                except Exception:
                    status = None
                    message = response.text or ''
                if status == 'RESOURCE_EXHAUSTED' or 'Quota exceeded' in message:
                    retry_after = 4 * 60 * 60  # 4 hours
                else:
                    retry_after = 120
            log_warn(f"[ALBUM] Rate limit/RESOURCE_EXHAUSTED. Pausing {retry_after} seconds before retry...")
            log_debug("Rate limit response:", {
                "status": response.status_code,
                "headers": dict(response.headers),
                "body": response.text
            })
            if shutdown_handler.check():
                raise KeyboardInterrupt()
            await interruptible_sleep(retry_after)
            raise Exception(f"Rate limit/RESOURCE_EXHAUSTED, retrying after {retry_after}s delay")

        # Handle invalid album ID (400) - album was deleted or is no longer accessible
        if response.status_code == 400 and "Invalid album ID" in response.text:
            log_warn(f"[ALBUM] Album {album_id} has invalid ID, recreating for {folder_name}")
            if folder_name in state:
                del state[folder_name]
                save_json(STATE_FILE, state)
            new_album_id = search_album_by_name(folder_name) or create_album(folder_name)
            resolved_path = state.get(folder_name, {}).get('path', str(Path(PHOTO_ROOT_DIR) / folder_name))
            state[folder_name] = {'album_id': new_album_id, 'path': resolved_path, 'files': []}
            save_json(STATE_FILE, state)
            body['albumId'] = new_album_id
            log_debug("Retrying with new album ID")
            response = session.post("https://photoslibrary.googleapis.com/v1/albums/{albumId}:batchAddMediaItems".format(albumId=new_album_id), json=body, timeout=(10, 30))
            
        if response.status_code == 404 and "The provided ID does not match any albums" in response.text:
            # Album no longer exists, remove it from state and create a new one
            log_warn(f"[ALBUM] Album {album_id} no longer exists, removing from state and creating new album for {folder_name}")
            
            # Remove old album from state
            if folder_name in state:
                del state[folder_name]
                save_json(STATE_FILE, state)
            
            # Create new album
            new_album_id = search_album_by_name(folder_name) or create_album(folder_name)
            # Update state with new album ID
            # Resolve a stable folder path from existing state or fallback to root/folder_name
            resolved_path = state.get(folder_name, {}).get('path', str(Path(PHOTO_ROOT_DIR) / folder_name))
            state[folder_name] = {
                'album_id': new_album_id,
                'path': resolved_path,
                'files': []
            }
            save_json(STATE_FILE, state)
            # Retry with new album ID
            body['albumId'] = new_album_id
            log_debug("Retrying with new album ID. Request body:", body)
            response = session.post("https://photoslibrary.googleapis.com/v1/albums/{albumId}:batchAddMediaItems".format(albumId=new_album_id), json=body, timeout=(10, 30))

        if response.status_code != 200:
            log_error(f"[ALBUM] Error response from API: {response.status_code} - {response.text}")
            log_debug("Error response body:", response.json() if response.text else None)
            try:
                raw_bytes = response.content[:500]
                log_warn(f"[ALBUM-EXISTING] Raw response bytes (truncated): {raw_bytes}")
            except Exception:
                pass
            raise Exception(f"Error adding existing media to album: {response.text}")
            
        # Parse response
        result = response.json()
        log_debug("API Response:", result)
        
        log_warn(f"[ALBUM] Successfully added existing media item to album: {media_item_id}")
        return True, body['albumId']  # Return both success flag and effective album_id
        
    except Exception as e:
        log_error(f"[ALBUM] Failed to add existing media item to album: {str(e)}")
        log_warn(f"[ALBUM] Raw exception: {repr(e)}")
        raise

# === FAILURE HANDLING ===
def add_failure(error_type, folder_name, file_name, folder_path, album_id=None, photo_id=None, upload_token=None):
    if error_type not in failures:
        failures[error_type] = {}
        
    if folder_name not in failures[error_type]:
        failures[error_type][folder_name] = {
            "path": str(folder_path.resolve()),
            "files": []
        }
        if album_id:
            failures[error_type][folder_name]["album_id"] = album_id
            
    if error_type == "AddToAlbumError":
        # For AddToAlbumError, store additional info including upload token
        failures[error_type][folder_name]["files"].append({
            "name": file_name,
            "photo_id": photo_id,
            "upload_token": upload_token,  # Store the upload token for retry
            "retry_count": 0,
            "last_attempt": datetime.now().isoformat()
        })
    else:
        # For other error types, store the filename and album id if available
        if file_name not in failures[error_type][folder_name]["files"]:
            failures[error_type][folder_name]["files"].append(file_name)
            log_warn(f"[FAILURE] Tracked {error_type}: {folder_name}/{file_name}")
        else:
            log_warn(f"[FAILURE] Already tracked {error_type}: {folder_name}/{file_name}")
        if album_id:
            failures[error_type][folder_name]["album_id"] = album_id

    save_json(FAILED_FILE, failures)

# === DATE FIXING ===

def extract_date_from_folder(folder_name):
    match = re.search(r'(\d{4})(?:[-_]?(\d{2}))?(?:[-_]?(\d{2}))?', folder_name)
    if not match:
        return None

    year = int(match.group(1))
    month = int(match.group(2)) if match.group(2) else None
    day = int(match.group(3)) if match.group(3) else None
    return year, month, day

# ===== NEW: Google Photos date tags (EXIF + QuickTime) =====
EXIF_DATE_FMT = "%Y:%m:%d %H:%M:%S"
GP_DATE_TAGS = [
    "EXIF:DateTimeOriginal",
    "EXIF:CreateDate",
    "EXIF:ModifyDate",
    "QuickTime:CreateDate",
    "QuickTime:ModifyDate",
    "QuickTime:MediaCreateDate",
    "QuickTime:MediaModifyDate",
    "QuickTime:TrackCreateDate",
    "QuickTime:TrackModifyDate",
]

def _parse_exiftool_dt(s: str):
    """Parse exiftool datetime string, handling timezone offsets."""
    s = (s or "").strip()
    if not s:
        return None
    # exiftool sometimes adds timezone like "2020:01:01 10:00:00+01:00"
    # take only the base part
    base = s[:19]
    try:
        return datetime.strptime(base, EXIF_DATE_FMT)
    except Exception:
        return None

def get_gphotos_relevant_dates_exiftool(file_path):
    """Read all Google-Photos-relevant date tags (EXIF + QuickTime) in one call.
    
    Uses explicit tag:value format to avoid misalignment if tags don't exist.
    """
    # Use -a to print all occurrences and tag name with each value
    args = ["exiftool", "-a"]
    for t in GP_DATE_TAGS:
        args.append(f"-{t}")
    args.append(file_path)

    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=8)
        if r.returncode != 0:
            # Don't crash: return empty dict, will use filesystem mtime as fallback
            return {}
        
        out = {}
        # Parse "TagName : Value" format
        for line in r.stdout.splitlines():
            line = line.strip()
            if not line or ':' not in line:
                continue
            
            # Split on first colon only
            tag_part, val_part = line.split(':', 1)
            tag_part = tag_part.strip()
            val_part = val_part.strip()
            
            # Match tag against our requested tags
            for gp_tag in GP_DATE_TAGS:
                # exiftool output format is "TagGroup:TagName", we need to match the end
                if tag_part.endswith(gp_tag.replace(":", ":")):
                    dt = _parse_exiftool_dt(val_part)
                    if dt:
                        out[gp_tag] = dt
                    break
        
        return out
    except Exception as e:
        log_warn(f"[EXIFTOOL-READ] Failed reading multi-date tags on {file_path}: {e}")
        return {}

def is_solidal(folder_info, dt: datetime) -> bool:
    """Check if dt matches folder info (year/month/day as specified)."""
    if not dt:
        return False
    y, m, d = folder_info
    if y is not None and dt.year != y:
        return False
    if m is not None and dt.month != m:
        return False
    if d is not None and dt.day != d:
        return False
    return True

def update_media_dates_if_mismatch(file_path, folder_name):
    """Update media dates (EXIF for images, QuickTime for video) if not solidal with folder."""
    folder_info = extract_date_from_folder(folder_name)
    log_warn(f"Reading date from folder: {folder_info}")
    if not folder_info:
        return

    # 1) Read all dates that matter for Google Photos
    dates = get_gphotos_relevant_dates_exiftool(file_path)

    # 2) Add filesystem mtime as fallback candidate
    fs_dt = datetime.fromtimestamp(Path(file_path).stat().st_mtime)
    candidate_dts = list(dates.values()) + [fs_dt]

    # 3) If any date is solidal with folder → no change needed
    if any(is_solidal(folder_info, dt) for dt in candidate_dts if dt):
        log_warn("[DATES] Dates are solidal with folder info (no change needed)")
        return

    # 4) Mismatch: pick a "base time" to preserve hour/min/sec
    # Priority: DateTimeOriginal → QuickTime:CreateDate → fs mtime
    base_dt = (
        dates.get("EXIF:DateTimeOriginal")
        or dates.get("QuickTime:CreateDate")
        or fs_dt
    )

    new_dt = build_datetime_from_folder_info(base_dt, folder_info)
    dt_str = new_dt.strftime(EXIF_DATE_FMT)

    if DRY_RUN:
        log_warn(f"[DRY-RUN] Would fix media dates of {Path(file_path).name}: base={base_dt} → {new_dt}")
        return

    # 5) Determine if video based on MIME type or extension
    mime_type, _ = mimetypes.guess_type(str(file_path))
    is_video = (
        (mime_type and mime_type.startswith("video/"))
        or Path(file_path).suffix.lower() in (".mov", ".mp4", ".m4v", ".avi", ".flv", ".wmv", ".mkv", ".webm", ".3gp", ".3g2", ".mts", ".m2ts")
    )

    if is_video:
        # Update QuickTime tags (what Google Photos uses for video)
        cmd = [
            "exiftool",
            "-overwrite_original",
            f"-QuickTime:CreateDate={dt_str}",
            f"-QuickTime:ModifyDate={dt_str}",
            f"-QuickTime:MediaCreateDate={dt_str}",
            f"-QuickTime:MediaModifyDate={dt_str}",
            f"-QuickTime:TrackCreateDate={dt_str}",
            f"-QuickTime:TrackModifyDate={dt_str}",
            # Also update EXIF for consistency
            f"-CreateDate={dt_str}",
            f"-ModifyDate={dt_str}",
            file_path
        ]
    else:
        # Update EXIF tags (images/raw)
        cmd = [
            "exiftool",
            "-overwrite_original",
            f"-DateTimeOriginal={dt_str}",
            f"-CreateDate={dt_str}",
            f"-ModifyDate={dt_str}",
            file_path
        ]

    try:
        subprocess.run(cmd, check=True)
        log_warn(f"[FIXED] {Path(file_path).name} dates aligned to folder: {dt_str}")
        # 6) Align filesystem mtime/atime ONLY after exiftool succeeds
        # This prevents desync if exiftool fails partway
        update_file_timestamp(Path(file_path), new_dt)
    except subprocess.CalledProcessError as e:
        add_failure("ExifErrors", folder_name, Path(file_path).name, Path(file_path).parent)
        log_warn(f"[EXIFTOOL-WRITE] Failed to update dates on {file_path}: {e}")

def get_exif_datetimeoriginal_exiftool(file_path):
    """Legacy function - now just reads from the new multi-date function."""
    dates = get_gphotos_relevant_dates_exiftool(file_path)
    return dates.get("EXIF:DateTimeOriginal") or dates.get("QuickTime:CreateDate")

def update_filesystem_date_if_mismatch(file: Path, folder_name: str):
    folder_info = extract_date_from_folder(folder_name)
    if not folder_info:
        return

    # Get current filesystem timestamp
    current_ts = datetime.fromtimestamp(file.stat().st_mtime)
    
    # Only change components that are specified and different from current values
    new_dt = current_ts
    y, m, d = folder_info
    
    if y is not None and y != current_ts.year:
        new_dt = new_dt.replace(year=y)
    if m is not None and m != current_ts.month:
        new_dt = new_dt.replace(month=m)
    if d is not None and d != current_ts.day:
        new_dt = new_dt.replace(day=d)

    if new_dt != current_ts:
        if DRY_RUN:
            log_warn(f"[DRY-RUN] Would fix Creation and Modification dates of {file.name}: {current_ts} → {new_dt}")
        else:
            update_file_timestamp(file, new_dt)
            log_warn(f"[FIXED] Filesystem timestamp of {file.name}: {current_ts} → {new_dt}")

def update_file_timestamp(path: Path, dt: datetime):
    ts = dt.timestamp()  # handles naive and UTC-aware datetimes
    os.utime(path, (ts, ts))

    
def build_datetime_from_folder_info(original_dt: datetime, folder_info: Tuple[Optional[int], Optional[int], Optional[int]]) -> datetime:
    y, m, d = folder_info
    try:
        # Only change components that are specified and different from current values
        new_dt = original_dt
        if y is not None and y != original_dt.year:
            new_dt = new_dt.replace(year=y)
        if m is not None and m != original_dt.month:
            new_dt = new_dt.replace(month=m)
        if d is not None and d != original_dt.day:
            new_dt = new_dt.replace(day=d)
        return new_dt
    except ValueError:
        log_warn(f"[DATES] Invalid date combination from folder info {folder_info}, falling back to day=1")
        # fallback if day is invalid (e.g., February 31st)
        return original_dt.replace(
            year=y if y is not None else original_dt.year,
            month=m if m is not None else original_dt.month,
            day=1
        )
    
def force_file_download(file_path: Path) -> bool:
    """Forza il download di un file usando exiftool."""
    try:
        log_warn(f"[DEBUG] Tentativo di forzare il download di {file_path}")
        
        # Verifica che il file esista e sia leggibile
        if not file_path.exists():
            log_warn(f"[ERROR] File non trovato: {file_path}")
            return False
            
        if not os.access(file_path, os.R_OK):
            log_warn(f"[ERROR] File non leggibile: {file_path}")
            return False
            
        # Usa exiftool per forzare il download, senza text=True per evitare problemi di encoding
        cmd = ["exiftool", "-a", "-u", "-g1", str(file_path)]
        log_warn(f"[DEBUG] Esecuzione comando: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True)
        
        if result.returncode != 0:
            log_warn(f"[ERROR] Errore nell'esecuzione di exiftool: {result.stderr.decode('utf-8', errors='replace')}")
            return False
            
        log_warn(f"[DEBUG] Download forzato completato per {file_path}")
        return True
        
    except Exception as e:
        log_warn(f"[ERROR] Errore durante il download forzato di {file_path}: {str(e)}")
        return False
    finally:
        # Pulisci le risorse
        gc.collect()

# === SIGNAL HANDLING ===
class GracefulShutdown:
    """Handle graceful shutdown on Ctrl+C - double Ctrl+C for hard exit"""
    def __init__(self):
        self.shutdown_requested = False
        self._last_sigint = 0.0
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        import time
        now = time.time()
        # Double Ctrl+C within 1 second = hard exit
        if now - self._last_sigint < 1.0:
            log_warn(f"\n[SHUTDOWN] Second Ctrl+C → forcing exit NOW (no waiting)", flush=True)
            import os
            os._exit(130)  # Immediate exit, no cleanup
        self._last_sigint = now
        log_warn(f"\n[SHUTDOWN] Received signal {signum}. Shutting down gracefully... (press Ctrl+C again to force exit)", flush=True)
        self.shutdown_requested = True
        # Cancel all pending tasks
        try:
            for task in asyncio.all_tasks():
                task.cancel()
        except Exception:
            pass
    
    def check(self):
        """Check if shutdown was requested"""
        return self.shutdown_requested

shutdown_handler = GracefulShutdown()

# === MAIN ===
total_uploaded = 0
total_failed = 0
too_large_files_in_session = set()  # Track files that are too large during this session

async def retry_failed():
    log_warn("🔁 Modalità retry: elaborazione file falliti da failed_uploads.json...\n")

    # Move any previously-recorded UnsupportedFormat files to _UNSUPPORTED and clean up
    for folder_name in list(failures.get("UnsupportedFormat", {}).keys()):
        entry = failures["UnsupportedFormat"][folder_name]
        folder_path = Path(entry.get("path", ""))
        for file_name in list(entry.get("files", [])):
            file = folder_path / file_name
            if file.exists():
                log_warn(f"[RETRY] Moving unsupported file to _UNSUPPORTED: {file_name}")
                move_to_unsupported(file, folder_name)
            failures["UnsupportedFormat"][folder_name]["files"].remove(file_name)
        if not failures["UnsupportedFormat"][folder_name]["files"]:
            del failures["UnsupportedFormat"][folder_name]
    if "UnsupportedFormat" in failures and not failures["UnsupportedFormat"]:
        del failures["UnsupportedFormat"]
    save_json(FAILED_FILE, failures)

    # Build a set of files that are too large to ever retry
    too_large_files = set()
    for folder_name, entry in failures.get("TooLarge", {}).items():
        for file_name in entry.get("files", []):
            too_large_files.add(file_name)

    if too_large_files:
        log_warn(f"[RETRY] Found {len(too_large_files)} file(s) marked as too large - these will be skipped")

    for error_type in ["UploadError", "AddToAlbumError"]:
        log_warn(f"[RETRY] Processing {error_type} failures...")
        for folder_name in list(failures.get(error_type, {}).keys()):
            entry = failures[error_type][folder_name]
            folder_path = Path(entry.get("path"))
            file_list = entry.get("files", [])

            if not folder_path.exists():
                log_warn(f"❌ Folder not found: {folder_path}")
                continue

            album_id = None
            for album_name, album_info in state.items():
                if Path(album_info.get("path")).resolve() == folder_path.resolve():
                    album_id = album_info["album_id"]
                    folder_name = album_name
                    break
            if not album_id:
                try:
                    # Avoid duplicates: search existing album by title before creating
                    album_id = search_album_by_name(folder_name) or create_album(folder_name)
                    state[folder_name] = {
                        'album_id': album_id,
                        'path': str(folder_path.resolve()),
                        'files': []
                    }
                    save_json(STATE_FILE, state)
                except Exception as e:
                    log_error(f"Errore creazione album retry: {e}", exc_info=True)
                    continue

            for item in file_list[:]:
                # Check for shutdown signal
                if shutdown_handler.check():
                    log_warn("[RETRY] Shutdown requested, stopping retry process...")
                    save_json(FAILED_FILE, failures)
                    return
                
                # item can be a filename (str) or a dict (for AddToAlbumError)
                file_name = item.get("name") if isinstance(item, dict) else item
                
                # Skip files that are too large - no point retrying
                if file_name in too_large_files:
                    log_warn(f"[RETRY] ⏭️  Skipping {file_name} (file too large, will not retry)")
                    continue
                
                log_warn(f"[RETRY] Processing file: {file_name}")
                
                # Handle AddToAlbumError differently - skip upload, use stored token/ID
                if error_type == "AddToAlbumError":
                    # Find the file entry in the failures list
                    if isinstance(item, dict):
                        file_entry = item
                    else:
                        file_entry = None
                        for entry in failures[error_type][folder_name]["files"]:
                            if isinstance(entry, dict) and entry.get("name") == file_name:
                                file_entry = entry
                                break
                    
                    if file_entry:
                        photo_id = file_entry.get("photo_id")
                        upload_token = file_entry.get("upload_token")
                        
                        if photo_id:
                            # Try to add existing media item to album
                            try:
                                success, effective_album_id = await add_existing_media_to_album(photo_id, album_id, folder_name)
                                state[folder_name]['album_id'] = effective_album_id
                                log_warn(f"✅ Successfully retried file using existing media ID: {file_name}")
                                failures[error_type][folder_name]["files"].remove(file_entry)
                                if not failures[error_type][folder_name]["files"]:
                                    del failures[error_type][folder_name]
                                save_json(FAILED_FILE, failures)
                                continue
                            except Exception as e:
                                log_error(f"Error adding existing media item {file_name}: {str(e)}", exc_info=True)
                                log_warn(f"❌ Failed to retry file using existing media ID: {file_name}")
                                continue
                        
                        elif upload_token:
                            # Try to add using upload token (if photo_id is not available)
                            try:
                                photo_id, effective_album_id = await add_to_album(upload_token, album_id, file_name, folder_name)
                                state[folder_name]['album_id'] = effective_album_id
                                log_warn(f"✅ Successfully retried file using upload token: {file_name}")
                                failures[error_type][folder_name]["files"].remove(file_entry)
                                if not failures[error_type][folder_name]["files"]:
                                    del failures[error_type][folder_name]
                                save_json(FAILED_FILE, failures)
                                continue
                            except Exception as e:
                                log_error(f"Error processing file {file_name}: {str(e)}", exc_info=True)
                                log_warn(f"❌ Failed to retry file using upload token: {file_name}")
                                continue
                        else:
                            log_warn(f"❌ No photo_id or upload_token found for {file_name}, skipping")
                            continue
                    else:
                        log_warn(f"❌ File entry not found for {file_name}, skipping")
                        continue
                
                # Below is only for UploadError branch; now build the path
                file = folder_path / file_name

                # Skip macOS hidden/metadata files
                if file.name.startswith('.'):
                    failures[error_type][folder_name]["files"].remove(file_name)
                    if not failures[error_type][folder_name]["files"]:
                        del failures[error_type][folder_name]
                    save_json(FAILED_FILE, failures)
                    continue

                log_warn(f"[DEBUG] File extension: {file.suffix} (lowercase: {file.suffix.lower()})")
                # Move genuinely empty LOCAL files to _TOOSMALL.
                # Cloud files reporting 0 bytes are un-hydrated placeholders — leave them
                # in failed_uploads and let the upload attempt trigger the download.
                if file.exists() and os.path.getsize(file) == 0:
                    if "/Library/CloudStorage/" not in str(file):
                        log_warn(f"[TOOSMALL] Empty local file in retry: {file_name} - moving to _TOOSMALL")
                        move_to_toosmall(file, folder_name)
                        failures[error_type][folder_name]["files"].remove(file_name)
                        if not failures[error_type][folder_name]["files"]:
                            del failures[error_type][folder_name]
                        save_json(FAILED_FILE, failures)
                        continue
                    # Cloud placeholder: proceed to upload which will trigger hydration

                # Skip files with unsupported media types (use same check as main path)
                if not is_supported_media(file):
                    mime_type, _ = mimetypes.guess_type(str(file))
                    log_warn(f"❌ Unsupported media type: {file_name} (MIME: {mime_type or 'unknown'}) - skipping")
                    failures[error_type][folder_name]["files"].remove(file_name)
                    if not failures[error_type][folder_name]["files"]:
                        del failures[error_type][folder_name]
                    save_json(FAILED_FILE, failures)
                    continue

                try:
                    upload_token = await upload_file(str(file))
                    photo_id, effective_album_id = await add_to_album(upload_token, album_id, file.name, folder_name)
                    state[folder_name]['album_id'] = effective_album_id
                    log_warn(f"✅ Successfully retried file: {file_name}")
                    failures[error_type][folder_name]["files"].remove(file_name)
                    if not failures[error_type][folder_name]["files"]:
                        del failures[error_type][folder_name]
                    save_json(FAILED_FILE, failures)
                except Exception as e:
                    log_error(f"Error processing file {file_name}: {str(e)}", exc_info=True)
                    log_warn(f"❌ Failed to retry file: {file_name}")

    log_warn("Retry process completed. Exiting.")

async def interruptible_sleep(total_seconds: int, step: float = 0.5):
    """
    Sleep that wakes up frequently to check shutdown signal.
    Allows Ctrl+C to interrupt immediately instead of waiting for sleep to complete.
    """
    remaining = total_seconds
    while remaining > 0:
        if shutdown_handler.check():
            raise KeyboardInterrupt()
        await asyncio.sleep(min(step, remaining))
        remaining -= step

HYDRATION_MAX_WAIT_SECS = 600   # 10 minutes max wait for Google Drive to download
HYDRATION_POLL_SECS = 10        # poll every 10 seconds

def stage_local_copy_if_cloud(path: Path) -> Path:
    """
    If file is on Google Drive CloudStorage, copy to temp locally first.
    If the file is a 0-byte placeholder (not yet downloaded), waits up to
    HYDRATION_MAX_WAIT_SECS for Drive to hydrate it before copying.
    Raises EmptyCloudFileError if the file is still 0 bytes after the wait.
    """
    p = str(path)
    if "/Library/CloudStorage/" not in p:
        return path

    path_sig = hashlib.sha1(str(path).encode()).hexdigest()[:8]
    dst = Path(tempfile.gettempdir()) / "gphotos_stage" / f"{path_sig}_{path.name}"
    dst.parent.mkdir(parents=True, exist_ok=True)

    try:
        log_warn(f"[STAGE] CloudStorage file detected, staging locally: {path.name}")

        st = os.stat(path)
        src_size = st.st_size
        log_warn(f"[STAGE] Source size (stat): {src_size} bytes")

        # If 0 bytes, trigger download and wait for Drive to hydrate the file
        if src_size == 0:
            log_warn(f"[STAGE] File is placeholder (0 bytes), waiting up to {HYDRATION_MAX_WAIT_SECS}s for Drive to download...")
            # Opening the file triggers Google Drive to start the download
            try:
                with open(path, 'rb') as f:
                    f.read(1)
            except Exception:
                pass

            waited = 0
            while waited < HYDRATION_MAX_WAIT_SECS:
                if shutdown_handler.check():
                    raise KeyboardInterrupt()
                time.sleep(HYDRATION_POLL_SECS)
                waited += HYDRATION_POLL_SECS
                src_size = os.path.getsize(path)
                if src_size > 0:
                    log_warn(f"[STAGE] File downloaded: {format_size(src_size)} (after {waited}s)")
                    st = os.stat(path)  # refresh for mtime
                    break
                if waited % 60 == 0:
                    log_warn(f"[STAGE] Still waiting for download... {waited}s / {HYDRATION_MAX_WAIT_SECS}s")
            else:
                # Could be a network/disk issue, not a genuinely empty file → retry later
                raise NonRetryableError(f"File still 0 bytes after {HYDRATION_MAX_WAIT_SECS}s wait (network or disk issue?)")

        # Streaming copy forces full download/hydration
        with open(path, "rb") as src, open(dst, "wb") as out:
            shutil.copyfileobj(src, out, length=1024 * 1024)

        dst_size = dst.stat().st_size
        log_warn(f"[STAGE] Staged size: {dst_size} bytes")

        if dst_size == 0:
            raise EmptyCloudFileError("Staged file is 0 bytes after copy: file is empty in Google Drive")

        # Restore timestamps so Google Photos uses the original date
        os.utime(dst, (st.st_atime, st.st_mtime))

        log_warn(f"[STAGE] Staged successfully: {dst.name}")
        return dst

    except (EmptyCloudFileError, KeyboardInterrupt):
        raise  # propagate directly, no fallback
    except Exception as e:
        log_error(f"[STAGE] Failed to stage file: {str(e)}", exc_info=True)
        log_warn("[STAGE] Falling back to original path (may timeout)")
        return path

def cleanup_staged_file(path: Path):
    """Remove temporary staged file if it exists"""
    stage_root = Path(tempfile.gettempdir()) / "gphotos_stage"
    try:
        if stage_root in path.parents:
            path.unlink()
            log_warn(f"[STAGE] Cleaned up: {path.name}")
    except Exception as e:
        log_warn(f"[STAGE] Could not remove temp file: {e}")


async def process_file(file: Path, folder_name: str, album_id: str, folder_path: Path):
    log_warn(f"Processing file: {file}")
    global total_uploaded, total_failed

    # Skip files that are not in the target directory
    if not str(file).startswith(str(folder_path)):
        log_warn(f"Skipping file outside target directory: {file}")
        return

    # Skip files already marked as too large in this session
    if file.name in too_large_files_in_session:
        log_warn(f"⏭️  Skipping {file.name} (already marked as too large)")
        return

    # Check if file is already in state
    files = set(state.get(folder_name, {}).get('files', []))
    file_already_processed = file.name in files

    if file_already_processed:
        if DRY_RUN:
            log_warn(f"[DRY-RUN] Skipping already processed file: {file.name}")
        return

    # CHECK MEDIA TYPE - skip unsupported formats BEFORE upload
    if not is_supported_media(file):
        mime_type, _ = mimetypes.guess_type(str(file))
        log_warn(f"❌ Unsupported media type: {file.name} (MIME: {mime_type or 'unknown'}) - moving to _UNSUPPORTED")
        if DRY_RUN:
            log_warn(f"[DRY-RUN] Would move {file.name} → ../{UNSUPPORTED_DIR_NAME}/{folder_name}/")
        else:
            move_to_unsupported(file, folder_name)
        total_failed += 1
        return

    # CHECK FILE SIZE BEFORE ATTEMPTING UPLOAD - avoid retry decorator
    file_size = os.path.getsize(file)
    max_size = 10 * 1024 * 1024 * 1024  # 10 GB

    if file_size == 0:
        if "/Library/CloudStorage/" in str(file):
            # Size is 0 because the file is a cloud placeholder not yet downloaded.
            # Proceed to staging: opening the file triggers macOS to hydrate it from Drive.
            # If it's still 0 after staging, NonRetryableError stops the retry immediately.
            log_warn(f"[CLOUD] {file.name} reports 0 bytes (placeholder) - proceeding to staging to trigger download")
        else:
            # Genuinely empty local file — move out of the way
            log_warn(f"[TOOSMALL] Empty local file (0 bytes): {file.name}")
            if DRY_RUN:
                log_warn(f"[DRY-RUN] Would move {file.name} → ../{TOOSMALL_DIR_NAME}/{folder_name}/")
            else:
                move_to_toosmall(file, folder_name)
            total_failed += 1
            return

    if file_size > max_size:
        log_warn(f"❌ File troppo grande: {file.name} ({format_size(file_size)}) - skipping")
        too_large_files_in_session.add(file.name)
        add_failure("TooLarge", folder_name, file.name, folder_path)
        total_failed += 1
        return

    if FIX_DATES:
        force_file_download(file)
        # Update media dates (now handles both EXIF and QuickTime tags)
        if is_supported_media(file):
            if DRY_RUN:
                log_warn(f"[DRY-RUN] Would check and update media dates for: {file.name}")
            else:
                update_media_dates_if_mismatch(str(file), folder_name)
        else:
            if DRY_RUN:
                log_warn(f"[DRY-RUN] Would skip media operations for unsupported format: {file.name}")
            else:
                log_warn(f"❌ Skip media operations for unsupported format: {file.name}")

        # Filesystem timestamp update for all files
        if DRY_RUN:
            log_warn(f"[DRY-RUN] Would check and update filesystem timestamp for: {file.name}")
            update_filesystem_date_if_mismatch(file, folder_name)
        else:
            update_filesystem_date_if_mismatch(file, folder_name)

    # Upload attempt for all files
    if DRY_RUN:
        log_warn(f"[DRY-RUN] Would upload {file.name} → album {folder_name}")
        return

    try:
        log_warn(f"[UPLOAD] Attempting to upload file: {file.name}")
        # Stage CloudStorage files locally first to avoid provider stalls
        local_file = stage_local_copy_if_cloud(file)
        try:
            upload_token = await upload_file(str(local_file))
            log_warn(f"✅ Uploaded {file.name} to {folder_name}")
            # CRITICAL: Check if shutdown requested before proceeding to album operations
            if shutdown_handler.check():
                raise KeyboardInterrupt()
        finally:
            # Clean up staged copy if it was created
            if local_file != file:
                cleanup_staged_file(local_file)
    except EmptyCloudFileError:
        log_warn(f"[TOOSMALL] Cloud file confirmed empty after download wait: {file.name}")
        if DRY_RUN:
            log_warn(f"[DRY-RUN] Would move {file.name} → ../{TOOSMALL_DIR_NAME}/{folder_name}/")
        else:
            move_to_toosmall(file, folder_name)
        total_failed += 1
        return
    except KeyboardInterrupt:
        log_warn(f"[SHUTDOWN] Interrupted during upload cleanup for {file.name}")
        raise
    except Exception as e:
        log_error(f"❌ Upload error for '{file}': {str(e)}", exc_info=True)
        add_failure("UploadError", folder_name, file.name, folder_path)
        total_failed += 1
        return False

    try:
        log_warn(f"[ALBUM] Attempting to add {file.name} to album {folder_name}")
        # Check shutdown before proceeding with album operations
        if shutdown_handler.check():
            raise KeyboardInterrupt()
        # Get the photo ID and effective album_id from the add_to_album response
        photo_id, effective_album_id = await add_to_album(upload_token, album_id, file.name, folder_name)
        # Align state with the effective album_id (in case it was recreated)
        state[folder_name]['album_id'] = effective_album_id
        # Save state only AFTER album add succeeds
        state[folder_name]['files'].append(file.name)
        save_json(STATE_FILE, state)
        logging.info(f"✅ {file.name} → {folder_name}")
        total_uploaded += 1
        return True
    except KeyboardInterrupt:
        log_warn(f"[SHUTDOWN] Interrupted during album operations for {file.name}")
        raise
    except Exception as e:
        log_error(f"❌ Album error for '{file}': {str(e)}", exc_info=True)
        add_failure("AddToAlbumError", folder_name, file.name, folder_path, album_id=album_id, upload_token=upload_token)
        total_failed += 1
        return False

async def main():
    log_warn("🔍 Scanning directory...")
    global total_uploaded, total_failed
    total_uploaded = 0
    total_failed = 0
    
    for folder_path in Path(PHOTO_ROOT_DIR).iterdir():
        # Check for shutdown signal
        if shutdown_handler.check():
            log_warn("[MAIN] Shutdown requested, stopping main process...")
            break
        
        if not folder_path.is_dir():
            continue
            
        folder_name = folder_path.name
        
        # Get or create album
        album_id = None
        if folder_name in state:
            album_id = state[folder_name].get("album_id")
            
        if not album_id:
            try:
                # Avoid duplicates: search existing album by title before creating
                album_id = search_album_by_name(folder_name) or create_album(folder_name)
                state[folder_name] = {
                    'album_id': album_id,
                    'path': str(folder_path.resolve()),
                    'files': []
                }
                save_json(STATE_FILE, state)
            except Exception as e:
                log_error(f"Error creating album: {e}", exc_info=True)
                continue
                
        # Process files in folder
        for file in folder_path.iterdir():
            log_warn("\n" + "=" * 120)
            log_warn(f"[FILE] Processing new file: {file}")
            log_warn("=" * 120)
            # Check for shutdown signal
            if shutdown_handler.check():
                log_warn("[MAIN] Shutdown requested, stopping file processing...")
                break
            
            if file.is_file():
                # Skip macOS hidden/metadata files (.DS_Store, ._*, etc.)
                if file.name.startswith('.'):
                    continue

                # IMPORTANT: reload album_id in case it was repaired during a previous file
                album_id = state.get(folder_name, {}).get("album_id")
                if not album_id:
                    log_warn(f"[MAIN] Album ID missing for {folder_name}, skipping file {file.name}")
                    continue
                await process_file(file, folder_name, album_id, folder_path)
                
    log_warn("\n✅ Elaborazione completata.")
    log_warn(f"📸 File caricati con successo: {total_uploaded}")
    log_warn(f"❌ File falliti: {total_failed} (vedi '{FAILED_FILE}')")
    logging.info(f"✔️ Fine script: successi={total_uploaded}, fallimenti={total_failed}")

if __name__ == "__main__":
    try:
        if RETRY_FAILED:
            asyncio.run(retry_failed())
        else:
            asyncio.run(main())
    except KeyboardInterrupt:
        log_warn("\n[INTERRUPTED] Script interrupted by user")
    except asyncio.CancelledError:
        log_warn("\n[CANCELLED] Async operations cancelled")
    except Exception as e:
        log_error(f"[ERROR] Unexpected error: {str(e)}", exc_info=True)
    finally:
        try:
            if session:
                session.close()
                log_warn("[CLEANUP] Session closed")
        except Exception as e:
            log_warn(f"[CLEANUP] Error closing session: {e}")
        log_warn("[SHUTDOWN] Script terminated")

