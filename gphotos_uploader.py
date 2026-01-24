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
from tenacity import retry, wait_fixed, stop_after_attempt, wait_exponential
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import AuthorizedSession
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

# Suppress urllib3 SSL warnings
warnings.filterwarnings('ignore', category=Warning)

def log_init(msg):
    print(msg, flush=True)
    sys.stdout.flush()

log_init("[INIT] Script starting...")

SUPPORTED_EXIF_EXT = ('.jpg', '.jpeg', '.heic', '.heif', '.cr2', '.tif', '.tiff', '.mov', '.mp4', '.nef', '.flv','.avi','.m4v','.mgg','.swf','.rw2')

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
LOG_FILE = 'upload.log'
STATE_FILE = 'upload_state.json'
FAILED_FILE = 'failed_uploads.json'
CHUNK_SIZE_BYTES = 32768  # 32KB read size to keep socket active
TIMEOUT_COUNTS = {}  # per-file timeout counters for adaptive cooldown
SAFETY_BUFFER_BYTES = 500 * 1024 * 1024  # 500 MB safety buffer

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
def authenticate():
    try:
        log_warn("[AUTH] Starting authentication process...")
        if not os.path.exists(CREDENTIALS_FILE):
            log_error(f"[AUTH] Credentials file not found: {CREDENTIALS_FILE}")
            raise FileNotFoundError(f"Credentials file not found: {CREDENTIALS_FILE}")
            
        log_warn("[AUTH] Loading credentials file...")
        flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
        log_warn("[AUTH] Starting local server for OAuth flow...")
        creds = flow.run_local_server(port=0)
        log_warn("[AUTH] Successfully obtained credentials")
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
        is_valid = response.status_code == 200
        if not is_valid:
            log_warn(f"[ALBUM] Album ID validation failed: {album_id} (status {response.status_code})")
        return is_valid
    except Exception as e:
        log_warn(f"[ALBUM] Album ID validation error: {album_id} - {str(e)}")
        return False

@retry(wait=wait_fixed(5), stop=stop_after_attempt(5))
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

@retry(wait=wait_fixed(5), stop=stop_after_attempt(5))
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

@retry(wait=wait_exponential(multiplier=2, min=5, max=300), stop=stop_after_attempt(7))
async def upload_file(file_path):
    file_size = os.path.getsize(file_path)
    max_size = 10 * 1024 * 1024 * 1024  # 10 GB

    folder_name = Path(file_path).parent.name
    file_name = Path(file_path).name

    # Convert sizes to human readable format
    def format_size(size):
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.2f}{unit}"
            size /= 1024
        return f"{size:.2f}TB"

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
        #'X-Goog-Upload-File-Name': urllib.parse.quote(file_name),
        'X-Goog-Upload-File-Name': file_name,
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
                    await asyncio.sleep(retry_after)
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
                await asyncio.sleep(30)  # 30 second delay between uploads
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
                    await asyncio.sleep(cooldown)
                raise
    except Exception as e:
        log_error(f"[UPLOAD] Failed to upload {file_name}: {str(e)}", exc_info=True)
        log_warn(f"[UPLOAD] Raw exception: {repr(e)}")
        raise


    
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
        response = session.post("https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate", json=body)
        
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
            await asyncio.sleep(retry_after)
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
            response = session.post("https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate", json=body)
            
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
            response = session.post("https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate", json=body)
            
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
                log_warn(f"[ALBUM] Successfully added photo to album: {description}")
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
        response = session.post("https://photoslibrary.googleapis.com/v1/albums/{albumId}:batchAddMediaItems".format(albumId=album_id), json=body)
        
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
            await asyncio.sleep(retry_after)
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
            response = session.post("https://photoslibrary.googleapis.com/v1/albums/{albumId}:batchAddMediaItems".format(albumId=new_album_id), json=body)
            
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
            response = session.post("https://photoslibrary.googleapis.com/v1/albums/{albumId}:batchAddMediaItems".format(albumId=new_album_id), json=body)
            
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

def get_exif_datetimeoriginal_exiftool(file_path):
    try:
        result = subprocess.run(
            ["exiftool", "-s", "-s", "-s", "-DateTimeOriginal", file_path],
            capture_output=True, text=True, check=True, timeout=5
        )
        value = result.stdout.strip()
        if not value:
            return None
        return datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
    except Exception as e:
        log_warn(f"[EXIFTOOL-READ] Failed to read EXIF from {file_path}: {e}")
        return None

def update_exif_date_if_mismatch(file_path, folder_name):
    folder_info = extract_date_from_folder(folder_name)
    log_warn(f"Reading date from folder: {folder_info}")
    if not folder_info:
        return

    exif_dt = get_exif_datetimeoriginal_exiftool(file_path)
    log_warn(f"Reading exif date from file: {exif_dt}")
    if not exif_dt:
        # fallback to filesystem timestamp
        fs_dt = datetime.fromtimestamp(Path(file_path).stat().st_mtime)
        exif_dt = fs_dt

    new_dt = build_datetime_from_folder_info(exif_dt, folder_info)

    if new_dt != exif_dt:
        dt_str = new_dt.strftime("%Y:%m:%d %H:%M:%S")
        exif_str = exif_dt.strftime("%Y:%m:%d %H:%M:%S")
        if DRY_RUN:
            log_warn(f"[DRY-RUN] Would fix EXIF of {Path(file_path).name}: {exif_str} → {dt_str}")
        else:
            try:
                subprocess.run([
                    "exiftool",
                    "-overwrite_original",
                    f"-DateTimeOriginal={dt_str}",
                    f"-CreateDate={dt_str}",
                    f"-ModifyDate={dt_str}",
                    file_path
                ], check=True)
                log_warn(f"[FIXED] {Path(file_path).name} EXIF: {exif_str} → {dt_str}")
            except subprocess.CalledProcessError as e:
                add_failure("ExifErrors", folder_name, Path(file_path).name, Path(file_path).parent)
                log_warn(f"[EXIFTOOL-WRITE] Failed to update EXIF on {file_path}: {e}")
    else:
        log_warn(f"[EXIFTOOL] EXIF date is ok")

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
        log_warn(f"❌ DISASTRO")
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
    """Handle graceful shutdown on Ctrl+C"""
    def __init__(self):
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        log_warn(f"\n[SHUTDOWN] Received signal {signum}. Shutting down gracefully...")
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
                log_warn(f"[DEBUG] File extension: {file.suffix} (lowercase: {file.suffix.lower()})")
                # Skip files with unsupported extensions (for UploadError only)
                if file.suffix.lower() not in SUPPORTED_EXIF_EXT:
                    log_warn(f"❌ Skipping file with unsupported extension: {file_name}")
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

    # CHECK FILE SIZE BEFORE ATTEMPTING UPLOAD - avoid retry decorator
    file_size = os.path.getsize(file)
    max_size = 10 * 1024 * 1024 * 1024  # 10 GB
    
    def format_size(size):
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.2f}{unit}"
            size /= 1024
        return f"{size:.2f}TB"
    
    if file_size > max_size:
        log_warn(f"❌ File troppo grande: {file.name} ({format_size(file_size)}) - skipping")
        too_large_files_in_session.add(file.name)
        add_failure("TooLarge", folder_name, file.name, folder_path)
        total_failed += 1
        return

    if FIX_DATES:
        force_file_download(file)
        # EXIF operations only for supported formats
        if file.suffix.lower() in SUPPORTED_EXIF_EXT:
            if DRY_RUN:
                log_warn(f"[DRY-RUN] Would check and update EXIF date for: {file.name}")
                update_exif_date_if_mismatch(str(file), folder_name)
            else:
                update_exif_date_if_mismatch(str(file), folder_name)
        else:
            if DRY_RUN:
                log_warn(f"[DRY-RUN] Would skip EXIF operations for unsupported format: {file.name}")
            else:
                log_warn(f"❌ Skip EXIF operations for unsupported format: {file.name}")

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
        upload_token = await upload_file(str(file))
        log_warn(f"✅ Uploaded {file.name} to {folder_name}")
    except Exception as e:
        log_error(f"❌ Upload error for '{file}': {str(e)}", exc_info=True)
        add_failure("UploadError", folder_name, file.name, folder_path)
        total_failed += 1
        return False

    try:
        log_warn(f"[ALBUM] Attempting to add {file.name} to album {folder_name}")
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
            # Check for shutdown signal
            if shutdown_handler.check():
                log_warn("[MAIN] Shutdown requested, stopping file processing...")
                break
            
            if file.is_file():
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
# === REPORT ===
log_warn("\n✅ Elaborazione completata.")
log_warn(f"📸 File caricati con successo: {total_uploaded}")
log_warn(f"❌ File falliti: {total_failed} (vedi '{FAILED_FILE}')")
logging.info(f"✔️ Fine script: successi={total_uploaded}, fallimenti={total_failed}")

