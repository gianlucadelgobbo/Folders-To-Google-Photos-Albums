# usage: python3 gphotos_uploader.py --path "/your/folder"
# usage: python3 gphotos_uploader.py --path "/your/folder" --retry-failed --listener

import os
import json
import time
import logging
import sys
import warnings
from tqdm import tqdm
from pathlib import Path
from tenacity import retry, wait_fixed, stop_after_attempt
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import AuthorizedSession
import argparse
import subprocess
import re
from datetime import datetime
import gc
from typing import Tuple, Optional
import urllib.parse

# Suppress urllib3 SSL warnings
warnings.filterwarnings('ignore', category=Warning)

def log_init(msg):
    print(msg, flush=True)
    sys.stdout.flush()

log_init("[INIT] Script starting...")

SUPPORTED_EXIF_EXT = ('.jpg', '.jpeg', '.heic', '.heif', '.cr2', '.tif', '.tiff', '.mov', '.mp4', '.nef')

# === CLI ===
log_init("[INIT] Setting up argument parser...")
parser = argparse.ArgumentParser(description="Uploader per Google Photos")
parser.add_argument("--path", type=str, required=True, help="Absolute path to the folder to process")
parser.add_argument("--retry-failed", action="store_true", help="Retry files listed in failed_uploads.json")
parser.add_argument("--listener", action="store_true", help="Continuously watch 'ExifErrors' and upload as they appear")
parser.add_argument("--update-exif-from-folder-if-mismatch", action="store_true", help="Fix EXIF date using folder name")
parser.add_argument("--dry-run", action="store_true", help="Simulate all actions without uploading or modifying anything")

log_init("[INIT] Parsing arguments...")
try:
    args = parser.parse_args()
    log_init(f"[INIT] Arguments parsed: path={args.path}, retry_failed={args.retry_failed}, listener={args.listener}, update_exif={args.update_exif_from_folder_if_mismatch}, dry_run={args.dry_run}")
except Exception as e:
    print(f"Error parsing arguments: {e}", file=sys.stderr)
    sys.exit(1)

PHOTO_ROOT_DIR = args.path
RETRY_FAILED = args.retry_failed
LISTENER_MODE = args.listener
UPDATE_FROM_FOLDER_DATE = args.update_exif_from_folder_if_mismatch
DRY_RUN = args.dry_run

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

# Cache for albums
album_cache = {}

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
@retry(wait=wait_fixed(5), stop=stop_after_attempt(5))
def create_album(title):
    log_warn(f"[ALBUM] Creating album: {title}")
    body = {"album": {"title": title[:100]}}
    try:
        log_warn(f"[ALBUM] Sending request to create album: {title}")
        response = session.post("https://photoslibrary.googleapis.com/v1/albums", json=body)
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
    
    # 1. First check upload_state.json
    if title in state:
        album_id = state[title].get('album_id')
        if album_id:
            log_warn(f"[ALBUM] Found album '{title}' in upload_state.json (id: {album_id})")
            return album_id
    
    # 2. Then check cache
    if title in album_cache:
        log_warn(f"[ALBUM] Found album '{title}' in cache (id: {album_cache[title]})")
        return album_cache[title]
    
    # 3. If not found, create cache and search
    log_warn(f"[ALBUM] Building album cache...")
    try:
        page_token = None
        while True:
            url = "https://photoslibrary.googleapis.com/v1/albums"
            if page_token:
                url += f"?pageToken={page_token}"
                
            response = session.get(url)
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

@retry(wait=wait_fixed(5), stop=stop_after_attempt(5))
def upload_file(file_path):
    file_size = os.path.getsize(file_path)
    max_size = 10 * 1024 * 1024 * 1024  # 10 GB

    folder_name = Path(file_path).parent.name
    file_name = Path(file_path).name

    log_warn(f"[UPLOAD] Starting upload of {file_name} ({file_size} bytes)")

    if file_size > max_size:
        log_warn(f"❌ File troppo grande: {file_name} ({file_size} bytes)")
        add_failure("TooLarge", folder_name, file_name, Path(file_path).parent)
        raise Exception(f"File too large: {file_size} > 10GB")

    headers = {
        'Content-Type': 'application/octet-stream',
        'X-Goog-Upload-File-Name': file_name,
        'X-Goog-Upload-Protocol': 'raw',
    }

    try:
        log_warn(f"[UPLOAD] Opening file for reading: {file_path}")
        with open(file_path, 'rb') as f:
            log_warn(f"[UPLOAD] Sending file to Google Photos API...")
            try:
                response = session.post(
                    "https://photoslibrary.googleapis.com/v1/uploads",
                    data=f,
                    headers=headers,
                    timeout=360
                )
                log_warn(f"[UPLOAD] Got response from API: {response.status_code}")
                
                if response.status_code != 200:
                    error_msg = f"[UPLOAD] Error response from API: {response.status_code} - {response.text}"
                    log_error(error_msg)
                    raise Exception(error_msg)
                    
                log_warn(f"[UPLOAD] Successfully uploaded {file_name}")
                return response.text
            except Exception as e:
                log_error(f"[UPLOAD] Error during API request: {str(e)}", exc_info=True)
                raise
    except Exception as e:
        log_error(f"[UPLOAD] Failed to upload {file_name}: {str(e)}", exc_info=True)
        raise

@retry(wait=wait_fixed(5), stop=stop_after_attempt(5))
def add_to_album(upload_token, album_id, description, folder_name):
    log_warn(f"[ALBUM] Adding photo to album {album_id}: {folder_name}")
    body = {
        'albumId': album_id,
        'newMediaItems': [{
            'description': description,
            'simpleMediaItem': {'uploadToken': upload_token}
        }]
    }
    try:
        log_warn(f"[ALBUM] Sending request to add photo to album")
        response = session.post("https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate", json=body)
        
        # Handle rate limiting
        if response.status_code == 429:
            log_warn("[ALBUM] Rate limit exceeded, waiting 65 seconds before retry...")
            time.sleep(65)  # Wait for 65 seconds (slightly more than 1 minute)
            raise Exception("Rate limit exceeded, retrying after delay")
            
        if response.status_code == 404 and "The provided ID does not match any albums" in response.text:
            # Album no longer exists, remove it from state and create a new one
            log_warn(f"[ALBUM] Album {album_id} no longer exists, removing from state and creating new album for {folder_name}")
            
            # Remove old album from state
            if folder_name in state:
                del state[folder_name]
                save_json(STATE_FILE, state)
            
            # Create new album
            new_album_id = create_album(folder_name)
            # Update state with new album ID
            state[folder_name] = {
                'album_id': new_album_id,
                'path': str(Path(description).parent),
                'files': []
            }
            save_json(STATE_FILE, state)
            # Retry with new album ID
            body['albumId'] = new_album_id
            response = session.post("https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate", json=body)
            
        if response.status_code != 200:
            log_error(f"[ALBUM] Error response from API: {response.status_code} - {response.text}")
            raise Exception(f"Error adding to album: {response.text}")
            
        # Check if the upload was successful
        result = response.json()
        if 'newMediaItemResults' in result:
            for item in result['newMediaItemResults']:
                if 'status' in item:
                    status = item['status']
                    if status.get('code') != 0:
                        error_msg = status.get('message', 'Unknown error')
                        log_error(f"[ALBUM] Failed to add media item: {error_msg}")
                        raise Exception(f"Failed to add media item: {error_msg}")
                    else:
                        # Success case
                        log_warn(f"[ALBUM] Successfully added photo to album: {description}")
                        return True
                    
        log_warn(f"[ALBUM] Successfully added photo to album: {description}")
        return True
    except Exception as e:
        log_error(f"[ALBUM] Failed to add photo to album: {str(e)}")
        raise

# === FAILURE HANDLING ===
def add_failure(error_type, folder_name, file_name, folder_path):
    if folder_name not in failures[error_type]:
        failures[error_type][folder_name] = {
            "path": str(folder_path.resolve()),
            "files": []
        }
    if file_name not in failures[error_type][folder_name]["files"]:
        failures[error_type][folder_name]["files"].append(file_name)
    save_json(FAILED_FILE, failures)

# === UPDATE_FROM_FOLDER_DATE ===

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

# === LISTENER ===
def process_exif_errors_loop():
    log_warn("🔄 Listening for new files in 'ExifErrors'...")
    already_uploaded = set()

    while True:
        failures = load_json(FAILED_FILE, {})
        exif_errors = failures.get("ExifErrors", {})

        for folder_name, entry in list(exif_errors.items()):
            folder_path = Path(entry.get("path"))
            file_list = entry.get("files", [])

            if not folder_path.exists():
                log_warn(f"❌ Folder not found: {folder_path}")
                continue

            album_id = state.get(folder_name, {}).get("album_id")
            if DRY_RUN:
                log_warn(f"[DRY-RUN] Would create album '{folder_name}' (retry mode)")
                log_warn(f"[DEBUG] Listing files in folder: {folder_path}")
                for f in folder_path.iterdir():
                    log_warn(f"[DEBUG] Found file: {f}")
                continue
            if not album_id:
                try:
                    album_id = create_album(folder_name)
                    state[folder_name] = {
                        'album_id': album_id,
                        'path': str(folder_path.resolve()),
                        'files': []
                    }
                    save_json(STATE_FILE, state)
                except Exception as e:
                    log_error(f"Errore creazione album (listener): {e}", exc_info=True)
                    continue

            for file_name in file_list[:]:
                if (folder_name, file_name) in already_uploaded:
                    continue

                file_path = folder_path / file_name
                if not file_path.is_file():
                    log_warn(f"❌ File not found: {file_path}")
                    continue

                try:
                    upload_token = upload_file(str(file_path))
                    add_to_album(upload_token, album_id, file_name, folder_name)
                    state[folder_name]['files'].append(file_name)
                    already_uploaded.add((folder_name, file_name))
                    save_json(STATE_FILE, state)
                    failures["ExifErrors"][folder_name]["files"].remove(file_name)
                    if not failures["ExifErrors"][folder_name]["files"]:
                        del failures["ExifErrors"][folder_name]
                    save_json(FAILED_FILE, failures)
                    log_warn(f"✅ Uploaded {file_name} from {folder_name}")
                except Exception as e:
                    log_error(f"Upload error: {e}", exc_info=True)

        time.sleep(10)

def process_file(file: Path, folder_name: str, album_id: str, folder_path: Path, already_uploaded: bool = False):
    log_warn(f"Processing file: {file}")
    global total_uploaded, total_failed

    # Skip files that are not in the target directory
    if not str(file).startswith(str(folder_path)):
        log_warn(f"Skipping file outside target directory: {file}")
        return

    # Check if file is already in state
    files = set(state.get(folder_name, {}).get('files', []))
    file_already_processed = file.name in files

    if file_already_processed:
        if DRY_RUN:
            log_warn(f"[DRY-RUN] Skipping already processed file: {file.name}")
        return

    if UPDATE_FROM_FOLDER_DATE:
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
        upload_token = upload_file(str(file))
        # Save state immediately after successful upload
        state[folder_name]['files'].append(file.name)
        save_json(STATE_FILE, state)
        log_warn(f"✅ Uploaded {file.name} to {folder_name}")
    except Exception as e:
        log_error(f"❌ Upload error for '{file}': {str(e)}", exc_info=True)
        add_failure("UploadError", folder_name, file.name, folder_path)
        total_failed += 1
        return False

    try:
        log_warn(f"[ALBUM] Attempting to add {file.name} to album {folder_name}")
        add_to_album(upload_token, album_id, file.name, folder_name)
        logging.info(f"✅ {file.name} → {folder_name}")
        total_uploaded += 1
        return True
    except Exception as e:
        log_error(f"❌ Album error for '{file}': {str(e)}", exc_info=True)
        add_failure("AddToAlbumError", folder_name, file.name, folder_path)
        total_failed += 1
        return False


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
        log_warn(f"")
        log_warn(f"")
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

# === MAIN ===
total_uploaded = 0
total_failed = 0

if RETRY_FAILED:
    log_warn("🔁 Modalità retry: elaborazione file falliti da failed_uploads.json...\n")
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
                    album_id = create_album(folder_name)
                    state[folder_name] = {
                        'album_id': album_id,
                        'path': str(folder_path.resolve()),
                        'files': []
                    }
                    save_json(STATE_FILE, state)
                except Exception as e:
                    log_error(f"Errore creazione album retry: {e}", exc_info=True)
                    continue

            for file_name in file_list[:]:
                file = folder_path / file_name
                log_warn(f"[RETRY] Processing file: {file_name}")
                log_warn(f"[DEBUG] File extension: {file.suffix} (lowercase: {file.suffix.lower()})")
                
                # Skip files with unsupported extensions
                if file.suffix.lower() not in SUPPORTED_EXIF_EXT:
                    log_warn(f"❌ Skipping file with unsupported extension: {file_name}")
                    failures[error_type][folder_name]["files"].remove(file_name)
                    if not failures[error_type][folder_name]["files"]:
                        del failures[error_type][folder_name]
                    continue

                if process_file(file, folder_name, album_id, folder_path):
                    log_warn(f"✅ Successfully retried file: {file_name}")
                    failures[error_type][folder_name]["files"].remove(file_name)
                    if not failures[error_type][folder_name]["files"]:
                        del failures[error_type][folder_name]
                else:
                    log_warn(f"❌ Failed to retry file: {file_name}")

    save_json(FAILED_FILE, failures)

    if LISTENER_MODE:
        process_exif_errors_loop()

else:
    photo_root = Path(PHOTO_ROOT_DIR)
    log_warn(f"Starting main loop with root: {photo_root}")
    for folder in tqdm(sorted(photo_root.iterdir())):
        log_warn(f"[DEBUG] folder: {folder} (type: {type(folder)})")
        log_warn(f"Checking folder: {folder}")

        if not folder.is_dir():
            log_warn(f"Skipping non-directory: {folder}")
            continue

        folder_name = folder.name
        folder_path = Path(folder)
        log_warn(f"Processing folder: {folder_name} at {folder_path}")

        # Check if we need to create a new album
        if folder_name not in state:
            log_warn(f"[ALBUM] No existing album found for folder: {folder_name}")
            if DRY_RUN:
                log_warn(f"[DRY-RUN] Would create album '{folder_name}'")
            try:
                log_warn(f"[ALBUM] Creating new album for folder: {folder_name}")
                album_id = create_album(folder_name)
                state[folder_name] = {
                    'album_id': album_id,
                    'path': str(folder_path),
                    'files': []
                }
                save_json(STATE_FILE, state)
                log_warn(f"[ALBUM] Successfully created and saved album state for: {folder_name}")
            except Exception as e:
                log_error(f"Errore creazione album '{folder_name}': {e}", exc_info=True)
                continue
        else:
            album_id = state[folder_name]['album_id']
            folder_path = Path(state[folder_name]['path'])
            log_warn(f"[ALBUM] Using existing album for {folder_name} (id: {album_id})")

        files = set(state[folder_name].get('files', []))
        # Only process files in the target directory
        found_files = [f for f in folder_path.iterdir() if f.is_file() and str(f).startswith(str(folder_path))]
        
        # Processa un file alla volta
        for f in found_files:
            log_warn(f"[DEBUG] Checking file extension: {f.suffix} (lowercase: {f.suffix.lower()})")
            if f.suffix.lower() in SUPPORTED_EXIF_EXT:
                # Skip already processed files in both dry-run and normal mode
                if f.name in files:
                    if DRY_RUN:
                        log_warn(f"[DRY-RUN] Skipping already processed file: {f.name}")
                    continue
                    
                # Process file directly
                process_file(f, folder_name, album_id, folder_path, already_uploaded=False)
            else:
                log_warn(f"❌ File not valid EXIF format: {f.name}")
            
            # Forza la liberazione delle risorse dopo ogni file
            del f
            gc.collect()

# === REPORT ===
log_warn("\n✅ Elaborazione completata.")
log_warn(f"📸 File caricati con successo: {total_uploaded}")
log_warn(f"❌ File falliti: {total_failed} (vedi '{FAILED_FILE}')")
logging.info(f"✔️ Fine script: successi={total_uploaded}, fallimenti={total_failed}")
