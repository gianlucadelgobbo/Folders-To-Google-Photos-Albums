#!/usr/bin/env python3

import os
import json
import time
import logging
import sys
import warnings
from pathlib import Path
from datetime import datetime
import argparse
import subprocess
import re
from typing import Dict, List, Optional, Tuple, Set

# Suppress urllib3 SSL warnings
warnings.filterwarnings('ignore', category=Warning)

def log_init(msg):
    print(msg, flush=True)
    sys.stdout.flush()

log_init("[INIT] Script starting...")

SUPPORTED_EXIF_EXT = ('.jpg', '.jpeg', '.heic', '.heif', '.cr2', '.tif', '.tiff', '.mov', '.mp4', '.nef', '.flv', '.avi', '.m4v', '.mgg', '.rw2')

# === CLI ===
log_init("[INIT] Setting up argument parser...")
parser = argparse.ArgumentParser(description="Check local folders against Google Photos albums")
parser.add_argument("--path", type=str, required=True, help="Absolute path to the folder to process")
parser.add_argument("--missing-files", action="store_true", help="Check for missing files in Google Photos")
parser.add_argument("--wrong-dates", action="store_true", help="Check for wrong dates based on folder structure")
parser.add_argument("--dry-run", action="store_true", help="Simulate all actions without modifying anything")

log_init("[INIT] Parsing arguments...")
try:
    args = parser.parse_args()
    log_init(f"[INIT] Arguments parsed: path={args.path}, missing_files={args.missing_files}, wrong_dates={args.wrong_dates}, dry_run={args.dry_run}")
except Exception as e:
    print(f"Error parsing arguments: {e}", file=sys.stderr)
    sys.exit(1)

PHOTO_ROOT_DIR = args.path
CHECK_MISSING_FILES = args.missing_files
CHECK_WRONG_DATES = args.wrong_dates
DRY_RUN = args.dry_run

# === CONFIG ===
log_init("[INIT] Loading configuration...")
LOG_FILE = "full_google_albums_and_files_PULL_missing.log"
STATE_FILE = 'upload_state.json'
ALBUM_CACHE_FILE = 'full_google_albums_and_files.json'
FAILED_FILE = 'failed_uploads.json'

# Initialize failures dictionary
failures = {
    "UploadError": {},
    "AddToAlbumError": {},
    "TooLarge": {},
    "ExifErrors": {},
    "UnsupportedFormat": {},
    "MissingInGooglePhotos": {},  # New category for files missing in Google Photos
    "ExtraInGooglePhotos": {}     # New category for extra files in Google Photos
}

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
    with open(path, 'w') as f:
        json.dump(data, f)

# === DATE HANDLING ===
def load_google_cache_by_album_id(path: str) -> Dict:
    data = load_json(path, {})
    # Expected structure from GET script:
    # { "timestamp": ..., "albums": { "<album_id>": { "title":..., "photos":[...] } } }
    if isinstance(data, dict) and "albums" in data and isinstance(data["albums"], dict):
        return data["albums"]
    # fallback: maybe already the albums dict
    if isinstance(data, dict):
        return data
    return {}

def extract_date_from_folder(folder_name: str) -> Optional[Tuple[int, int, int]]:
    """Extract date from folder name in format YYYY-MM or YYYY-MM-DD."""
    match = re.search(r'(\d{4})(?:[-_]?(\d{2}))?(?:[-_]?(\d{2}))?', folder_name)
    if not match:
        return None
        
    year = int(match.group(1))
    month = int(match.group(2)) if match.group(2) else None
    day = int(match.group(3)) if match.group(3) else None
    return (year, month, day)

def get_exif_datetimeoriginal_exiftool(file_path: Path) -> Optional[datetime]:
    """Get EXIF DateTimeOriginal using exiftool."""
    try:
        result = subprocess.run(
            ["exiftool", "-s", "-s", "-s", "-DateTimeOriginal", str(file_path)],
            capture_output=True, text=True, check=True, timeout=5
        )
        value = result.stdout.strip()
        if not value:
            return None
        return datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
    except Exception as e:
        log_warn(f"[EXIFTOOL-READ] Failed to read EXIF from {file_path}: {e}")
        return None

def check_file_date(file_path: Path, folder_name: str) -> bool:
    """Check if file date matches folder date structure."""
    folder_date = extract_date_from_folder(folder_name)
    if not folder_date:
        return True  # Skip if folder doesn't have a date structure
        
    exif_date = get_exif_datetimeoriginal_exiftool(file_path)
    if not exif_date:
        return True  # Skip if no EXIF date
        
    year, month, day = folder_date
    
    # Check year
    if year != exif_date.year:
        return False
        
    # Check month if specified
    if month is not None and month != exif_date.month:
        return False
        
    # Check day if specified
    if day is not None and day != exif_date.day:
        return False
        
    return True

def add_failure(error_type, folder_name, file_name, folder_path, album_id=None):
    """Add a failed upload to the failures record with album ID."""
    if folder_name not in failures[error_type]:
        failures[error_type][folder_name] = {
            "path": str(folder_path.resolve()),
            "album_id": album_id,
            "files": []
        }
    if file_name not in failures[error_type][folder_name]["files"]:
        failures[error_type][folder_name]["files"].append(file_name)
    save_json(FAILED_FILE, failures)

def remove_from_upload_state(state: Dict, folder_name: str, file_name: str) -> bool:
    """Remove file_name from upload_state[folder_name]['files'] if present."""
    info = state.get(folder_name)
    if not info:
        return False
    files = info.get("files")
    if not isinstance(files, list):
        return False
    if file_name in files:
        files.remove(file_name)
        return True
    return False

def check_missing_files(folder_path: Path, google_albums_by_id: Dict, state: Dict):
    folder_name = folder_path.name

    # local supported files
    local_files = {
        f.name for f in folder_path.iterdir()
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXIF_EXT
    }

    state_info = state.get(folder_name)
    if not state_info or not state_info.get("album_id"):
        log_warn(f"\nFolder {folder_name} has no album_id in upload_state.json -> skip (can't compare safely)")
        return

    album_id = state_info["album_id"]
    g_album = google_albums_by_id.get(album_id)
    if not g_album:
        log_warn(f"\nAlbum id {album_id} for folder {folder_name} not found in Google cache -> treat as missing-all")
        # everything is missing on google -> remove from state files (optional) + optionally add to failed
        for fn in sorted(local_files):
            if not DRY_RUN:
                removed = remove_from_upload_state(state, folder_name, fn)
                if removed:
                    log_warn(f"[STATE] Removed from upload_state: {folder_name}/{fn}")
            # Optional: if you want retry-failed
            # add_failure("MissingInGooglePhotos", folder_name, fn, folder_path, album_id)
        return

    # google filenames from cache
    photos = g_album.get("photos", [])
    gphotos_files = {p.get("filename") for p in photos if isinstance(p, dict) and p.get("filename")}
    gphotos_files.discard(None)

    log_warn(f"\nFile count for {folder_name}:")
    log_warn(f"  - Local files: {len(local_files)}")
    log_warn(f"  - Google Photos (cached): {len(gphotos_files)}")
    log_warn(f"  - Difference: {len(local_files) - len(gphotos_files)}")

    missing_in_gphotos = local_files - gphotos_files
    if missing_in_gphotos:
        log_warn(f"\nMissing in Google Photos for {folder_name}:")
        for fn in sorted(missing_in_gphotos):
            log_warn(f"  - {fn}")
            if not DRY_RUN:
                removed = remove_from_upload_state(state, folder_name, fn)
                if removed:
                    log_warn(f"[STATE] Removed from upload_state: {folder_name}/{fn}")
                # Optional: if you want retry-failed
                # add_failure("MissingInGooglePhotos", folder_name, fn, folder_path, album_id)
    else:
        log_warn(f"No missing files in Google Photos for {folder_name}.")

    extra_in_gphotos = gphotos_files - local_files
    if extra_in_gphotos:
        log_warn(f"\nExtra files in Google Photos for {folder_name} (not found locally):")
        for fn in sorted(extra_in_gphotos):
            log_warn(f"  - {fn}")
            # Optional only (doesn't affect reupload)
            # add_failure("ExtraInGooglePhotos", folder_name, fn, folder_path, album_id)
    else:
        log_warn(f"No extra files in Google Photos for {folder_name}.")
def check_wrong_dates(folder_path: Path):
    """Check for files with dates that don't match folder structure."""
    wrong_dates = []
    
    for file in folder_path.iterdir():
        if not file.is_file() or file.suffix.lower() not in SUPPORTED_EXIF_EXT:
            continue
            
        if not check_file_date(file, folder_path.name):
            wrong_dates.append(file.name)
            
    if wrong_dates:
        log_warn(f"\nFiles with wrong dates in {folder_path.name}:")
        for file in sorted(wrong_dates):
            log_warn(f"  - {file}")
    else:
        log_warn(f"\nNo wrong dates in {folder_path.name}")

def main():
    google_albums_by_id = load_google_cache_by_album_id(ALBUM_CACHE_FILE)
    state = load_json(STATE_FILE, {})

    photo_root = Path(PHOTO_ROOT_DIR)
    for folder in sorted(photo_root.iterdir()):
        if not folder.is_dir():
            continue

        log_warn(f"\nProcessing folder: {folder.name}")

        if CHECK_MISSING_FILES:
            check_missing_files(folder, google_albums_by_id, state)

        if CHECK_WRONG_DATES:
            check_wrong_dates(folder)

    if CHECK_MISSING_FILES and not DRY_RUN:
        save_json(STATE_FILE, state)
        log_warn(f"[STATE] upload_state.json updated after missing check")

if __name__ == "__main__":
    main() 