# usage: python3 gphotos_uploader.py --path "/your/folder"
# usage: python3 gphotos_uploader.py --path "/your/folder" --retry-failed --listener

import os
import json
import time
import logging
from tqdm import tqdm
from pathlib import Path
from tenacity import retry, wait_fixed, stop_after_attempt
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import AuthorizedSession
import argparse
import subprocess
import re
from datetime import datetime

SUPPORTED_EXIF_EXT = ('.jpg', '.jpeg', '.heic', '.heif', '.cr2', '.tif', '.tiff', '.mov', '.mp4')

# === CLI ===
parser = argparse.ArgumentParser(description="Uploader per Google Photos")
parser.add_argument("--path", type=str, required=True, help="Absolute path to the folder to process")
parser.add_argument("--retry-failed", action="store_true", help="Retry files listed in failed_uploads.json")
parser.add_argument("--listener", action="store_true", help="Continuously watch 'ExifErrors' and upload as they appear")
parser.add_argument("--update-exif-from-folder-if-mismatch", action="store_true", help="Fix EXIF date using folder name")
parser.add_argument("--dry-run", action="store_true", help="Simulate all actions without uploading or modifying anything")

args = parser.parse_args()

PHOTO_ROOT_DIR = args.path
RETRY_FAILED = args.retry_failed
LISTENER_MODE = args.listener
UPDATE_FROM_FOLDER_DATE = args.update_exif_from_folder_if_mismatch

# === CONFIG ===
SCOPES = ['https://www.googleapis.com/auth/photoslibrary.appendonly']
CREDENTIALS_FILE = 'credentials.json'
LOG_FILE = 'upload.log'
STATE_FILE = 'upload_state.json'
FAILED_FILE = 'failed_uploads.json'

# === LOGGING ===
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_warn(msg):
    print(msg)
    logging.warning(msg)
    
def log_error(msg, exc_info=False):
    print(msg)
    if exc_info:
        logging.error(msg, exc_info=True)
    else:
        logging.error(msg)

if not os.path.isdir(PHOTO_ROOT_DIR):
    log_warn(f"❌ Invalid folder: {PHOTO_ROOT_DIR}")
    exit(1)

# === STATE HANDLING ===
def load_json(path, default):
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return default

def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

state = load_json(STATE_FILE, {})
failures = load_json(FAILED_FILE, {
    "UploadError": {},
    "AddToAlbumError": {},
    "TooLarge": {},
    "ExifErrors": {},
    "UnsupportedFormat": {}
})
# === AUTH ===
def authenticate():
    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
    creds = flow.run_local_server(port=0)
    return AuthorizedSession(creds)

session = authenticate()

# === API WRAPPERS ===
@retry(wait=wait_fixed(5), stop=stop_after_attempt(5))
def create_album(title):
    body = {"album": {"title": title[:100]}}
    response = session.post("https://photoslibrary.googleapis.com/v1/albums", json=body)
    if response.status_code != 200:
        raise Exception(f"Errore creazione album: {response.text}")
    album_id = response.json()["id"]
    logging.info(f"Album creato: {title} (id: {album_id})")
    return album_id

@retry(wait=wait_fixed(5), stop=stop_after_attempt(5))
def upload_file(file_path):
    file_size = os.path.getsize(file_path)
    max_size = 10 * 1024 * 1024 * 1024  # 10 GB

    folder_name = Path(file_path).parent.name
    file_name = Path(file_path).name

    if file_size > max_size:
        log_warn(f"❌ File troppo grande: {file_name} ({file_size} bytes)")
        add_failure("TooLarge", folder_name, file_name, Path(file_path).parent)
        raise Exception(f"File too large: {file_size} > 10GB")

    headers = {
        'Content-Type': 'application/octet-stream',
        'X-Goog-Upload-File-Name': file_name,
        'X-Goog-Upload-Protocol': 'raw',
    }

    with open(file_path, 'rb') as f:
        response = session.post(
            "https://photoslibrary.googleapis.com/v1/uploads",
            data=f,
            headers=headers,
            timeout=360
        )

    if response.status_code != 200:
        raise Exception(f"Errore upload file: {response.text}")
    return response.text

@retry(wait=wait_fixed(5), stop=stop_after_attempt(5))
def add_to_album(upload_token, album_id, description):
    body = {
        'albumId': album_id,
        'newMediaItems': [{
            'description': description,
            'simpleMediaItem': {'uploadToken': upload_token}
        }]
    }
    response = session.post("https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate", json=body)
    if response.status_code != 200:
        raise Exception(f"Errore add_to_album: {response.text}")

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
    date_from_folder = extract_date_from_folder(folder_name)
    if not date_from_folder:
        return

    exif_dt = get_exif_datetimeoriginal_exiftool(file_path)
    if not exif_dt:
        return

    y, m, d = date_from_folder
    new_dt = exif_dt

    if y and exif_dt.year != y:
        new_dt = new_dt.replace(year=y)
    if m and exif_dt.month != m:
        new_dt = new_dt.replace(month=m)
    if d and exif_dt.day != d:
        try:
            new_dt = new_dt.replace(day=d)
        except ValueError:
            new_dt = new_dt.replace(day=1)  # fallback

    if new_dt != exif_dt:
        dt_str = new_dt.strftime("%Y:%m:%d %H:%M:%S")
        try:
            subprocess.run([
                "exiftool",
                "-overwrite_original",
                f"-DateTimeOriginal={dt_str}",
                f"-CreateDate={dt_str}",
                f"-ModifyDate={dt_str}",
                file_path
            ], check=True)
            log_warn(f"[FIXED] {file_path} EXIF: {exif_dt} → {dt_str}")
        except subprocess.CalledProcessError as e:
            add_failure("ExifErrors", folder_name, Path(file_path).name, Path(file_path).parent)
            log_warn(f"[EXIFTOOL-WRITE] Failed to update EXIF on {file_path}: {e}")

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
                    add_to_album(upload_token, album_id, file_name)
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

# === MAIN ===
total_uploaded = 0
total_failed = 0

if RETRY_FAILED:
    log_warn("🔁 Modalità retry: elaborazione file falliti da failed_uploads.json...\n")
    for error_type in ["UploadError", "AddToAlbumError"]:
        for folder_name in list(failures.get(error_type, {}).keys()):
            entry = failures[error_type][folder_name]
            folder_path = Path(entry.get("path"))
            file_list = entry.get("files", [])

            if not folder_path.exists():
                log_warn(f"❌ Folder not found: {folder_path}")
                continue

            album_id = None
            for album_name, album_info in state.items():
                if album_info.get("path") == str(folder_path):
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
                file_path = folder_path / file_name
                if not file_path.is_file():
                    log_warn(f"❌ File not found in retry: {file_path}")
                    continue

                if UPDATE_FROM_FOLDER_DATE:
                    if file_path.name.lower().endswith(SUPPORTED_EXIF_EXT):
                        update_exif_date_if_mismatch(str(file_path), folder_name)
                    else:
                        add_failure("UnsupportedExifFormat", folder_name, Path(file_path).name, Path(file_path).parent)
                        log_warn(f"❌ File not valid Exif format: {file_path}")

                try:
                    upload_token = upload_file(str(file_path))
                    add_to_album(upload_token, album_id, file_name)
                    state[folder_name]['files'].append(file_name)
                    save_json(STATE_FILE, state)
                    logging.info(f"✅ RETRY {file_name} → {folder_name}")
                    total_uploaded += 1

                    failures[error_type][folder_name]["files"].remove(file_name)
                    if not failures[error_type][folder_name]["files"]:
                        del failures[error_type][folder_name]

                except Exception as e:
                    log_error(f"Errore in retry '{file_path}': {e}", exc_info=True)
                    total_failed += 1

    save_json(FAILED_FILE, failures)

    if LISTENER_MODE:
        process_exif_errors_loop()

else:
    photo_root = Path(PHOTO_ROOT_DIR)
    for folder in tqdm(sorted(photo_root.iterdir())):
        if not folder.is_dir():
            continue

        folder_name = folder.name
        folder_path = folder.resolve()

        if folder_name not in state:
            try:
                album_id = create_album(folder_name)
                state[folder_name] = {
                    'album_id': album_id,
                    'path': str(folder_path),
                    'files': []
                }
                save_json(STATE_FILE, state)
            except Exception as e:
                log_error(f"Errore creazione album '{folder_name}': {e}", exc_info=True)
                continue
        else:
            album_id = state[folder_name]['album_id']
            folder_path = Path(state[folder_name]['path'])

        files = set(state[folder_name].get('files', []))

        for file in sorted(folder_path.iterdir()):
            if not file.is_file():
                log_warn(f"❌ File not found in retry: {file}")
                continue

            if UPDATE_FROM_FOLDER_DATE:
                if file.name.lower().endswith(SUPPORTED_EXIF_EXT):
                    update_exif_date_if_mismatch(str(file), folder_name)
                else: 
                    add_failure("UnsupportedFormat", folder_name, file.name, folder_path)
                    log_warn(f"❌ File not valid format: {file}")

            try:
                upload_token = upload_file(str(file))
            except Exception as e:
                log_error(f"Errore upload '{file}': {e}", exc_info=True)
                add_failure("UploadError", folder_name, file.name, folder_path)
                total_failed += 1
                continue
            try:
                add_to_album(upload_token, album_id, file.name)
                state[folder_name]['files'].append(file.name)
                save_json(STATE_FILE, state)
                logging.info(f"✅ {file.name} → {folder_name}")
                total_uploaded += 1
            except Exception as e:
                log_error(f"Errore add_to_album '{file}': {e}", exc_info=True)
                add_failure("AddToAlbumError", folder_name, file.name, folder_path)
                total_failed += 1
                continue

# === REPORT ===
log_warn("\n✅ Elaborazione completata.")
log_warn(f"📸 File caricati con successo: {total_uploaded}")
log_warn(f"❌ File falliti: {total_failed} (vedi '{FAILED_FILE}')")
logging.info(f"✔️ Fine script: successi={total_uploaded}, fallimenti={total_failed}")
