# usage: python3 gphotos_uploader.py --path "/Users/gianlucadelgobbo/Library/CloudStorage/GoogleDrive-archive@flyer.it/My Drive/_PHOTOS-VIDEOS/Chromosphere"
# usage: python3 gphotos_uploader.py --path "/Users/gianlucadelgobbo/Library/CloudStorage/GoogleDrive-archive@flyer.it/My Drive/_PHOTOS-VIDEOS/Chromosphere" --retry-failed
import os
import json
import logging
from tqdm import tqdm
from pathlib import Path
from tenacity import retry, wait_fixed, stop_after_attempt
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import AuthorizedSession
import argparse

# === CLI ===
parser = argparse.ArgumentParser(description="Uploader per Google Photos")
parser.add_argument("--path", type=str, help="Percorso assoluto della cartella da elaborare", required=True)
parser.add_argument("--retry-failed", action="store_true", help="Riprova solo i file presenti in failed_uploads.json")
args = parser.parse_args()

PHOTO_ROOT_DIR = args.path
RETRY_FAILED = args.retry_failed

if not os.path.isdir(PHOTO_ROOT_DIR):
    print(f"❌ Cartella non valida: {PHOTO_ROOT_DIR}")
    exit(1)

# === CONFIG ===
SCOPES = ['https://www.googleapis.com/auth/photoslibrary.appendonly']
CREDENTIALS_FILE = 'credentials.json'
LOG_FILE = 'upload.log'
STATE_FILE = 'upload_state.json'
FAILED_FILE = 'failed_uploads.json'

# === LOGGING ===
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
failures = load_json(FAILED_FILE, {"UploadError": {}, "AddToAlbumError": {}, "TooLarge": {}})

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
        logging.warning(f"❌ File troppo grande: {file_name} ({file_size} bytes)")
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

# === MAIN ===
total_uploaded = 0
total_failed = 0

if RETRY_FAILED:
    print("🔁 Modalità retry: elaborazione file falliti da failed_uploads.json...\n")
    for error_type in ["UploadError", "AddToAlbumError"]:
        for folder_name, entry in failures.get(error_type, {}).items():
            folder_path = Path(entry.get("path"))
            file_list = entry.get("files", [])

            if not folder_path.exists():
                logging.warning(f"❌ Cartella non trovata: {folder_path}")
                continue

            # Cerca il nodo giusto in `state` confrontando i path assoluti
            album_id = None
            for album_name, album_info in state.items():
                if album_info.get("path") == str(folder_path):
                    album_id = album_info["album_id"]
                    folder_name = album_name  # allinea il nome corretto allo state
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
                    logging.error(f"Errore creazione album in retry per {folder_name}: {e}", exc_info=True)
                    continue

            for file_name in file_list[:]:
                file_path = folder_path / file_name
                if not file_path.is_file():
                    logging.warning(f"❌ File mancante in retry: {file_path}")
                    continue

                try:
                    upload_token = upload_file(str(file_path))
                    add_to_album(upload_token, album_id, file_name)
                    state[folder_name]['uploaded_files'].append(file_name)
                    save_json(STATE_FILE, state)
                    logging.info(f"✅ RETRY {file_name} → {folder_name}")
                    total_uploaded += 1

                    # ✅ Rimuove il file dai falliti
                    failures[error_type][folder_name]["files"].remove(file_name)

                    # ✅ Rimuove la cartella se vuota
                    if not failures[error_type][folder_name]["files"]:
                        del failures[error_type][folder_name]

                except Exception as e:
                    logging.error(f"Errore in retry '{file_path}': {e}", exc_info=True)
                    total_failed += 1


    save_json(FAILED_FILE, failures)

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
                logging.error(f"Errore creazione album '{folder_name}': {e}", exc_info=True)
                continue
        else:
            album_id = state[folder_name]['album_id']
            folder_path = Path(state[folder_name]['path'])

        uploaded_files = set(state[folder_name].get('files', []))

        for file in sorted(folder_path.iterdir()):
            if not file.is_file():
                continue
            if file.name in uploaded_files:
                continue
            try:
                upload_token = upload_file(str(file))
            except Exception as e:
                logging.error(f"Errore upload '{file}': {e}", exc_info=True)
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
                logging.error(f"Errore add_to_album '{file}': {e}", exc_info=True)
                add_failure("AddToAlbumError", folder_name, file.name, folder_path)
                total_failed += 1
                continue

# === REPORT ===
print("\n✅ Elaborazione completata.")
print(f"📸 File caricati con successo: {total_uploaded}")
print(f"❌ File falliti: {total_failed} (vedi '{FAILED_FILE}')")
logging.info(f"✔️ Fine script: successi={total_uploaded}, fallimenti={total_failed}")
