#!/usr/bin/env python3
"""
Dedicated script to build or refresh galbum_cache.json from Google Photos.
Does not touch, modify, or scan any local photo files.

Usage (run from project root):
    python3 tools/refresh_cache.py
"""

import json
import os
import sys
import time
import warnings
from pathlib import Path
from typing import Dict, Set

warnings.filterwarnings('ignore', category=Warning)

from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# === CONFIG ===
SCOPES = [
    'https://www.googleapis.com/auth/photoslibrary.readonly',
]
PROJECT_ROOT = Path(__file__).parent.parent
CREDENTIALS_FILE = str(PROJECT_ROOT / 'credentials.json')
TOKEN_FILE = str(PROJECT_ROOT / 'token.json')
ALBUM_CACHE_FILE = str(PROJECT_ROOT / 'galbum_cache.json')

# === LOGGING ===
def log(msg): print(msg, flush=True)
def err(msg): print(msg, file=sys.stderr, flush=True)

# === JSON ===
def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

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

# === CACHE BUILDER ===
def rebuild_cache(session: AuthorizedSession):
    log('[INDEX] Fetching all albums from Google Photos API...')

    # Step 1: fetch all albums
    album_map: Dict[str, str] = {}  # title → album_id
    id_to_title: Dict[str, str] = {}
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
            
    log(f'[INDEX] Found {len(album_map)} total album(s)')

    # Step 2: for each album, fetch all media items
    cache_albums: Dict[str, dict] = {}  # album_id → {title, photos:[{filename, id}]}
    total_photos_indexed = 0

    for i, (album_id, title) in enumerate(id_to_title.items(), 1):
        log(f'[INDEX] [{i}/{len(id_to_title)}] Requesting items for album: "{title}"...')
        page_token = None
        photos = []
        
        while True:
            body: dict = {'albumId': album_id, 'pageSize': 100}
            if page_token:
                body['pageToken'] = page_token
            r = session.post(
                'https://photoslibrary.googleapis.com/v1/mediaItems:search',
                json=body, timeout=(10, 60),
            )
            if r.status_code != 200:
                err(f'  [INDEX] Error listing items in "{title}": {r.status_code} {r.text[:200]}')
                break
            data = r.json()
            for item in data.get('mediaItems', []):
                fn = item.get('filename', '')
                mid = item.get('id', '')
                if fn:
                    photos.append({'filename': fn, 'id': mid})
                    total_photos_indexed += 1
            page_token = data.get('nextPageToken')
            if not page_token:
                break
                
        cache_albums[album_id] = {'title': title, 'photos': photos}
        log(f'  → Cached {len(photos)} item(s)')

    # Step 3: Package data and commit to disk
    cache_data = {
        'timestamp': int(time.time()), 
        'albums': cache_albums
    }
    
    log(f'[INDEX] Saving complete data to {ALBUM_CACHE_FILE}...')
    save_json(ALBUM_CACHE_FILE, cache_data)
    log(f'[SUCCESS] Cache rebuild complete! Total cached items: {total_photos_indexed}')

def main():
    if not os.path.exists(CREDENTIALS_FILE):
        err(f'[ERROR] Missing credentials file at: {CREDENTIALS_FILE}')
        sys.exit(1)
        
    log('[INIT] Authenticating with Google Photos...')
    session = authenticate()
    log('[INIT] Authenticated successfully.')
    
    rebuild_cache(session)

if __name__ == '__main__':
    main()