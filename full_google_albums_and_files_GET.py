#!/usr/bin/env python3

import os
import json
import time
import logging
import sys
import warnings
from pathlib import Path
from tenacity import retry, wait_fixed, stop_after_attempt
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import AuthorizedSession
import argparse

# Suppress urllib3 SSL warnings
warnings.filterwarnings('ignore', category=Warning)

def log_init(msg):
    print(msg, flush=True)
    sys.stdout.flush()

log_init("[INIT] Script starting...")

# === CONFIG ===
log_init("[INIT] Loading configuration...")
SCOPES = [
    'https://www.googleapis.com/auth/photoslibrary',
    'https://www.googleapis.com/auth/photoslibrary.readonly',
    'https://www.googleapis.com/auth/photoslibrary.appendonly',
    'https://www.googleapis.com/auth/photoslibrary.readonly.appcreateddata'
]
CREDENTIALS_FILE = 'credentials.json'
LOG_FILE = 'full_google_albums_and_files_GET.log'
STATE_FILE = 'upload_state.json'
ALBUM_CACHE_FILE = 'full_google_albums_and_files.json'  # New cache file for albums
ALBUM_CACHE_EXPIRY = 3600  # Cache expiry time in seconds (1 hour)

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
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log_warn(f"[STATE] Saved state to {path}: {len(data)} entries")

def load_album_cache():
    """Load album cache from file if it exists and is not expired"""
    if not os.path.exists(ALBUM_CACHE_FILE):
        return None
        
    try:
        with open(ALBUM_CACHE_FILE, 'r') as f:
            cache_data = json.load(f)
            
        # Check if cache is expired
        if time.time() - cache_data.get('timestamp', 0) > ALBUM_CACHE_EXPIRY:
            log_warn("[CACHE] Album cache expired")
            return None
            
        log_warn(f"[CACHE] Loaded {len(cache_data['albums'])} albums from cache")
        return cache_data['albums']
    except Exception as e:
        log_error(f"[CACHE] Error loading album cache: {str(e)}")
        return None

def save_album_cache(albums):
    """Save album cache to file"""
    try:
        cache_data = {
            'timestamp': time.time(),
            'albums': albums
        }
        with open(ALBUM_CACHE_FILE, 'w') as f:
            json.dump(cache_data, f, indent=2)
        log_warn(f"[CACHE] Saved {len(albums)} albums to cache")
    except Exception as e:
        log_error(f"[CACHE] Error saving album cache: {str(e)}")

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

# === API WRAPPERS ===
@retry(wait=wait_fixed(5), stop=stop_after_attempt(5))
def get_all_albums():
    log_warn("[ALBUM] Getting all albums...")
    
    # Try to load from cache first
    cached_albums = load_album_cache()
    if cached_albums is not None:
        return cached_albums
    
    # If no cache or expired, fetch from API
    all_albums = []
    page_token = None
    
    while True:
        url = "https://photoslibrary.googleapis.com/v1/albums"
        if page_token:
            url += f"?pageToken={page_token}"
            
        response = session.get(url)
        if response.status_code != 200:
            log_error(f"[ALBUM] Error getting albums: {response.status_code} - {response.text}")
            break
            
        data = response.json()
        albums = data.get('albums', [])
        all_albums.extend(albums)
        
        page_token = data.get('nextPageToken')
        if not page_token:
            break
            
        log_warn(f"[ALBUM] Getting next page of albums...")
    
    log_warn(f"[ALBUM] Found {len(all_albums)} albums")
    
    # Save to cache
    save_album_cache(all_albums)
    
    return all_albums

@retry(wait=wait_fixed(5), stop=stop_after_attempt(5))
def delete_album(album_id):
    log_warn(f"[ALBUM] Deleting album with ID: {album_id}")
    try:
        response = session.delete(f"https://photoslibrary.googleapis.com/v1/albums/{album_id}")
        if response.status_code == 200:
            log_warn(f"[ALBUM] Successfully deleted album: {album_id}")
            return True
        else:
            log_error(f"[ALBUM] Error deleting album: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        log_error(f"[ALBUM] Failed to delete album: {str(e)}")
        return False

def analyze_albums():
    """Analyze albums and print statistics"""
    log_warn("[ANALYZE] Starting album analysis...")
    
    # Load state
    state = load_json(STATE_FILE, {})
    
    # Get all albums from Google Photos
    all_albums = get_all_albums()
    
    # Analyze albums in state
    state_albums = set()
    for folder_name, album_info in state.items():
        album_id = album_info.get('album_id')
        if album_id:
            state_albums.add(album_id)
    
    # Find albums that exist in Google Photos but not in state
    orphaned_albums = []
    for album in all_albums:
        if album['id'] not in state_albums:
            orphaned_albums.append(album)
    
    # Find albums that exist in state but not in Google Photos
    missing_albums = []
    for folder_name, album_info in state.items():
        album_id = album_info.get('album_id')
        if album_id and not any(a['id'] == album_id for a in all_albums):
            missing_albums.append((folder_name, album_id))
    
    # Print statistics
    log_warn("\n=== Album Analysis ===")
    log_warn(f"Total albums in Google Photos: {len(all_albums)}")
    log_warn(f"Total albums in state: {len(state_albums)}")
    log_warn(f"Orphaned albums (in Google Photos but not in state): {len(orphaned_albums)}")
    log_warn(f"Missing albums (in state but not in Google Photos): {len(missing_albums)}")
    
    if orphaned_albums:
        log_warn("\nOrphaned albums:")
        for album in orphaned_albums:
            log_warn(f"- {album['title']} (ID: {album['id']})")
    
    if missing_albums:
        log_warn("\nMissing albums:")
        for folder_name, album_id in missing_albums:
            log_warn(f"- {folder_name} (ID: {album_id})")

def cleanup_state():
    """Remove invalid albums from state"""
    log_warn("[CLEANUP] Starting state cleanup...")
    
    # Load state
    state = load_json(STATE_FILE, {})
    
    # Get all albums from Google Photos
    all_albums = get_all_albums()
    valid_album_ids = {album['id'] for album in all_albums}
    
    # Remove invalid albums from state
    removed_count = 0
    for folder_name, album_info in list(state.items()):
        album_id = album_info.get('album_id')
        if album_id and album_id not in valid_album_ids:
            log_warn(f"[CLEANUP] Removing invalid album from state: {folder_name}")
            del state[folder_name]
            removed_count += 1
    
    if removed_count > 0:
        save_json(STATE_FILE, state)
        log_warn(f"[CLEANUP] Removed {removed_count} invalid albums from state")
    else:
        log_warn("[CLEANUP] No invalid albums found in state")

def delete_orphaned_albums():
    """Delete albums that exist in Google Photos but not in state"""
    log_warn("[DELETE] Starting deletion of orphaned albums...")
    
    # Load state
    state = load_json(STATE_FILE, {})
    
    # Get all albums from Google Photos
    all_albums = get_all_albums()
    
    # Find and delete orphaned albums
    deleted_count = 0
    for album in all_albums:
        if not any(album_info.get('album_id') == album['id'] for album_info in state.values()):
            log_warn(f"[DELETE] Deleting orphaned album: {album['title']}")
            if delete_album(album['id']):
                deleted_count += 1
    
    log_warn(f"[DELETE] Deleted {deleted_count} orphaned albums")

def delete_albums_by_prefix(prefix):
    """Delete albums that start with the specified prefix"""
    log_warn(f"[DELETE] Starting deletion of albums with prefix '{prefix}'...")
    
    # Get all albums from Google Photos
    all_albums = get_all_albums()
    
    # Find albums with the prefix
    albums_to_delete = []
    for album in all_albums:
        if album['title'].startswith(prefix):
            albums_to_delete.append(album)
    
    if not albums_to_delete:
        log_warn(f"[DELETE] No albums found with prefix '{prefix}'")
        return
    
    # Print albums that would be deleted
    log_warn(f"\nFound {len(albums_to_delete)} albums with prefix '{prefix}':")
    for album in albums_to_delete:
        log_warn(f"- {album['title']} (ID: {album['id']})")
    
    if args.dry_run:
        log_warn("\n[DRY-RUN] Would delete these albums")
        return
    
    # Actually delete the albums
    deleted_count = 0
    for album in albums_to_delete:
        log_warn(f"[DELETE] Deleting album: {album['title']}")
        if delete_album(album['id']):
            deleted_count += 1
            # Also remove from state if present
            for folder_name, album_info in list(state.items()):
                if album_info.get('album_id') == album['id']:
                    log_warn(f"[DELETE] Removing album from state: {folder_name}")
                    del state[folder_name]
    
    if deleted_count > 0:
        save_json(STATE_FILE, state)
        log_warn(f"[DELETE] Deleted {deleted_count} albums with prefix '{prefix}'")
    else:
        log_warn(f"[DELETE] No albums were deleted")

def list_all_albums():
    """Print all albums (title and ID) found in Google Photos."""
    all_albums = get_all_albums()
    log_warn(f"\n=== Lista completa album trovati ({len(all_albums)}) ===")
    for album in all_albums:
        log_warn(f"- {album.get('title','<no title>')} (ID: {album.get('id','<no id>')})")

@retry(wait=wait_fixed(5), stop=stop_after_attempt(5))
def remove_media_from_album(album_id):
    """Remove all media items from an album"""
    log_warn(f"[ALBUM] Removing all media from album ID: {album_id}")
    try:
        # First get all media items in the album
        page_token = None
        all_media_items = []
        
        while True:
            url = f"https://photoslibrary.googleapis.com/v1/mediaItems:search"
            body = {
                "albumId": album_id,
                "pageSize": 100
            }
            if page_token:
                body["pageToken"] = page_token
                
            response = session.post(url, json=body, timeout=300)
            if response.status_code != 200:
                log_error(f"[ALBUM] Error getting media items: {response.status_code} - {response.text}")
                return False
                
            data = response.json()
            media_items = data.get('mediaItems', [])
            all_media_items.extend(media_items)
            
            page_token = data.get('nextPageToken')
            if not page_token:
                break
                
            log_warn(f"[ALBUM] Getting next page of media items...")
        
        if not all_media_items:
            log_warn(f"[ALBUM] No media items found in album")
            return True
            
        # Remove media items in batches
        batch_size = 50
        for i in range(0, len(all_media_items), batch_size):
            batch = all_media_items[i:i + batch_size]
            media_item_ids = [item['id'] for item in batch]
            
            url = f"https://photoslibrary.googleapis.com/v1/albums/{album_id}:batchRemoveMediaItems"
            body = {
                "mediaItemIds": media_item_ids
            }
            
            response = session.post(url, json=body, timeout=300)
            if response.status_code != 200:
                log_error(f"[ALBUM] Error removing media items: {response.status_code} - {response.text}")
                return False
                
            log_warn(f"[ALBUM] Removed {len(batch)} media items from album")
            
        return True
    except Exception as e:
        log_error(f"[ALBUM] Failed to remove media items: {str(e)}")
        return False

def remove_media_from_albums_by_prefix(prefix):
    """Remove all media items from albums that start with the specified prefix"""
    log_warn(f"[DELETE] Starting removal of media from albums with prefix '{prefix}'...")
    
    # Get all albums from Google Photos
    all_albums = get_all_albums()
    
    # Find albums with the prefix
    albums_to_process = []
    for album in all_albums:
        if album['title'].startswith(prefix):
            albums_to_process.append(album)
    
    if not albums_to_process:
        log_warn(f"[DELETE] No albums found with prefix '{prefix}'")
        return
    
    # Print albums that will be processed
    log_warn(f"\nFound {len(albums_to_process)} albums with prefix '{prefix}':")
    for album in albums_to_process:
        log_warn(f"- {album['title']} (ID: {album['id']})")
    
    if args.dry_run:
        log_warn("\n[DRY-RUN] Would remove media from these albums")
        return
    
    # Process each album
    processed_count = 0
    for album in albums_to_process:
        log_warn(f"[DELETE] Processing album: {album['title']}")
        if remove_media_from_album(album['id']):
            processed_count += 1
    
    log_warn(f"[DELETE] Processed {processed_count} albums with prefix '{prefix}'")

@retry(wait=wait_fixed(5), stop=stop_after_attempt(5))
def move_media_to_album(source_album_id, target_album_id):
    """Move all media items from source album to target album"""
    log_warn(f"[ALBUM] Moving media from album {source_album_id} to {target_album_id}")
    try:
        # First get all media items in the source album
        page_token = None
        all_media_items = []
        
        while True:
            url = f"https://photoslibrary.googleapis.com/v1/mediaItems:search"
            body = {
                "albumId": source_album_id,
                "pageSize": 100
            }
            if page_token:
                body["pageToken"] = page_token
                
            response = session.post(url, json=body, timeout=300)
            if response.status_code != 200:
                log_error(f"[ALBUM] Error getting media items: {response.status_code} - {response.text}")
                return False
                
            data = response.json()
            media_items = data.get('mediaItems', [])
            all_media_items.extend(media_items)
            
            page_token = data.get('nextPageToken')
            if not page_token:
                break
                
            log_warn(f"[ALBUM] Getting next page of media items...")
        
        if not all_media_items:
            log_warn(f"[ALBUM] No media items found in source album")
            return True
            
        # Add media items to target album in batches
        batch_size = 50
        for i in range(0, len(all_media_items), batch_size):
            batch = all_media_items[i:i + batch_size]
            media_item_ids = [item['id'] for item in batch]
            
            url = f"https://photoslibrary.googleapis.com/v1/albums/{target_album_id}:batchAddMediaItems"
            body = {
                "mediaItemIds": media_item_ids
            }
            
            response = session.post(url, json=body, timeout=300)
            if response.status_code != 200:
                log_error(f"[ALBUM] Error adding media items: {response.status_code} - {response.text}")
                return False
                
            log_warn(f"[ALBUM] Added {len(batch)} media items to target album")
            
        return True
    except Exception as e:
        log_error(f"[ALBUM] Failed to move media items: {str(e)}")
        return False

def move_media_from_albums_by_prefix(prefix, target_album_name="TEMP_ALBUM"):
    """Move all media items from albums with prefix to a target album and remove them from source albums."""
    log_warn(f"[MOVE] Starting move of media from albums with prefix '{prefix}' to '{target_album_name}'...")
    
    # Get all albums from Google Photos
    all_albums = get_all_albums()
    
    # Find albums with the prefix
    albums_to_process = []
    for album in all_albums:
        if album['title'].startswith(prefix):
            albums_to_process.append(album)
    
    if not albums_to_process:
        log_warn(f"[MOVE] No albums found with prefix '{prefix}'")
        return
    
    # Create target album if it doesn't exist
    target_album_id = None
    for album in all_albums:
        if album['title'] == target_album_name:
            target_album_id = album['id']
            break
    
    if not target_album_id:
        log_warn(f"[MOVE] Creating target album: {target_album_name}")
        target_album_id = create_album(target_album_name)
    
    # Print albums that will be processed
    log_warn(f"\nFound {len(albums_to_process)} albums with prefix '{prefix}':")
    for album in albums_to_process:
        log_warn(f"- {album['title']} (ID: {album['id']})")
    
    if args.dry_run:
        log_warn("\n[DRY-RUN] Would move media from these albums")
        return
    
    # Process each album
    processed_count = 0
    for album in albums_to_process:
        log_warn(f"[MOVE] Processing album: {album['title']}")
        if move_media_to_album(album['id'], target_album_id):
            # Remove media from source album after moving
            if remove_media_from_album(album['id']):
                processed_count += 1
    
    log_warn(f"[MOVE] Processed {processed_count} albums with prefix '{prefix}'")

def create_album(album_name):
    """Create a new album with the given name."""
    try:
        body = {
            'album': {
                'title': album_name
            }
        }
        response = session.post(f"https://photoslibrary.googleapis.com/v1/albums", json=body, timeout=300)
        if response.status_code != 200:
            log_error(f"Error creating album {album_name}: {response.status_code} - {response.text}")
            raise Exception(f"Error creating album {album_name}: {response.status_code}")
        data = response.json()
        log_warn(f"Created album: {album_name} (ID: {data['id']})")
        return data['id']
    except Exception as e:
        log_error(f"Error creating album {album_name}: {str(e)}")
        raise

def rename_album(album_id, new_title):
    """Rename an album with the given ID to the new title."""
    try:
        body = {
            'album': {
                'title': new_title
            }
        }
        response = session.patch(f"https://photoslibrary.googleapis.com/v1/albums/{album_id}", json=body, timeout=300)
        if response.status_code != 200:
            log_error(f"Error renaming album {album_id}: {response.status_code} - {response.text}")
            raise Exception(f"Error renaming album {album_id}: {response.status_code}")
        log_warn(f"Renamed album {album_id} to {new_title}")
        return True
    except Exception as e:
        log_error(f"Error renaming album {album_id}: {str(e)}")
        raise

def rename_albums_by_prefix(prefix, new_name):
    """Rename all albums that start with the specified prefix to the new name."""
    log_warn(f"[RENAME] Starting rename of albums with prefix '{prefix}' to '{new_name}'...")
    
    # Get all albums from Google Photos
    all_albums = get_all_albums()
    
    # Find albums with the prefix
    albums_to_rename = []
    for album in all_albums:
        if album['title'].startswith(prefix):
            albums_to_rename.append(album)
    
    if not albums_to_rename:
        log_warn(f"[RENAME] No albums found with prefix '{prefix}'")
        return
    
    # Print albums that will be renamed
    log_warn(f"\nFound {len(albums_to_rename)} albums with prefix '{prefix}':")
    for album in albums_to_rename:
        log_warn(f"- {album['title']} (ID: {album['id']})")
    
    if args.dry_run:
        log_warn("\n[DRY-RUN] Would rename these albums")
        return
    
    # Process each album
    processed_count = 0
    for album in albums_to_rename:
        log_warn(f"[RENAME] Processing album: {album['title']}")
        if rename_album(album['id'], new_name):
            processed_count += 1
    
    log_warn(f"[RENAME] Processed {processed_count} albums with prefix '{prefix}'")

def create_album_cache():
    """Create a detailed cache of albums with their photos."""
    log_warn("[CACHE] Creating detailed album cache...")
    
    # Clear the cache first
    if os.path.exists('album_cache.json'):
        os.remove('album_cache.json')
    
    # Get all albums from Google Photos
    all_albums = get_all_albums()
    
    # First, save the list of albums
    albums_cache = {
        'timestamp': time.time(),
        'albums': {}
    }
    
    # Create new cache with album IDs as keys
    for album in all_albums:
        if isinstance(album, dict):  # Make sure album is a dictionary
            albums_cache['albums'][album['id']] = {
                'title': album['title'],
                'photo_count': 0,  # Will be updated later
                'photos': []       # Will be populated later
            }
    
    # Save initial albums cache
    with open('album_cache.json', 'w') as f:
        json.dump(albums_cache, f, indent=2)
    
    log_warn(f"[CACHE] Saved {len(albums_cache['albums'])} albums to cache")
    
    # Now, for each album, get and save its photos
    for album_id, album_info in albums_cache['albums'].items():
        log_warn(f"[CACHE] Getting photos for album: {album_info['title']}")
        
        photos = []
        page_token = None
        
        while True:
            url = f"https://photoslibrary.googleapis.com/v1/mediaItems:search"
            body = {
                "albumId": album_id,
                "pageSize": 100
            }
            if page_token:
                body["pageToken"] = page_token
                
            response = session.post(url, json=body, timeout=300)
            if response.status_code != 200:
                log_error(f"[CACHE] Error getting photos for album {album_info['title']}: {response.status_code} - {response.text}")
                break
                
            data = response.json()
            media_items = data.get('mediaItems', [])
            
            for item in media_items:
                photos.append({
                    'id': item['id'],
                    'filename': item.get('filename', ''),
                    'mimeType': item.get('mimeType', ''),
                    'creationTime': item.get('creationTime', '')
                })
            
            page_token = data.get('nextPageToken')
            if not page_token:
                break
        
        # Update album info with photos
        album_info['photo_count'] = len(photos)
        album_info['photos'] = photos
        
        # Save updated cache after each album
        with open('album_cache.json', 'w') as f:
            json.dump(albums_cache, f, indent=2)
        
        log_warn(f"[CACHE] Saved {len(photos)} photos for album: {album_info['title']}")
    
    log_warn("[CACHE] Completed creating detailed album cache")
    return albums_cache

def analyze_duplicates():
    """Analyze the cache to find albums with identical names."""
    log_warn("[ANALYZE] Starting album name analysis...")
    
    try:
        with open('album_cache.json', 'r') as f:
            cache_data = json.load(f)
            
        # Get all album titles
        title_groups = {}
        for album_id, album_info in cache_data['albums'].items():
            title = album_info['title']
            if title in title_groups:
                title_groups[title].append(album_id)
            else:
                title_groups[title] = [album_id]
        
        # Filter only groups with more than one album
        duplicate_groups = {title: album_ids for title, album_ids in title_groups.items() if len(album_ids) > 1}
        
        # Print statistics
        log_warn("\n=== Album Name Analysis ===")
        log_warn(f"Total albums analyzed: {len(cache_data['albums'])}")
        log_warn(f"Groups with identical names: {len(duplicate_groups)}")
        
        if duplicate_groups:
            log_warn("\nIdentical album names found:")
            for title, album_ids in duplicate_groups.items():
                log_warn(f"\nAlbum name: {title}")
                log_warn(f"Found {len(album_ids)} identical albums:")
                for album_id in album_ids:
                    album_info = cache_data['albums'][album_id]
                    log_warn(f"- {title} (ID: {album_id}, Photos: {album_info['photo_count']})")
        
    except Exception as e:
        log_error(f"[ANALYZE] Error analyzing album names: {str(e)}")

def create_merge_cache():
    """Create a cache structure for merging duplicate albums using the existing album cache."""
    log_warn("[CACHE] Creating merge cache for duplicate albums...")
    
    try:
        # Load existing album cache
        with open('album_cache.json', 'r') as f:
            cache_data = json.load(f)
            
        # Create a dictionary to group albums by their exact name
        name_groups = {}
        
        # Group albums by their exact name
        for album_id, album_info in cache_data['albums'].items():
            title = album_info['title']
            if title in name_groups:
                name_groups[title].append({
                    'id': album_id,
                    'title': album_info['title'],
                    'photo_count': album_info['photo_count'],
                    'photos': album_info['photos']
                })
            else:
                name_groups[title] = [{
                    'id': album_id,
                    'title': album_info['title'],
                    'photo_count': album_info['photo_count'],
                    'photos': album_info['photos']
                }]
        
        # Create the merge cache structure
        merge_cache = {
            'timestamp': time.time(),
            'groups': []
        }
        
        # Process each name group
        for title, albums in name_groups.items():
            if len(albums) > 1:  # Only include groups with duplicates
                # Sort albums by ID to ensure consistent ordering
                albums.sort(key=lambda x: x['id'])
                
                # Create group entry
                group = {
                    'main_album': albums[0],  # First album becomes the main one
                    'duplicates': albums[1:],  # Rest are marked for deletion
                    'total_photos': sum(album['photo_count'] for album in albums)
                }
                merge_cache['groups'].append(group)
        
        # Save the merge cache
        with open('merge_cache.json', 'w') as f:
            json.dump(merge_cache, f, indent=2)
        
        log_warn(f"[CACHE] Created merge cache with {len(merge_cache['groups'])} duplicate groups")
        return merge_cache
        
    except Exception as e:
        log_error(f"[CACHE] Error creating merge cache: {str(e)}")
        return None

def analyze_merge_cache():
    """Analyze the merge cache and print statistics."""
    log_warn("[ANALYZE] Analyzing merge cache...")
    
    try:
        with open('merge_cache.json', 'r') as f:
            cache_data = json.load(f)
            
        # Print statistics
        log_warn("\n=== Merge Cache Analysis ===")
        log_warn(f"Total duplicate groups: {len(cache_data['groups'])}")
        
        total_duplicates = sum(len(group['duplicates']) for group in cache_data['groups'])
        log_warn(f"Total albums to be merged: {total_duplicates}")
        
        if cache_data['groups']:
            log_warn("\nDuplicate groups found:")
            for i, group in enumerate(cache_data['groups'], 1):
                log_warn(f"\nGroup {i}:")
                log_warn(f"Main album: {group['main_album']['title']} (ID: {group['main_album']['id']}, Photos: {group['main_album']['photo_count']})")
                log_warn(f"Duplicates to be merged:")
                for dup in group['duplicates']:
                    log_warn(f"- {dup['title']} (ID: {dup['id']}, Photos: {dup['photo_count']})")
        
    except Exception as e:
        log_error(f"[ANALYZE] Error analyzing merge cache: {str(e)}")

def merge_duplicate_albums(dry_run=False):
    """Merge duplicate albums by moving all photos from duplicates to a main album and renaming duplicates with _buttare prefix."""
    try:
        # Load merge cache
        print("\n[DEBUG] Loading merge cache...")
        with open('merge_cache.json', 'r') as f:
            merge_cache = json.load(f)
        print(f"[DEBUG] Loaded {len(merge_cache['groups'])} groups from merge cache")
        
        print("\n=== Processing duplicate albums ===")
        
        # Process each group
        for group in merge_cache['groups']:
            main_album = group['main_album']
            if not main_album['title'].startswith('IMG_'):
                print(f"\n[DEBUG] Processing group with main album: {main_album['title']}")
                duplicates = group['duplicates']
                print(f"[DEBUG] Found {len(duplicates)} duplicates for {main_album['title']}")
                
                print(f"\nProcessing group: {main_album['title']}")
                print(f"Main album: {main_album['title']} (ID: {main_album['id']})")
                print(f"Photos in main album: {main_album['photo_count']}")
                
                for dup in duplicates:
                    print(f"\nDuplicate album: {dup['title']} (ID: {dup['id']})")
                    print(f"Photos in duplicate: {dup['photo_count']}")
                    
                    # Get photos that are not in the main album
                    new_photos = [p for p in dup['photos'] if p['id'] not in [mp['id'] for mp in main_album['photos']]]
                    print(f"[DEBUG] Found {len(new_photos)} new photos to move from {dup['title']}")
                    
                    if new_photos:
                        if dry_run:
                            print(f"Would add {len(new_photos)} new photos to main album:")
                            for photo in new_photos:
                                print(f"  - {photo['filename']} - {photo['id']}")
                                main_album['photos'].append(photo)  # Update main album photos
                        else:
                            print(f"\Adding {len(new_photos)} photos from {dup['title']} to {main_album['title']}")
                            for photo in new_photos:
                                try:
                                    print(f"[DEBUG] Adding photo {photo['filename']} to album {main_album['title']}")
                                    response = session.post(
                                        f"https://photoslibrary.googleapis.com/v1/albums/{main_album['id']}:batchAddMediaItems",
                                        json={
                                            'mediaItemIds': [photo['id']]
                                        }
                                    )
                                    if response.status_code == 200:
                                        print(f"  - Moved {photo['filename']}")
                                        main_album['photos'].append(photo)  # Update main album photos
                                    else:
                                        print(f"  - Error adding {photo['filename']}: {response.status_code} - {response.text}")
                                except Exception as e:
                                    print(f"  - Error adding {photo['filename']}: {str(e)}")
                    else:
                        print("No new photos to add")
                    
            else:
                print(f"[DEBUG] Skipping group with main album: {main_album['title']} (not the special album or starts with IMG_)")
        
        print("\n=== Process completed ===")
            
    except Exception as e:
        print(f"Error merging duplicate albums: {str(e)}")
        raise

# === CLI ===
log_init("[INIT] Setting up argument parser...")
parser = argparse.ArgumentParser(description="Google Photos Album Manager")
parser.add_argument("--analyze", action="store_true", help="Analyze albums and print statistics")
parser.add_argument("--cleanup-state", action="store_true", help="Remove invalid albums from state")
parser.add_argument("--delete-orphaned", action="store_true", help="Delete albums that exist in Google Photos but not in state")
parser.add_argument("--delete-prefix", type=str, help="Delete albums that start with the specified prefix")
parser.add_argument("--list-all", action="store_true", help="List all albums found in Google Photos")
parser.add_argument("--dry-run", action="store_true", help="Simulate all actions without making changes")
parser.add_argument("--remove-media-prefix", type=str, help="Remove all media items from albums that start with the specified prefix")
parser.add_argument("--move-media-prefix", type=str, help="Move all media items from albums that start with the specified prefix to a target album")
parser.add_argument("--target-album", type=str, default="TEMP_ALBUM", help="Name of the target album for moving media items")
parser.add_argument("--clear-cache", action="store_true", help="Clear the album cache")
parser.add_argument("--rename-prefix", type=str, help="Rename albums that start with the specified prefix to a new name")
parser.add_argument("--new-name", type=str, help="New name for the albums to be renamed")
parser.add_argument("--create-cache", action="store_true", help="Create a detailed cache of albums with their photos")
parser.add_argument("--analyze-duplicates", action="store_true", help="Analyze duplicate albums by name")
parser.add_argument("--create-merge-cache", action="store_true", help="Create a cache for merging duplicate albums")
parser.add_argument("--analyze-merge-cache", action="store_true", help="Analyze the merge cache")
parser.add_argument("--merge-duplicates", action="store_true", help="Merge duplicate albums and rename duplicates with _buttare prefix")

log_init("[INIT] Parsing arguments...")
try:
    args = parser.parse_args()
    log_init(f"[INIT] Arguments parsed: analyze={args.analyze}, cleanup_state={args.cleanup_state}, delete_orphaned={args.delete_orphaned}, delete_prefix={args.delete_prefix}, list_all={args.list_all}, dry_run={args.dry_run}, remove_media_prefix={args.remove_media_prefix}, move_media_prefix={args.move_media_prefix}, target_album={args.target_album}, clear_cache={args.clear_cache}, rename_prefix={args.rename_prefix}, new_name={args.new_name}, create_cache={args.create_cache}, analyze_duplicates={args.analyze_duplicates}, create_merge_cache={args.create_merge_cache}, analyze_merge_cache={args.analyze_merge_cache}, merge_duplicates={args.merge_duplicates}")
except Exception as e:
    print(f"Error parsing arguments: {e}", file=sys.stderr)
    sys.exit(1)

# === MAIN ===
try:
    # Check which operations need authentication
    needs_auth = [
        args.delete_orphaned,
        args.delete_prefix,
        args.move_media_prefix,
        args.rename_prefix,
        args.new_name,
        args.create_cache,
        args.analyze_duplicates,
        args.merge_duplicates and not args.dry_run  # Only need auth for actual merge, not dry run
    ]
    
    if any(needs_auth):
        log_warn("[INIT] Initializing Google Photos session...")
        session = authenticate()
        log_warn("[INIT] Successfully initialized session")
        # Load state at the beginning
        state = load_json(STATE_FILE, {})
except Exception as e:
    log_error(f"[INIT] Failed to initialize session: {str(e)}", exc_info=True)
    sys.exit(1)

if args.list_all:
    list_all_albums()

if args.analyze:
    analyze_albums()

if args.cleanup_state:
    if args.dry_run:
        log_warn("[DRY-RUN] Would clean up state")
    else:
        cleanup_state()

if args.delete_orphaned:
    if args.dry_run:
        log_warn("[DRY-RUN] Would delete orphaned albums")
    else:
        delete_orphaned_albums()

if args.delete_prefix:
    if args.dry_run:
        log_warn(f"[DRY-RUN] Would delete albums with prefix '{args.delete_prefix}'")
    else:
        delete_albums_by_prefix(args.delete_prefix)

if args.remove_media_prefix:
    if args.dry_run:
        log_warn(f"[DRY-RUN] Would remove media from albums with prefix '{args.remove_media_prefix}'")
    else:
        remove_media_from_albums_by_prefix(args.remove_media_prefix)

if args.move_media_prefix:
    if args.dry_run:
        log_warn(f"[DRY-RUN] Would move media from albums with prefix '{args.move_media_prefix}' to '{args.target_album}'")
    else:
        move_media_from_albums_by_prefix(args.move_media_prefix, args.target_album)

if args.clear_cache:
    if os.path.exists(ALBUM_CACHE_FILE):
        os.remove(ALBUM_CACHE_FILE)
        log_warn("[CACHE] Cleared album cache")

if args.rename_prefix:
    if args.dry_run:
        log_warn(f"[DRY-RUN] Would rename albums with prefix '{args.rename_prefix}' to '{args.new_name}'")
    else:
        rename_albums_by_prefix(args.rename_prefix, args.new_name)

if args.create_cache:
    create_album_cache()

if args.analyze_duplicates:
    analyze_duplicates()

if args.create_merge_cache:
    create_merge_cache()

if args.analyze_merge_cache:
    analyze_merge_cache()

if args.merge_duplicates:
    merge_duplicate_albums(dry_run=args.dry_run)

log_warn("\n✅ Script completed") 