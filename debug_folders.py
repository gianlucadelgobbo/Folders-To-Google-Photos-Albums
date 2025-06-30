#!/usr/bin/env python3
"""
Debug script to see what folders are being found and processed
"""

from pathlib import Path
import json
import os

PHOTO_ROOT_DIR = "/Users/gianlucadelgobbo/Library/CloudStorage/GoogleDrive-archive@flyer.it/My Drive/_PHOTOS-VIDEOS/LCF"

# Load state to see what's already processed
def load_json(path, default):
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return default

state = load_json("upload_state.json", {})
folder_cache = load_json("folder_cache.json", {
    "processed_folders": [],
    "last_processed_time": None,
    "folder_delay": 30
})

print("🔍 Debugging folder processing...")
print(f"Root directory: {PHOTO_ROOT_DIR}")
print(f"State entries: {len(state)}")
print(f"Folder cache: {folder_cache}")

folders_found = []
folders_skipped = []
folders_to_process = []

for folder_path in Path(PHOTO_ROOT_DIR).iterdir():
    if not folder_path.is_dir():
        continue
        
    folder_name = folder_path.name
    folders_found.append(folder_name)
    
    # Check if folder is in state
    in_state = folder_name in state
    in_cache = str(folder_path) in folder_cache.get("processed_folders", [])
    
    if in_state:
        print(f"✅ Found folder in state: {folder_name}")
        folders_to_process.append(folder_name)
    else:
        print(f"❌ Folder NOT in state: {folder_name}")
        folders_skipped.append(folder_name)

print(f"\n📊 Summary:")
print(f"Total folders found: {len(folders_found)}")
print(f"Folders in state: {len([f for f in folders_found if f in state])}")
print(f"Folders NOT in state: {len([f for f in folders_found if f not in state])}")
print(f"Folders to process: {len(folders_to_process)}")

print(f"\nFirst 10 folders to process:")
for i, folder in enumerate(folders_to_process[:10]):
    print(f"  {i+1}. {folder}")

if len(folders_to_process) > 10:
    print(f"  ... and {len(folders_to_process) - 10} more folders") 