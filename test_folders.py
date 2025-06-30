#!/usr/bin/env python3
"""
Test script to see what folders are found in the LCF directory
"""

from pathlib import Path
import sys

PHOTO_ROOT_DIR = "/Users/gianlucadelgobbo/Library/CloudStorage/GoogleDrive-archive@flyer.it/My Drive/_PHOTOS-VIDEOS/LCF"

print("🔍 Scanning directory...")
print(f"Root directory: {PHOTO_ROOT_DIR}")

folders_found = []
for folder_path in Path(PHOTO_ROOT_DIR).iterdir():
    if folder_path.is_dir():
        folders_found.append(folder_path.name)
        print(f"Found folder: {folder_path.name}")

print(f"\nTotal folders found: {len(folders_found)}")
print("First 10 folders:")
for i, folder in enumerate(folders_found[:10]):
    print(f"  {i+1}. {folder}")

if len(folders_found) > 10:
    print(f"  ... and {len(folders_found) - 10} more folders") 