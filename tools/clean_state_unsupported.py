#!/usr/bin/env python3
"""
Remove files with unsupported Google Photos formats from upload_state.json.

Run from the project root:
    python3 utilities/clean_state_unsupported.py [--dry-run]

Formats removed: .flv, .f4v, .swf (same blocklist as gphotos_uploader.py).
After running, gphotos_uploader.py will re-encounter those files and move them
to _UNSUPPORTED/ instead of silently skipping them.
"""

import argparse
import json
import mimetypes
import sys
from pathlib import Path

STATE_FILE = Path("upload_state.json")

GPHOTOS_UNSUPPORTED_EXTS = {".flv", ".f4v", ".swf"}


def is_supported_media(filename: str) -> bool:
    ext = Path(filename).suffix.lower()
    if ext in GPHOTOS_UNSUPPORTED_EXTS:
        return False
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type:
        return mime_type.startswith("image/") or mime_type.startswith("video/")
    supported_exts = {
        ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp",
        ".heic", ".heif", ".raw", ".cr2", ".nef", ".rw2",
        ".mp4", ".mov", ".avi", ".mkv", ".wmv", ".webm",
        ".m4v", ".3gp", ".3g2", ".mts", ".m2ts", ".wm",
    }
    return ext in supported_exts


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Show what would be removed without modifying the file")
    args = parser.parse_args()

    if not STATE_FILE.exists():
        print(f"❌ {STATE_FILE} not found. Run from the project root.")
        sys.exit(1)

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)

    total_removed = 0
    for folder_name, folder_state in state.items():
        original = folder_state.get("files", [])
        clean = [f for f in original if is_supported_media(f)]
        removed = [f for f in original if f not in clean]
        if removed:
            for filename in removed:
                print(f"  [{folder_name}] {filename}")
            total_removed += len(removed)
            if not args.dry_run:
                folder_state["files"] = clean

    if total_removed == 0:
        print("✅ No unsupported files found in upload_state.json")
        return

    if args.dry_run:
        print(f"\n[DRY-RUN] Would remove {total_removed} file(s) from upload_state.json")
    else:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        print(f"\n✅ Removed {total_removed} unsupported file(s) from {STATE_FILE}")
        print("   Run gphotos_uploader.py again to move them to _UNSUPPORTED/")


if __name__ == "__main__":
    main()
