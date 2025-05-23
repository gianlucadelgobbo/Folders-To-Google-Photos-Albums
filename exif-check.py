# usage:
# Check only: python3 exif-check.py "/your/folder"
# Fix + update failed_uploads.json live: python3 exif-check.py "/your/folder" --save

import os
import sys
import re
import json
import argparse
import subprocess
from datetime import datetime
from PIL import Image
from PIL.ExifTags import TAGS
import piexif

EXIF_ANOMALIES_FILE = "exif-anomalies.txt"
FAILED_UPLOADS_FILE = "failed_uploads.json"

def extract_folder_date(folder_name):
    match = re.search(r'(\d{4})[-_]?(\d{2})', folder_name)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None

def get_datetime_original(image_path):
    ext = os.path.splitext(image_path)[1].lower()

    if ext in ('.jpg', '.jpeg'):
        try:
            with Image.open(image_path) as img:
                exif_data = img._getexif()
                if exif_data:
                    for tag_id, value in exif_data.items():
                        tag = TAGS.get(tag_id, tag_id)
                        if tag == 'DateTimeOriginal':
                            return datetime.strptime(value, '%Y:%m:%d %H:%M:%S')
        except Exception as e:
            print(f"[ERROR] Could not read EXIF from {image_path}: {e}")
        return None

    try:
        result = subprocess.run(
            ["exiftool", "-s", "-s", "-s", "-DateTimeOriginal", image_path],
            capture_output=True,
            text=True,
            check=True,
            timeout=5
        )
        output = result.stdout.strip()
        if output:
            return datetime.strptime(output, '%Y:%m:%d %H:%M:%S')
    except subprocess.TimeoutExpired:
        print(f"[TIMEOUT] exiftool timed out on {image_path}")
    except Exception as e:
        print(f"[ERROR] exiftool failed on {image_path}: {e}")
    return None

def update_exif_datetime(image_path, new_datetime):
    dt_str = new_datetime.strftime("%Y:%m:%d %H:%M:%S")
    ext = os.path.splitext(image_path)[1].lower()

    if ext in ('.jpg', '.jpeg'):
        try:
            exif_dict = piexif.load(image_path)
            exif_dict["Exif"][piexif.ExifIFD.DateTimeOriginal] = dt_str.encode()
            exif_dict["Exif"][piexif.ExifIFD.DateTimeDigitized] = dt_str.encode()
            exif_bytes = piexif.dump(exif_dict)
            piexif.insert(exif_bytes, image_path)
            print(f"  🔄 EXIF updated (piexif): {image_path}")
        except Exception as e:
            print(f"[ERROR] Could not update EXIF in {image_path}: {e}")
    elif ext in ('.heic', '.heif', '.cr2', '.tif', '.tiff'):
        try:
            subprocess.run([
                "exiftool",
                "-overwrite_original",
                f"-DateTimeOriginal={dt_str}",
                f"-CreateDate={dt_str}",
                f"-ModifyDate={dt_str}",
                image_path
            ], check=True)
            print(f"  🔄 EXIF updated (exiftool): {image_path}")
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] exiftool failed on {image_path}: {e}")
    else:
        print(f"[SKIP] Format not supported for EXIF writing: {image_path}")

def append_to_failed_uploads(file_path):
    folder = os.path.basename(os.path.dirname(file_path))
    folder_path = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)

    if os.path.exists(FAILED_UPLOADS_FILE):
        with open(FAILED_UPLOADS_FILE, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"[ERROR] failed_uploads.json is corrupted. Creating new.")
                data = {}
    else:
        data = {}

    data.setdefault("ExifErrors", {})
    if folder not in data["ExifErrors"]:
        data["ExifErrors"][folder] = {"path": folder_path, "files": []}
    if file_name not in data["ExifErrors"][folder]["files"]:
        data["ExifErrors"][folder]["files"].append(file_name)

    with open(FAILED_UPLOADS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"[INFO] Appended to failed_uploads.json → {file_name}")

def process_directory(root_dir, save=False):
    anomalies = []

    supported_read = ('.jpg', '.jpeg', '.png', '.tif', '.tiff', '.heic', '.cr2', '.heif')

    for dirpath, _, filenames in os.walk(root_dir):
        year, month = extract_folder_date(os.path.basename(dirpath))
        if not year:
            continue
        print(f"🔍 Scanning {dirpath}")

        for filename in filenames:
            if not filename.lower().endswith(supported_read):
                continue

            file_path = os.path.join(dirpath, filename)
            print(f"Checking {filename}")
            exif_dt = get_datetime_original(file_path)
            if not exif_dt:
                print(f"  ⛔ No EXIF datetime found — skipped")
                continue
            else:
                print(f"  ✅ EXIF datetime found: {exif_dt}")

            if exif_dt.year != year or exif_dt.month != month:
                try:
                    new_dt = exif_dt.replace(year=year, month=month)
                except ValueError:
                    new_dt = exif_dt.replace(year=year, month=month, day=1)
                print(f"[MISMATCH] {file_path} | EXIF: {exif_dt} → FIX: {new_dt}")
                anomalies.append((file_path, new_dt))
                if save:
                    update_exif_datetime(file_path, new_dt)
                    append_to_failed_uploads(file_path)

    if not save:
        with open(EXIF_ANOMALIES_FILE, "w") as f:
            for path, new_dt in anomalies:
                f.write(f"{path} | FIX: {new_dt}\n")
        print(f"[INFO] Wrote EXIF anomalies to '{EXIF_ANOMALIES_FILE}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Directory to scan")
    parser.add_argument("--save", action="store_true", help="Apply EXIF fixes and update failed_uploads.json")
    args = parser.parse_args()

    if not os.path.isdir(args.path):
        print(f"[ERROR] Invalid directory: {args.path}")
        sys.exit(1)

    process_directory(args.path, save=args.save)
