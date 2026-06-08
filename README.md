# Folders to Google Photos Albums

A Python script to upload large batches of photos and videos to Google Photos, organized by folders into albums. It supports retry logic, upload state tracking, and automatic album creation.

## Features

- Processes folders and uploads media files
- Creates Google Photos albums based on folder names
- Retries failed uploads and logs them in a structured JSON file
- Tracks already uploaded files to avoid duplication
- Skips files larger than 10GB (Google Photos API limitation)
- Supports a `--retry-failed` mode to reprocess only previously failed uploads
- Can fix EXIF and filesystem dates using folder names

## Supported File Types

- Image files: `.jpg`, `.jpeg`, `.png`, `.heic`, `.gif`, `.webp`, `.bmp`, etc.
- Video files: `.mp4`, `.mov`, `.m4v`, `.avi`, etc.

These are passed directly to the Google Photos API; supported formats depend on Google's official documentation.

## Requirements

- Python 3.9+
- A Google Cloud Project with OAuth2 credentials (download `credentials.json`)
- Install dependencies with:

```bash
pip install -r requirements.txt
```

## Starting a New Batch

Follow these steps each time you start uploading a new folder.

### 1. Triage — separate problematic files

```bash
python3 tools/triage_files.py --path "/path/to/folder" --dry-run
python3 tools/triage_files.py --path "/path/to/folder"
```

Moves files into dedicated subfolders:
- `_UNSUPPORTED/` — unsupported formats (FLV, F4V, SWF)
- `_TOOSMALL/` — empty files (0 bytes)
- `_TOOLARGE/` — files over 10 GB

### 2. Convert unsupported files (if any)

```bash
python3 tools/convert_unsupported.py \
    --path "/path/to/folder/_UNSUPPORTED" \
    --dest "/path/to/folder" \
    --dry-run

python3 tools/convert_unsupported.py \
    --path "/path/to/folder/_UNSUPPORTED" \
    --dest "/path/to/folder"
```

Converts FLV/F4V/SWF to MP4 and moves them back into the original subfolders.

### 3. Upload

```bash
python3 gphotos_uploader.py --path "/path/to/folder"
```

### 4. Retry failed uploads (if needed)

```bash
python3 gphotos_uploader.py --path "/path/to/folder" --retry-failed
```

### State files between batches

- `upload_state.json` — auto-cleans entries pointing to paths that no longer exist on startup; safe to reuse across batches.
- `galbum_cache.json` — global cache of your Google Photos albums; safe to reuse.
- `failed_uploads.json` — accumulates failures across batches. Before starting a new batch, either retry remaining failures from the previous one, or archive the file and start fresh:
  ```bash
  mv failed_uploads.json failed_uploads_prev.json
  echo '{"UploadError":{},"AddToAlbumError":{},"TooLarge":{},"ExifErrors":{},"UnsupportedFormat":{}}' > failed_uploads.json
  ```

## Usage

### Basic Upload
```bash
python3 gphotos_uploader.py --path "/absolute/path/to/photos-folders"
```

### Additional Options

```bash
# Fix EXIF and filesystem dates using folder names
python3 gphotos_uploader.py --path "/path/to/folders" --fix-dates

# Simulate actions without making changes
python3 gphotos_uploader.py --path "/path/to/folders" --dry-run

# Enable detailed debug logging
python3 gphotos_uploader.py --path "/path/to/folders" --debug

# Retry failed uploads
python3 gphotos_uploader.py --path "/path/to/folders" --retry-failed

# Combine options
python3 gphotos_uploader.py --path "/path/to/folders" --fix-dates --dry-run --debug
```

### Folder Structure

```bash
/absolute/path/to/photos-folders/
    ├── Album Folder 1/
    │   ├── IMG_0001.JPG
    │   └── VID_0001.MOV
    ├── Album Folder 2/
    │   └── IMG_1000.JPG
```

This will:

- Create an album for each subfolder with subfolder naming
- Upload all files not yet uploaded
- Track progress in `upload_state.json`

## State Management

- `upload_state.json` is updated immediately after each successful file upload
- Files are tracked by name in their respective album entries
- State is preserved between runs to prevent duplicate uploads

## Error Handling

- Failed uploads are categorized by error type in `failed_uploads.json`:
  - `UploadError`: Failed to upload file
  - `AddToAlbumError`: Failed to add file to album
  - `TooLarge`: Files exceeding 10GB limit
  - `ExifErrors`: Issues with EXIF data
  - `UnsupportedFormat`: Unsupported file types

## Files

- `upload_state.json`: tracks uploaded files and album IDs
- `failed_uploads.json`: stores failed uploads by error type
- `upload.log`: detailed log of all actions and errors
- `credentials.json`: your OAuth2 credentials (ignored via `.gitignore`)

## Utilities

See the [utilities README](utilities/README.md) for additional tools and functions:
- EXIF management
- File timestamp synchronization
- Album cache management
- Date extraction from folder names

## Notes

- Max file size is 10GB due to Google Photos API limitations
- Albums are created using folder names (truncated to 100 characters if needed)
- You must manually approve the OAuth2 access in the browser on the first run
- The script uses async/await for better performance
- Rate limiting is handled automatically with exponential backoff

## License

MIT License – see `LICENSE`

## Author

Developed by Gianluca Del Gobbo. Contributions are welcome.
