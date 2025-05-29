# Folders to Google Photos Albums

A Python script to upload large batches of photos and videos to Google Photos, organized by folders into albums. It supports retry logic, upload state tracking, and automatic album creation.

## Features

- Processes folders and uploads media files
- Creates Google Photos albums based on folder names
- Retries failed uploads and logs them in a structured JSON file
- Tracks already uploaded files to avoid duplication
- Skips files larger than 10GB (Google Photos API limitation)
- Supports a `--retry-failed` mode to reprocess only previously failed uploads

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

## Usage

### Basic Upload
```bash
python3 gphotos_uploader.py --path "/absolute/path/to/photos-folders"
```

### Additional Options

```bash
# Create albums without uploading files
python3 gphotos_uploader.py --path "/path/to/folders" --create-albums-only

# Fix EXIF dates using folder names
python3 gphotos_uploader.py --path "/path/to/folders" --update-exif-from-folder-if-mismatch

# Simulate actions without making changes
python3 gphotos_uploader.py --path "/path/to/folders" --dry-run
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

### Retry Failed Uploads Only

```bash
python3 gphotos_uploader.py --path "/absolute/path/to/photos-folders" --retry-failed
```

This will:

- Load `failed_uploads.json`
- Attempt to re-upload failed files
- Remove successfully reprocessed files from the failure list

### Listener Mode

```bash
python3 gphotos_uploader.py --path "/path/to/folders" --listener
```

Continuously monitors the `ExifErrors` category in `failed_uploads.json` and attempts to process new files as they appear.

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

## Notes

- Max file size is 10GB due to Google Photos API limitations
- Albums are created using folder names (truncated to 100 characters if needed)
- You must manually approve the OAuth2 access in the browser on the first run

## License

MIT License – see `LICENSE`

## Author

Developed by Gianluca Del Gobbo. Contributions are welcome.
