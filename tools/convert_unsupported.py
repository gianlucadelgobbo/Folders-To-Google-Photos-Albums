#!/usr/bin/env python3
"""
Converte FLV, F4V, SWF in MP4 e li rimette nella cartella LPM originale.
Il file originale viene eliminato solo se la conversione va a buon fine.
Le cartelle vuote in _UNSUPPORTED vengono rimosse alla fine.

Usage:
    python3 tools/convert_unsupported.py \
        --path "/path/to/_UNSUPPORTED" \
        --dest "/path/to/LPM" \
        [--dry-run]
"""

import argparse
import subprocess
import sys
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--path',    required=True, help='Cartella _UNSUPPORTED da processare')
parser.add_argument('--dest',    required=True, help='LPM root dove rimettere i file convertiti')
parser.add_argument('--dry-run', action='store_true', help='Mostra cosa farebbe senza convertire')
args = parser.parse_args()

ROOT    = Path(args.path)
DEST    = Path(args.dest)
DRY_RUN = args.dry_run

for p in (ROOT, DEST):
    if not p.is_dir():
        print(f'[ERROR] Not a directory: {p}', file=sys.stderr)
        sys.exit(1)

EXTS = {'.flv', '.f4v', '.swf'}

files = sorted(f for f in ROOT.rglob('*') if f.is_file() and f.suffix.lower() in EXTS)
print(f'[INIT] {len(files)} file da convertire\n')

if DRY_RUN:
    print('[DRY-RUN] Nessun file verrà convertito.\n')

total_ok = total_fail = total_skip = 0

for src in files:
    folder_name = src.parent.name
    dest_folder = DEST / folder_name
    dest_file   = dest_folder / src.with_suffix('.mp4').name

    if dest_file.exists() and dest_file.stat().st_size > 0:
        print(f'  [SKIP] già in LPM: {folder_name}/{dest_file.name}')
        if not DRY_RUN:
            src.unlink()
        total_skip += 1
        continue

    print(f'  [CONVERT] {folder_name}/{src.name}')
    print(f'    TO  {dest_file}')

    if DRY_RUN:
        print(f'    → DRY-RUN: non convertito')
        continue

    tmp = src.with_suffix('.mp4')
    ext = src.suffix.lower()
    if ext == '.f4v':
        cmd = ['ffmpeg', '-y', '-i', str(src), '-c', 'copy', str(tmp)]
    else:
        cmd = ['ffmpeg', '-y', '-i', str(src),
               '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
               '-c:v', 'libx264', '-c:a', 'aac', str(tmp)]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0 and tmp.exists() and tmp.stat().st_size > 0:
        dest_folder.mkdir(parents=True, exist_ok=True)
        tmp.rename(dest_file)
        src.unlink()
        print(f'    → CONVERTED OK  ({dest_file.stat().st_size // 1024} KB)')
        total_ok += 1
    else:
        print(f'    → FAILED')
        if result.stderr:
            last = [l for l in result.stderr.splitlines() if l.strip()]
            if last:
                print(f'    → ffmpeg: {last[-1]}')
        if tmp.exists():
            tmp.unlink()
        total_fail += 1

# Rimuovi cartelle vuote da _UNSUPPORTED
total_empty = 0
for folder in sorted(ROOT.iterdir()):
    if folder.is_dir() and not any(f for f in folder.iterdir() if not f.name.startswith('.')):
        print(f'  [RMDIR] {folder.name}')
        if not DRY_RUN:
            for f in folder.iterdir():
                f.unlink()
            folder.rmdir()
        total_empty += 1

print(f'\n[DONE] convertiti={total_ok}  falliti={total_fail}  saltati={total_skip}  cartelle_vuote={total_empty}')
