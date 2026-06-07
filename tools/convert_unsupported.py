#!/usr/bin/env python3
"""
Converte FLV, F4V, SWF in MP4 nella stessa cartella.
Il file originale viene eliminato solo se la conversione va a buon fine.

Usage:
    python3 tools/convert_unsupported.py --path "/path/to/_UNSUPPORTED" [--dry-run]
"""

import argparse
import subprocess
import sys
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--path',    required=True, help='Cartella _UNSUPPORTED da processare')
parser.add_argument('--dry-run', action='store_true', help='Mostra cosa farebbe senza convertire')
args = parser.parse_args()

ROOT    = Path(args.path)
DRY_RUN = args.dry_run

if not ROOT.is_dir():
    print(f'[ERROR] Not a directory: {ROOT}', file=sys.stderr)
    sys.exit(1)

EXTS = {'.flv', '.f4v', '.swf'}

files = sorted(f for f in ROOT.rglob('*') if f.is_file() and f.suffix.lower() in EXTS)
print(f'[INIT] {len(files)} file da convertire\n')

if DRY_RUN:
    print('[DRY-RUN] Nessun file verrà convertito.\n')

total_ok = total_fail = total_skip = 0

for src in files:
    dest = src.with_suffix('.mp4')

    if dest.exists() and dest.stat().st_size > 0:
        print(f'  [SKIP] già esiste: {src.name}')
        total_skip += 1
        continue

    print(f'  [CONVERT] {src.relative_to(ROOT)}')

    if DRY_RUN:
        print(f'    → DRY-RUN')
        continue

    ext = src.suffix.lower()
    if ext == '.f4v':
        cmd = ['ffmpeg', '-y', '-i', str(src), '-c', 'copy', str(dest)]
    else:
        cmd = ['ffmpeg', '-y', '-i', str(src),
               '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
               '-c:v', 'libx264', '-c:a', 'aac', str(dest)]

    result = subprocess.run(cmd, capture_output=True)
    if result.returncode == 0 and dest.exists() and dest.stat().st_size > 0:
        print(f'    → OK  ({dest.stat().st_size // 1024} KB)')
        src.unlink()
        total_ok += 1
    else:
        print(f'    → FAIL')
        if dest.exists():
            dest.unlink()
        total_fail += 1

print(f'\n[DONE] convertiti={total_ok}  falliti={total_fail}  saltati={total_skip}')
