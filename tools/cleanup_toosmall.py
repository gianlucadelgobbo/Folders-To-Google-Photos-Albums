#!/usr/bin/env python3
"""
Elimina da _TOOSMALL i file 0-byte che sono già stati ripristinati in LPM.

Per ogni file in _TOOSMALL/<folder>/<file>:
  - Se DEST/<folder>/<file> esiste e ha size > 0 → elimina il 0-byte da _TOOSMALL
  - Altrimenti → lascia stare

Usage:
    python3 tools/cleanup_toosmall.py \
        --toosmall "/path/to/_TOOSMALL" \
        --dest     "/path/to/LPM" \
        [--dry-run]
"""

import argparse
import sys
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--toosmall', required=True, help='Path to _TOOSMALL folder')
parser.add_argument('--dest',     required=True, help='LPM root where files were restored')
parser.add_argument('--dry-run',  action='store_true', help='Mostra cosa farebbe senza eliminare')
args = parser.parse_args()

TOOSMALL = Path(args.toosmall)
DEST     = Path(args.dest)
DRY_RUN  = args.dry_run

for p in (TOOSMALL, DEST):
    if not p.is_dir():
        print(f'[ERROR] Not a directory: {p}', file=sys.stderr)
        sys.exit(1)

if DRY_RUN:
    print('[DRY-RUN] Nessun file verrà eliminato.\n')

total_deleted = total_skipped = total_missing = 0

for folder in sorted(TOOSMALL.iterdir()):
    if not folder.is_dir():
        continue
    for file in sorted(folder.iterdir()):
        if not file.is_file() or file.name.startswith('.'):
            continue

        dest_file = DEST / folder.name / file.name
        if dest_file.exists() and dest_file.stat().st_size > 0:
            print(f'  [DELETE] {folder.name}/{file.name}  (ripristinato in LPM)')
            if not DRY_RUN:
                file.unlink()
            total_deleted += 1
        else:
            total_skipped += 1

print(f'\n[DONE] eliminati={total_deleted}  lasciati={total_skipped}')
