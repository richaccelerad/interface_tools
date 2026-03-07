"""
drawing_scanner.py — Scan Box.com drawings folder via API and upsert into DB.

Usage
-----
python drawing_scanner.py --init
python drawing_scanner.py --scan [--verbose]
python drawing_scanner.py --stats
python drawing_scanner.py --lookup 100100
python drawing_scanner.py -h
"""

from __future__ import annotations

import os
import re
import sys
import time
from typing import Optional

from box_sdk_gen import BoxCCGAuth, BoxClient, CCGConfig

from drawing_db import DrawingDatabase

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Matches the part folder naming conventions:
#   100100_Window Ceramic          (6-digit numeric)
#   F-102726_ETM Target Insulator  (letter prefix + dash + digits)
PART_FOLDER_RE = re.compile(r"^(\d{6}|[A-Z]-\d+)([ _](.+))?$")

# Extracts revision from a filename stem
# Handles: _Rev C, _rev X1, _RevB, _X2, space variants
REVISION_RE = re.compile(r"[_ ](?:[Rr]ev\s?)?([A-Z]|[Xx]\d+)\s*$")

# Seconds to sleep between sub-folder API calls to avoid Box rate limits
SCAN_SLEEP_SECS = 0.05

# File extensions -> column name mapping (lowercase)
FILE_TYPE_MAP = {
    ".pdf":    "pdf_file_id",
    ".step":   "step_file_id",
    ".stp":    "step_file_id",
    ".slddrw": "slddrw_file_id",
    ".sldprt": "sldprt_file_id",
    ".sldasm": "sldasm_file_id",
}

# ---------------------------------------------------------------------------
# Revision helpers
# ---------------------------------------------------------------------------

def parse_revision(stem: str) -> Optional[str]:
    """Extract revision code from a filename stem. Returns uppercase or None."""
    m = REVISION_RE.search(stem)
    if m:
        return m.group(1).upper()
    return None


def _revision_sort_key(rev: Optional[str]):
    if rev is None:
        return (0, 0, "")
    if len(rev) == 1 and rev.isalpha():
        return (2, 0, rev)
    if rev.startswith("X") and rev[1:].isdigit():
        return (1, int(rev[1:]), "")
    return (2, 0, rev)


def is_letter_revision(rev: str) -> bool:
    return len(rev) == 1 and rev.isalpha()


def determine_latest(revisions: list[Optional[str]]) -> Optional[str]:
    """
    Return whichever revision should be marked is_latest.
    Letter revisions outrank x-revisions; highest within each tier wins.
    """
    non_none = [r for r in revisions if r is not None]
    if not non_none:
        return None
    letter_revs = [r for r in non_none if is_letter_revision(r)]
    if letter_revs:
        return max(letter_revs)
    return max(non_none, key=_revision_sort_key)


# ---------------------------------------------------------------------------
# Box API helpers
# ---------------------------------------------------------------------------

def make_box_client(client_id: str, client_secret: str, enterprise_id: str) -> BoxClient:
    config = CCGConfig(
        client_id=client_id,
        client_secret=client_secret,
        enterprise_id=enterprise_id,
    )
    return BoxClient(BoxCCGAuth(config))


def list_folder_items(client: BoxClient, folder_id: str) -> list:
    """Return all items in a Box folder, handling marker-based pagination."""
    items = []
    marker = None
    while True:
        page = client.folders.get_folder_items(
            folder_id, limit=1000, usemarker=True, marker=marker
        )
        if page.entries:
            items.extend(page.entries)
        marker = page.next_marker
        if not marker:
            break
    return items


def scan_part_folder_box(
    client: BoxClient,
    folder_id: str,
    folder_name: str,
    verbose: bool = False,
) -> dict:
    """
    List top-level files in a Box part folder.
    Returns dict: revision (or None) -> {column: box_file_id}
    """
    rev_groups: dict[Optional[str], dict[str, str]] = {}

    try:
        items = list_folder_items(client, folder_id)
    except Exception as exc:
        print(f"  [WARN] Cannot list {folder_name}: {exc}", file=sys.stderr)
        return rev_groups

    for item in items:
        if item.type.value != "file":
            continue  # skip sub-folders

        name = item.name
        # Split extension (last dot only)
        dot = name.rfind(".")
        if dot == -1:
            continue
        ext = name[dot:].lower()
        col = FILE_TYPE_MAP.get(ext)
        if col is None:
            continue

        stem = name[:dot]
        rev = parse_revision(stem)

        if verbose:
            print(f"    {name}  ->  rev={rev!r}  col={col}")

        if rev not in rev_groups:
            rev_groups[rev] = {}
        if col not in rev_groups[rev]:
            rev_groups[rev][col] = item.id  # store Box file ID

    return rev_groups


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------

def scan_drawings_folder(
    client: BoxClient,
    drawings_folder_id: str,
    db: DrawingDatabase,
    verbose: bool = False,
    new_only: bool = False,
    sleep_secs: float = SCAN_SLEEP_SECS,
) -> tuple[int, int, int]:
    """
    List the top-level Box drawings folder, find part sub-folders,
    and upsert records into *db*.

    new_only: skip folders whose part number is already in the DB.
    Returns (parts_processed, file_ids_stored, parts_skipped).
    """
    parts_processed = 0
    file_ids_stored = 0
    parts_skipped = 0

    print("Fetching top-level folder listing from Box...")
    try:
        top_items = list(list_folder_items(client, drawings_folder_id))
    except Exception as exc:
        print(f"ERROR: Cannot list Box folder {drawings_folder_id}: {exc}", file=sys.stderr)
        sys.exit(1)

    # Sort by name for deterministic output
    top_items.sort(key=lambda i: i.name)

    box_total = sum(1 for i in top_items if i.type.value == "folder" and PART_FOLDER_RE.match(i.name))

    # Fetch existing parts once for count display and --new-only filtering
    existing_parts_list = db.get_all_parts()
    db_count = len(existing_parts_list)
    existing_part_nums: set[str] = {p.part_num for p in existing_parts_list} if new_only else set()

    to_scan = box_total - db_count if new_only else box_total
    print(f"Box: {box_total} part folders | DB: {db_count} scanned | To process: {to_scan}")
    if new_only and db_count >= box_total:
        print("All Box folders are already in the database.")
        return 0, 0, db_count

    done = 0
    for item in top_items:
        if item.type.value != "folder":
            continue
        m = PART_FOLDER_RE.match(item.name)
        if not m:
            if verbose:
                print(f"  [SKIP] {item.name}")
            continue

        part_num = m.group(1)

        if new_only and part_num in existing_part_nums:
            parts_skipped += 1
            continue

        description = m.group(3) or None

        if verbose:
            print(f"\n[PART] {item.name}")

        rev_groups = scan_part_folder_box(client, item.id, item.name, verbose=verbose)

        all_revisions = list(rev_groups.keys())
        latest_rev = determine_latest(all_revisions)

        part_id = db.upsert_part(
            part_num=part_num,
            description=description,
            box_folder_id=item.id,
            latest_revision=latest_rev,
        )

        for rev, file_ids in rev_groups.items():
            is_latest = (rev == latest_rev) and (rev is not None or len(rev_groups) == 1)
            if rev is None and latest_rev is not None:
                is_latest = False
            is_released = (rev is not None and is_letter_revision(rev))

            db.upsert_drawing(
                part_id=part_id,
                revision=rev,
                is_latest=is_latest,
                is_released=is_released,
                pdf_file_id=file_ids.get("pdf_file_id"),
                step_file_id=file_ids.get("step_file_id"),
                slddrw_file_id=file_ids.get("slddrw_file_id"),
                sldprt_file_id=file_ids.get("sldprt_file_id"),
                sldasm_file_id=file_ids.get("sldasm_file_id"),
            )
            file_ids_stored += sum(1 for v in file_ids.values() if v is not None)

        parts_processed += 1
        done += 1
        if not verbose and done % 50 == 0:
            print(f"  {done}/{to_scan} new parts scanned...")

        if sleep_secs > 0:
            time.sleep(sleep_secs)

    return parts_processed, file_ids_stored, parts_skipped


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------

def print_help() -> None:
    print(
        """drawing_scanner.py — Scan Box drawings folder via API and manage DB

Options:
  --init                Create/migrate database tables
  --scan                Scan Box drawings folder and upsert records
  --new-only            With --scan: skip folders already in the DB (faster catch-up)
  --sleep SECS          Seconds to sleep between folder API calls (default: 0.05)
  --folder-id ID        Override Box drawings folder ID (default: from config)
  --db URL              Override database URL (default: from config)
  --stats               Print summary counts from DB and Box scan completeness
  --lookup PARTNUM      Show all DB records for a part number
  --verbose             Print each file found during scan
  -h, --help            Show this help message
"""
    )


def cmd_init(db: DrawingDatabase) -> None:
    db.init_schema()
    print("Database tables created/verified.")


def cmd_scan(
    db: DrawingDatabase,
    client: BoxClient,
    folder_id: str,
    verbose: bool,
    new_only: bool = False,
    sleep_secs: float = SCAN_SLEEP_SECS,
) -> None:
    parts, file_ids, skipped = scan_drawings_folder(
        client, folder_id, db, verbose=verbose, new_only=new_only, sleep_secs=sleep_secs
    )
    msg = f"Done. New parts scanned: {parts}, file IDs stored: {file_ids}"
    if skipped:
        msg += f", already-in-DB skipped: {skipped}"
    print(msg)


def cmd_stats(
    db: DrawingDatabase,
    client: Optional[BoxClient] = None,
    folder_id: Optional[str] = None,
) -> None:
    stats = db.get_stats()
    print("Database statistics:")
    print(f"  Parts in DB:           {stats['total_parts']}")
    print(f"  Total drawing rows:    {stats['total_drawing_rows']}")
    print(f"  Latest revs with PDF:  {stats['latest_with_pdf']}")
    print(f"  Latest revs with STEP: {stats['latest_with_step']}")
    print(f"  Released revisions:    {stats['released_revisions']}")

    if client and folder_id:
        print("\nFetching Box folder count for completeness check...")
        try:
            top_items = list_folder_items(client, folder_id)
            box_total = sum(
                1 for i in top_items
                if i.type.value == "folder" and PART_FOLDER_RE.match(i.name)
            )
            db_count = stats["total_parts"]
            missing = box_total - db_count
            pct = db_count / box_total * 100 if box_total else 0
            print(f"\nScan completeness:")
            print(f"  Box part folders:      {box_total}")
            print(f"  DB coverage:           {db_count}/{box_total} ({pct:.1f}%)")
            if missing > 0:
                print(f"  Unscanned folders:     {missing}  (run --scan --new-only to add them)")
            else:
                print("  Scan is complete.")
        except Exception as exc:
            print(f"  [Could not fetch Box total: {exc}]", file=sys.stderr)


def cmd_lookup(db: DrawingDatabase, part_num: str) -> None:
    part = db.get_part(part_num)
    if part is None:
        print(f"Part {part_num!r} not found in database.")
        return

    print(f"Part:         {part.part_num}")
    print(f"Description:  {part.description}")
    print(f"Box folder:   {part.box_folder_id}")
    print(f"Latest rev:   {part.latest_revision}")
    print(f"Scanned at:   {part.scanned_at}")
    print()

    drawings = db.get_all_drawings_for_part(part_num)
    if not drawings:
        print("  (no drawing rows)")
        return

    for d in drawings:
        latest_flag = " [LATEST]" if d.is_latest else ""
        released_flag = " [RELEASED]" if d.is_released else ""
        print(f"  Revision: {d.revision!r}{latest_flag}{released_flag}")
        for attr, label in [
            ("pdf_file_id",    "    PDF:   "),
            ("step_file_id",   "    STEP:  "),
            ("slddrw_file_id", "    SLDDRW:"),
            ("sldprt_file_id", "    SLDPRT:"),
            ("sldasm_file_id", "    SLDASM:"),
        ]:
            val = getattr(d, attr)
            if val:
                print(f"  {label} {val}")
        print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _get_arg(args: list[str], flag: str) -> Optional[str]:
    """Return the value after *flag* in *args*, or None."""
    if flag in args:
        idx = args.index(flag)
        if idx + 1 >= len(args):
            print(f"ERROR: {flag} requires an argument.", file=sys.stderr)
            sys.exit(1)
        return args[idx + 1]
    return None


def main() -> None:
    args = sys.argv[1:]

    if not args or "-h" in args or "--help" in args:
        print_help()
        return

    verbose    = "--verbose"  in args
    do_init    = "--init"     in args
    do_scan    = "--scan"     in args
    do_stats   = "--stats"    in args
    new_only   = "--new-only" in args
    lookup_part = _get_arg(args, "--lookup")
    db_url      = _get_arg(args, "--db")
    folder_id_override = _get_arg(args, "--folder-id")
    sleep_str   = _get_arg(args, "--sleep")
    sleep_secs  = float(sleep_str) if sleep_str is not None else SCAN_SLEEP_SECS

    # Resolve database URL: flag > env var > config > default
    if db_url is None:
        db_url = os.environ.get("DATABASE_URL")
    if db_url is None:
        try:
            from config import DATABASE_URL as _url
            db_url = _url
        except (ImportError, AttributeError):
            db_url = "sqlite:///drawings.db"

    print(f"Database: {db_url}")
    db = DrawingDatabase(db_url)

    if do_init:
        cmd_init(db)

    if do_scan:
        # Load Box credentials from config
        try:
            from config import (
                BOX_CLIENT_ID,
                BOX_CLIENT_SECRET,
                BOX_ENTERPRISE_ID,
                BOX_DRAWINGS_FOLDER_ID,
            )
        except (ImportError, AttributeError) as exc:
            print(f"ERROR: Missing Box config: {exc}", file=sys.stderr)
            sys.exit(1)

        folder_id = folder_id_override or BOX_DRAWINGS_FOLDER_ID

        print("Authenticating with Box...")
        client = make_box_client(BOX_CLIENT_ID, BOX_CLIENT_SECRET, BOX_ENTERPRISE_ID)
        cmd_scan(db, client, folder_id, verbose, new_only=new_only, sleep_secs=sleep_secs)

    if do_stats:
        # Try to load Box credentials so --stats can show scan completeness
        box_client_for_stats = None
        box_folder_for_stats = None
        try:
            from config import (
                BOX_CLIENT_ID,
                BOX_CLIENT_SECRET,
                BOX_ENTERPRISE_ID,
                BOX_DRAWINGS_FOLDER_ID,
            )
            box_client_for_stats = make_box_client(BOX_CLIENT_ID, BOX_CLIENT_SECRET, BOX_ENTERPRISE_ID)
            box_folder_for_stats = folder_id_override or BOX_DRAWINGS_FOLDER_ID
        except (ImportError, AttributeError):
            pass
        cmd_stats(db, box_client_for_stats, box_folder_for_stats)

    if lookup_part:
        cmd_lookup(db, lookup_part)

    if not any([do_init, do_scan, do_stats, lookup_part]):
        print("No action specified. Use -h for help.")


if __name__ == "__main__":
    main()
