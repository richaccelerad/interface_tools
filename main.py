"""
Main program: Sync PO data from Epicor to Monday.com

Reads PartNum values from a Monday.com board, looks up associated POs
in Epicor, and displays the results.
"""

from monday_client import MondayClient
from epicor_po_x2 import EpicorClient, POLineMatch, PartQtySummary
from datetime import datetime, timedelta
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import config
import sys


def get_monday_client() -> MondayClient:
    return MondayClient(config.MONDAY_API_TOKEN)


def get_epicor_client() -> EpicorClient:
    return EpicorClient(
        base_url=config.EPICOR_BASE_URL,
        company=config.EPICOR_COMPANY,
        plant=config.EPICOR_PLANT,
        api_key=config.EPICOR_API_KEY,
        username=config.EPICOR_USERNAME,
        password=config.EPICOR_PASSWORD,
        learn_missing_getrows_params=True,
    )


# Column definitions for PO tracking
PO_COLUMNS = {
    "part_description": {"title": "Part Description", "type": "text"},
    "qty_on_hand": {"title": "Qty On Hand", "type": "numbers"},
    "location": {"title": "Location", "type": "long_text"},
    "open_pos": {"title": "Open POs", "type": "long_text"},
    "closed_pos_recent": {"title": "Closed POs (Last 12 Mo)", "type": "long_text"},
    "closed_pos_old": {"title": "Closed POs (Older)", "type": "long_text"},
}


def configure_board(monday: MondayClient, board_id: str) -> dict[str, str]:
    """
    Configure a Monday.com board with the required columns for PO tracking.

    Creates columns if they don't exist:
    - Open POs
    - Closed POs (Last 12 Mo)
    - Closed POs (Older)

    Returns a dict mapping column keys to their IDs.
    """
    print(f"Configuring board {board_id}...")

    # Get existing columns
    existing_columns = monday.get_board_columns(board_id)
    existing_by_title = {col['title']: col['id'] for col in existing_columns}

    print(f"Existing columns: {list(existing_by_title.keys())}")

    column_ids = {}

    for key, col_def in PO_COLUMNS.items():
        title = col_def['title']
        col_type = col_def['type']

        if title in existing_by_title:
            column_ids[key] = existing_by_title[title]
            print(f"  Column '{title}' already exists (id: {column_ids[key]})")
        else:
            column_ids[key] = monday.create_column(board_id, title, col_type)
            print(f"  Created column '{title}' (id: {column_ids[key]})")

    print("Board configuration complete.")
    return column_ids


def format_date(date_str: Optional[str]) -> str:
    """Format an ISO date string to MM/DD/YYYY, or return empty string if None."""
    if not date_str:
        return ""
    try:
        # Handle ISO format like "2024-01-15T00:00:00Z"
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%m/%d/%Y")
    except (ValueError, AttributeError):
        return date_str[:10] if len(date_str) >= 10 else date_str


def format_po_line(po: POLineMatch) -> str:
    """Format a single PO line for display: PO#, Qty, Due Date, Vendor."""
    qty = int(po.order_qty) if po.order_qty is not None else "?"
    due = format_date(po.due_date) or "No date"
    vendor = po.vendor_name or "Unknown"
    return f"PO {po.po_num}: Qty {qty}, Due {due}, {vendor}"


def categorize_pos(po_lines: list[POLineMatch]) -> dict[str, list[POLineMatch]]:
    """
    Categorize PO lines into:
    - open: status == "open"
    - closed_recent: status == "closed" and order_date within last 12 months
    - closed_old: status == "closed" and order_date older than 12 months
    """
    now = datetime.now()
    twelve_months_ago = now - timedelta(days=365)

    result = {
        "open": [],
        "closed_recent": [],
        "closed_old": [],
    }

    for po in po_lines:
        if po.status == "open":
            result["open"].append(po)
        elif po.status == "closed":
            # Determine if recent or old based on order_date
            if po.order_date:
                try:
                    order_dt = datetime.fromisoformat(po.order_date.replace("Z", "+00:00"))
                    if order_dt >= twelve_months_ago:
                        result["closed_recent"].append(po)
                    else:
                        result["closed_old"].append(po)
                except (ValueError, AttributeError):
                    # Can't parse date, default to old
                    result["closed_old"].append(po)
            else:
                # No date, default to old
                result["closed_old"].append(po)

    return result


def format_po_column(po_lines: list[POLineMatch]) -> str:
    """Format a list of PO lines for a Monday.com column."""
    if not po_lines:
        return ""
    return "\n".join(format_po_line(po) for po in po_lines)


def get_column_ids(monday: MondayClient, board_id: str) -> dict[str, str]:
    """Get the column IDs for the PO columns on the board."""
    columns = monday.get_board_columns(board_id)
    column_map = {}

    for col in columns:
        title = col['title']
        if title == "Open POs":
            column_map["open_pos"] = col['id']
        elif title == "Closed POs (Last 12 Mo)":
            column_map["closed_pos_recent"] = col['id']
        elif title == "Closed POs (Older)":
            column_map["closed_pos_old"] = col['id']
        elif title == "Qty On Hand":
            column_map["qty_on_hand"] = col['id']
        elif title == "Location":
            column_map["location"] = col['id']
        elif title == "Part Description":
            column_map["part_description"] = col['id']

    return column_map


def format_inventory(inv_summary: PartQtySummary) -> str:
    """Format inventory details showing qty by location and job."""
    if not inv_summary.by_location:
        return ""

    lines = []
    for inv in inv_summary.by_location:
        qty = int(inv.on_hand_qty) if inv.on_hand_qty == int(inv.on_hand_qty) else inv.on_hand_qty
        location = f"{inv.warehouse}/{inv.bin_num}" if inv.bin_num else inv.warehouse
        if inv.job_num:
            lines.append(f"{qty} in {location} (Job {inv.job_num})")
        else:
            lines.append(f"{qty} in {location}")

    return "\n".join(lines)


def update_monday_item(monday: MondayClient, board_id: str, item_id: str,
                       column_ids: dict[str, str], categorized_pos: dict[str, list[POLineMatch]],
                       inventory: Optional[PartQtySummary] = None,
                       part_description: Optional[str] = None,
                       existing_values: Optional[dict[str, str]] = None) -> bool:
    """
    Update a Monday.com item with PO and inventory data.

    Only updates if values have changed from existing values.

    Args:
        existing_values: Dict of column_id -> current text value from Monday.com

    Returns:
        True if an update was made, False if skipped (no changes)
    """
    if existing_values is None:
        existing_values = {}

    # Build new values
    new_values = {}

    if "open_pos" in column_ids:
        new_values[column_ids["open_pos"]] = format_po_column(categorized_pos["open"])

    if "closed_pos_recent" in column_ids:
        new_values[column_ids["closed_pos_recent"]] = format_po_column(categorized_pos["closed_recent"])

    if "closed_pos_old" in column_ids:
        new_values[column_ids["closed_pos_old"]] = format_po_column(categorized_pos["closed_old"])

    if "qty_on_hand" in column_ids and inventory is not None:
        # For numeric column, just put the total
        new_values[column_ids["qty_on_hand"]] = str(int(inventory.total_on_hand))

    if "location" in column_ids and inventory is not None:
        # Format location details: qty in warehouse/bin (job if applicable)
        new_values[column_ids["location"]] = format_inventory(inventory)

    if "part_description" in column_ids and part_description:
        new_values[column_ids["part_description"]] = part_description

    # Compare with existing values - only include changed columns
    changed_values = {}
    for col_id, new_val in new_values.items():
        existing_val = existing_values.get(col_id, "")
        # Normalize for comparison (strip whitespace, handle None)
        new_normalized = (new_val or "").strip()
        existing_normalized = (existing_val or "").strip()

        if new_normalized != existing_normalized:
            changed_values[col_id] = new_val

    # Only update if there are actual changes
    if changed_values:
        print(f"[DEBUG] Updating item {item_id} - {len(changed_values)} columns changed", file=sys.stderr)
        monday.update_item(board_id, item_id, changed_values)
        return True

    print(f"[DEBUG] Skipping item {item_id} - no changes", file=sys.stderr)
    return False


def get_partnums_from_monday(monday: MondayClient, board_id: str) -> list[dict]:
    """
    Get all items from the Monday.com board and extract PartNum values.

    Returns list of dicts with 'item_id', 'item_name', and 'partnum'.
    """
    # First, get the board structure to understand columns
    board = monday.get_board(board_id)
    if not board:
        raise ValueError(f"Board {board_id} not found")

    print(f"Board: {board['name']}")
    print(f"Columns:")
    for col in board['columns']:
        print(f"  - {col['id']}: {col['title']} ({col['type']})")

    # Get all items from the board
    items = monday.get_items(board_id, limit=500)
    print(f"\nFound {len(items)} items")

    # Look for a column that might contain PartNum
    # Common patterns: column named "PartNum", "Part Number", "Part", or the item name itself
    partnum_column_id = None
    for col in board['columns']:
        title_lower = col['title'].lower()
        if 'partnum' in title_lower or 'part number' in title_lower or title_lower == 'part':
            partnum_column_id = col['id']
            print(f"\nUsing column '{col['title']}' (id: {col['id']}) for PartNum")
            break

    results = []
    for item in items:
        item_id = item['id']
        item_name = item['name']

        # Try to get PartNum from the identified column, or fall back to item name
        partnum = None
        if partnum_column_id:
            for cv in item['column_values']:
                if cv['id'] == partnum_column_id:
                    partnum = cv['text'] if cv['text'] else None
                    break

        # If no dedicated column or it's empty, use item name as PartNum
        if not partnum:
            partnum = item_name

        # Build a dict of existing column values (id -> text value)
        existing_values = {}
        for cv in item['column_values']:
            # Use 'text' for display value comparison
            existing_values[cv['id']] = cv['text'] or ""

        if partnum:
            results.append({
                'item_id': item_id,
                'item_name': item_name,
                'partnum': partnum.strip(),
                'existing_values': existing_values,
            })

    return results


def process_item(epicor: EpicorClient, monday: MondayClient, board_id: str,
                  column_ids: dict[str, str], item: dict) -> dict:
    """
    Process a single item: look up POs and inventory in Epicor, update Monday.com.

    Only updates Monday.com if data has changed.

    Returns a dict with processing results for aggregation.
    """
    partnum = item['partnum']
    item_id = item['item_id']
    item_name = item['item_name']
    existing_values = item.get('existing_values', {})

    result = {
        'partnum': partnum,
        'item_name': item_name,
        'item_id': item_id,
        'po_lines': [],
        'categorized': None,
        'inventory': None,
        'part_description': None,
        'error': None,
        'updated': False,  # Track if Monday.com was updated
    }

    try:
        # Query Epicor for part description
        try:
            result['part_description'] = epicor.get_part_description(partnum)
        except Exception:
            pass

        # Query Epicor for POs
        po_lines = epicor.get_po_lines_by_partnum(partnum)
        result['po_lines'] = po_lines

        # Categorize POs
        categorized = categorize_pos(po_lines)
        result['categorized'] = categorized

        # Query Epicor for inventory
        try:
            result['inventory'] = epicor.get_qty_on_hand(partnum)
        except Exception as inv_err:
            result['inventory_error'] = str(inv_err)

        # Update Monday.com (only if values changed)
        result['updated'] = update_monday_item(
            monday, board_id, item_id, column_ids, categorized,
            result['inventory'], result['part_description'], existing_values
        )

    except Exception as e:
        result['error'] = str(e)

    return result


def main(limit: Optional[int] = None, sequential: bool = False):
    print("=" * 60)
    print("Epicor -> Monday.com PO Sync")
    print("=" * 60)

    # Initialize clients
    print("\nConnecting to Monday.com...")
    monday = get_monday_client()

    print("Connecting to Epicor...")
    epicor = get_epicor_client()

    board_ids = getattr(config, "MONDAY_PARTS_BOARD_IDS", None) or [config.MONDAY_PARTS_BOARD_ID]

    all_results = {}

    for board_id in board_ids:
        board_id = str(board_id)

        print("\n" + "=" * 60)
        print(f"Processing board {board_id}")
        print("=" * 60)

        # Get column IDs for the board
        print(f"\nGetting column configuration for board {board_id}...")
        column_ids = get_column_ids(monday, board_id)
        print(f"  Found columns: {list(column_ids.keys())}")

        # Get PartNums from Monday.com
        print(f"\nFetching items from board {board_id}...")
        items = get_partnums_from_monday(monday, board_id)

        # Apply limit if specified
        if limit is not None and limit > 0:
            items = items[:limit]
            print(f"  (Limited to first {limit} items for testing)")

        # Look up POs for each item and update Monday.com
        print("\nLooking up POs in Epicor and updating Monday.com...")
        if sequential:
            print(f"Processing {len(items)} items sequentially...")
        else:
            print(f"Processing {len(items)} items with 3 parallel workers...")

        completed_count = 0
        error_count = 0
        updated_count = 0
        skipped_count = 0

        def handle_result(result, item):
            """Process a single result and update counters."""
            nonlocal completed_count, error_count, updated_count, skipped_count

            completed_count += 1
            partnum = result['partnum']
            all_results[partnum] = result['po_lines']

            # Track updates vs skips
            if result.get('updated'):
                updated_count += 1
            else:
                skipped_count += 1

            # Print progress
            status_parts = []
            if result['error']:
                error_count += 1
                status_parts.append(f"ERROR: {result['error']}")
            else:
                if result['categorized']:
                    cat = result['categorized']
                    status_parts.append(f"POs: {len(cat['open'])} open, {len(cat['closed_recent'])} recent, {len(cat['closed_old'])} old")
                if result['inventory']:
                    status_parts.append(f"Inv: {result['inventory'].total_on_hand}")
                elif result.get('inventory_error'):
                    status_parts.append("Inv: N/A")
                # Indicate if update was skipped
                if not result.get('updated'):
                    status_parts.append("(no change)")

            status = " | ".join(status_parts) if status_parts else "OK"
            print(f"[{completed_count}/{len(items)}] {result['item_name']}: {status}")

        if sequential:
            # Sequential processing - one item at a time
            for item in items:
                try:
                    result = process_item(epicor, monday, board_id, column_ids, item)
                    handle_result(result, item)
                except Exception as e:
                    completed_count += 1
                    error_count += 1
                    all_results[item['partnum']] = []
                    print(f"[{completed_count}/{len(items)}] {item['item_name']}: FAILED - {e}")
        else:
            # Parallel processing with thread pool
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {
                    executor.submit(process_item, epicor, monday, board_id, column_ids, item): item
                    for item in items
                }

                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        result = future.result()
                        handle_result(result, item)
                    except Exception as e:
                        completed_count += 1
                        error_count += 1
                        all_results[item['partnum']] = []
                        print(f"[{completed_count}/{len(items)}] {item['item_name']}: FAILED - {e}")

        # Board summary
        print(f"\nBoard {board_id} summary:")
        total_pos = sum(len(pos) for pos in all_results.values())
        parts_with_pos = sum(1 for pos in all_results.values() if pos)
        print(f"  Items processed: {len(items)}")
        print(f"  Parts with POs: {parts_with_pos}")
        print(f"  Total PO lines found: {total_pos}")
        print(f"  Monday.com updates: {updated_count} updated, {skipped_count} unchanged")
        if error_count > 0:
            print(f"  Errors: {error_count}")

    # Show Monday.com rate limit stats if any retries occurred
    rate_stats = monday.rate_limit_stats
    if rate_stats["rate_limit_hits"] > 0:
        print(f"\nMonday.com rate limits hit: {rate_stats['rate_limit_hits']} "
              f"(retries: {rate_stats['total_retries']})")

    return all_results


def show_api_usage():
    """Display Monday.com API usage statistics."""
    print("=" * 60)
    print("Monday.com API Usage")
    print("=" * 60)

    monday = get_monday_client()

    try:
        usage = monday.get_api_usage()

        print("\nUsage by Day:")
        print("-" * 40)
        for day_data in usage["by_day"]:
            print(f"  {day_data['day']}: {day_data['usage']:,} calls")

        if usage["by_app"]:
            print("\nUsage by App:")
            print("-" * 40)
            for app_data in usage["by_app"]:
                print(f"  {app_data['app_name']}: {app_data['usage']:,} calls")

        if usage["by_user"]:
            print("\nUsage by User:")
            print("-" * 40)
            for user_data in usage["by_user"]:
                print(f"  {user_data['user_name']}: {user_data['usage']:,} calls")

        if usage["last_updated"]:
            print(f"\nLast updated: {usage['last_updated']}")

        # Calculate today's total
        if usage["by_day"]:
            today_usage = usage["by_day"][0]["usage"] if usage["by_day"] else 0
            print(f"\nToday's usage: {today_usage:,} API calls")

    except Exception as e:
        print(f"Error fetching usage: {e}")


def estimate_sync_calls(limit: Optional[int] = None):
    """Estimate how many API calls a sync would use (dry run)."""
    print("=" * 60)
    print("Estimating API Calls (Dry Run)")
    print("=" * 60)

    monday = get_monday_client()

    # These calls are needed just to estimate
    print("\nCounting items on board...")
    startup_calls = 0

    try:
        # 1 call: get board columns
        startup_calls += 1
        column_ids = get_column_ids(monday, config.MONDAY_PARTS_BOARD_ID)

        # 1 call: get board info + 1 call: get items
        startup_calls += 2
        items = get_partnums_from_monday(monday, config.MONDAY_PARTS_BOARD_ID)

        total_items = len(items)
        if limit is not None and limit > 0:
            items = items[:limit]
            print(f"  Board has {total_items} items (limited to {limit} for this estimate)")
        else:
            print(f"  Board has {total_items} items")

        items_to_process = len(items)

        print(f"\nAPI Call Estimate:")
        print("-" * 40)
        print(f"  Startup calls (already used):     {startup_calls}")
        print(f"  Items to process:                 {items_to_process}")
        print(f"  Max update calls (if all change): {items_to_process}")
        print(f"  -" * 20)
        print(f"  WORST CASE TOTAL:                 {startup_calls + items_to_process}")
        print(f"  BEST CASE (no changes):           {startup_calls}")

        print(f"\nNote: Actual calls depend on how many items have changed data.")
        print(f"      The change detection will skip items with no updates.")

        # Show current usage for context
        print(f"\nChecking current API usage...")
        try:
            usage = monday.get_api_usage()
            if usage["by_day"]:
                today = usage["by_day"][0]
                print(f"  Today's usage so far: {today['usage']:,} calls (as of {today['day']})")
                remaining_estimate = 10000 - today['usage']  # Assume 10k limit
                print(f"  Estimated remaining (assuming 10k limit): ~{max(0, remaining_estimate):,} calls")
        except:
            pass

    except Exception as e:
        print(f"Error during estimation: {e}")
        print("\nNote: This error consumed some API calls. If you hit the daily limit,")
        print("      you'll need to wait until tomorrow to run the sync.")


if __name__ == "__main__":
    import sys

    # Show help
    if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
        print("""
Epicor -> Monday.com PO Sync

Usage:
    python main.py [options]

Options:
    --sequential     Run sequentially (no threading) to avoid rate limits
    --limit N        Process only the first N items
    --usage          Show Monday.com API usage statistics
    --dry-run        Estimate API calls without running sync
    --configure      Configure board columns only

Examples:
    python main.py --usage                    # Check API usage
    python main.py --dry-run                  # Estimate calls for full sync
    python main.py --dry-run --limit 50       # Estimate calls for 50 items
    python main.py --sequential --limit 50   # Run sync on 50 items
""")
        sys.exit(0)

    if len(sys.argv) > 1 and sys.argv[1] == "--configure":
        # Run board configuration only
        monday = get_monday_client()
        if len(sys.argv) > 2:
            # Explicit board ID on the command line
            configure_board(monday, sys.argv[2])
        else:
            board_ids = getattr(config, "MONDAY_PARTS_BOARD_IDS", None) or [config.MONDAY_PARTS_BOARD_ID]
            for board_id in board_ids:
                configure_board(monday, str(board_id))

    elif len(sys.argv) > 1 and sys.argv[1] == "--usage":
        # Show API usage
        show_api_usage()

    elif "--dry-run" in sys.argv:
        # Estimate API calls
        limit = None
        for i, arg in enumerate(sys.argv):
            if arg == "--limit" and i + 1 < len(sys.argv):
                try:
                    limit = int(sys.argv[i + 1])
                except ValueError:
                    pass
        estimate_sync_calls(limit=limit)

    else:
        # Parse command line options
        limit = None
        sequential = False

        i = 1
        while i < len(sys.argv):
            arg = sys.argv[i]
            if arg == "--limit" and i + 1 < len(sys.argv):
                try:
                    limit = int(sys.argv[i + 1])
                except ValueError:
                    pass
                i += 2
            elif arg == "--sequential":
                sequential = True
                i += 1
            else:
                i += 1

        main(limit=limit, sequential=sequential)
