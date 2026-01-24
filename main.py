"""
Main program: Sync PO data from Epicor to Monday.com

Reads PartNum values from a Monday.com board, looks up associated POs
in Epicor, and displays the results.
"""

from monday_client import MondayClient
from epicor_po_x2 import EpicorClient, POLineMatch, PartQtySummary
from datetime import datetime, timedelta
from typing import Optional
import config


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
                       part_description: Optional[str] = None) -> None:
    """Update a Monday.com item with PO and inventory data."""
    column_values = {}

    if "open_pos" in column_ids:
        column_values[column_ids["open_pos"]] = format_po_column(categorized_pos["open"])

    if "closed_pos_recent" in column_ids:
        column_values[column_ids["closed_pos_recent"]] = format_po_column(categorized_pos["closed_recent"])

    if "closed_pos_old" in column_ids:
        column_values[column_ids["closed_pos_old"]] = format_po_column(categorized_pos["closed_old"])

    if "qty_on_hand" in column_ids and inventory is not None:
        # For numeric column, just put the total
        column_values[column_ids["qty_on_hand"]] = str(int(inventory.total_on_hand))

    if "location" in column_ids and inventory is not None:
        # Format location details: qty in warehouse/bin (job if applicable)
        column_values[column_ids["location"]] = format_inventory(inventory)

    if "part_description" in column_ids and part_description:
        column_values[column_ids["part_description"]] = part_description

    if column_values:
        monday.update_item(board_id, item_id, column_values)


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

        if partnum:
            results.append({
                'item_id': item_id,
                'item_name': item_name,
                'partnum': partnum.strip(),
            })

    return results


def main(limit: Optional[int] = None):
    print("=" * 60)
    print("Epicor -> Monday.com PO Sync")
    print("=" * 60)

    # Initialize clients
    print("\nConnecting to Monday.com...")
    monday = get_monday_client()

    print("Connecting to Epicor...")
    epicor = get_epicor_client()

    # Get column IDs for the board
    print(f"\nGetting column configuration for board {config.MONDAY_PARTS_BOARD_ID}...")
    column_ids = get_column_ids(monday, config.MONDAY_PARTS_BOARD_ID)
    print(f"  Found columns: {list(column_ids.keys())}")

    # Get PartNums from Monday.com
    print(f"\nFetching items from board {config.MONDAY_PARTS_BOARD_ID}...")
    items = get_partnums_from_monday(monday, config.MONDAY_PARTS_BOARD_ID)

    # Apply limit if specified
    if limit is not None and limit > 0:
        items = items[:limit]
        print(f"  (Limited to first {limit} items for testing)")

    # Look up POs for each item and update Monday.com
    print("\n" + "=" * 60)
    print("Looking up POs in Epicor and updating Monday.com...")
    print("=" * 60)

    all_results = {}
    for item in items:
        partnum = item['partnum']
        item_id = item['item_id']
        item_name = item['item_name']

        print(f"\nProcessing: {item_name} (PartNum: {partnum})")

        try:
            # Query Epicor for part description
            part_description = None
            try:
                part_description = epicor.get_part_description(partnum)
                if part_description:
                    print(f"  Description: {part_description}")
            except Exception:
                pass

            # Query Epicor for POs
            po_lines = epicor.get_po_lines_by_partnum(partnum)
            all_results[partnum] = po_lines

            # Categorize POs
            categorized = categorize_pos(po_lines)

            print(f"  POs: {len(categorized['open'])} open, "
                  f"{len(categorized['closed_recent'])} closed (recent), "
                  f"{len(categorized['closed_old'])} closed (old)")

            # Query Epicor for inventory
            inventory = None
            try:
                inventory = epicor.get_qty_on_hand(partnum)
                print(f"  Inventory: {inventory.total_on_hand} on hand")
                for inv in inventory.by_location:
                    job_info = f" (Job {inv.job_num})" if inv.job_num else ""
                    print(f"    - {inv.on_hand_qty} in {inv.warehouse}/{inv.bin_num}{job_info}")
            except Exception as inv_err:
                print(f"  Inventory: Could not retrieve ({inv_err})")

            # Update Monday.com
            update_monday_item(monday, config.MONDAY_PARTS_BOARD_ID, item_id, column_ids, categorized, inventory, part_description)
            print(f"  Updated Monday.com item {item_id}")

        except Exception as e:
            print(f"  Error: {e}")
            all_results[partnum] = []

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    total_pos = sum(len(pos) for pos in all_results.values())
    parts_with_pos = sum(1 for pos in all_results.values() if pos)
    print(f"Items processed: {len(items)}")
    print(f"Parts with POs: {parts_with_pos}")
    print(f"Total PO lines found: {total_pos}")

    return all_results


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--configure":
        # Run board configuration only
        monday = get_monday_client()
        board_id = sys.argv[2] if len(sys.argv) > 2 else config.MONDAY_PARTS_BOARD_ID
        configure_board(monday, board_id)
    else:
        # Check for --limit N option
        limit = None
        for i, arg in enumerate(sys.argv[1:], 1):
            if arg == "--limit" and i + 1 < len(sys.argv):
                try:
                    limit = int(sys.argv[i + 1])
                except ValueError:
                    pass
        main(limit=limit)
