"""
BOM Lookup: Query Bill of Materials from Epicor

Usage:
    python bom_lookup.py <part_number> [--revision REV] [--recursive] [--json] [--csv FILE]

Arguments:
    part_number     The assembly part number to look up

Options:
    --revision REV  Specific revision to query (default: approved/latest)
    --recursive     Show nested BOMs for subassemblies
    --json          Output as JSON instead of formatted text
    --csv FILE      Output to CSV file (includes 'level' column for hierarchy)
"""

import sys
import json
import csv
from typing import Optional, Set, List
from epicor_po_x2 import EpicorClient, BillOfMaterials, BOMComponent
import config


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


def print_bom(bom: BillOfMaterials, indent: int = 0) -> None:
    """Print a formatted BOM."""
    prefix = "  " * indent

    if indent == 0:
        print("=" * 70)
        print(f"Bill of Materials: {bom.part_num}")
        if bom.revision:
            print(f"Revision: {bom.revision}")
        if bom.description:
            print(f"Description: {bom.description}")
        print("=" * 70)
        print()

    if not bom.components:
        print(f"{prefix}  (No components found)")
        return

    # Print header
    if indent == 0:
        print(f"{'Seq':<6} {'Part Number':<25} {'Qty':<10} {'UOM':<8} {'Description'}")
        print("-" * 70)

    for comp in bom.components:
        qty_str = f"{comp.qty_per:g}"
        uom_str = comp.uom or ""
        desc_str = (comp.description or "")[:30]

        # Mark subassemblies
        asm_marker = " [ASM]" if comp.pull_as_asm or comp.view_as_asm else ""

        print(f"{prefix}{comp.mtl_seq:<6} {comp.part_num:<25} {qty_str:<10} {uom_str:<8} {desc_str}{asm_marker}")


def print_bom_recursive(epicor: EpicorClient, partnum: str, revision: Optional[str] = None,
                        indent: int = 0, visited: Optional[Set[str]] = None) -> None:
    """Print a BOM with recursive expansion of subassemblies."""
    if visited is None:
        visited = set()

    # Prevent infinite recursion for circular BOMs
    key = f"{partnum}:{revision or ''}"
    if key in visited:
        print("  " * indent + f"  (circular reference to {partnum})")
        return
    visited.add(key)

    bom = epicor.get_bom(partnum, revision)
    print_bom(bom, indent)

    # Recursively print subassemblies
    for comp in bom.components:
        if comp.pull_as_asm or comp.view_as_asm:
            print()
            print("  " * (indent + 1) + f"--- Subassembly: {comp.part_num} ---")
            print_bom_recursive(epicor, comp.part_num, indent=indent + 1, visited=visited)


def collect_bom_recursive(epicor: EpicorClient, partnum: str, revision: Optional[str] = None,
                          visited: Optional[Set[str]] = None) -> dict:
    """Collect BOM data recursively as a nested dict structure."""
    if visited is None:
        visited = set()

    key = f"{partnum}:{revision or ''}"
    if key in visited:
        return {"part_num": partnum, "circular_reference": True}
    visited.add(key)

    bom = epicor.get_bom(partnum, revision)
    result = bom.to_dict()

    # Add nested BOMs for subassemblies
    for i, comp in enumerate(result["components"]):
        if bom.components[i].pull_as_asm or bom.components[i].view_as_asm:
            comp["subassembly_bom"] = collect_bom_recursive(
                epicor, comp["part_num"], visited=visited
            )

    return result


def flatten_bom_for_csv(epicor: EpicorClient, partnum: str, revision: Optional[str] = None,
                        recursive: bool = False, level: int = 0,
                        visited: Optional[Set[str]] = None) -> List[dict]:
    """
    Flatten BOM into a list of rows suitable for CSV export.

    Each row includes a 'level' field indicating hierarchy depth (0 = top level).
    """
    if visited is None:
        visited = set()

    rows = []

    # Prevent infinite recursion for circular BOMs
    key = f"{partnum}:{revision or ''}"
    if key in visited:
        rows.append({
            "level": level,
            "parent_part": partnum,
            "parent_description": "(circular reference)",
            "parent_revision": revision or "",
            "seq": "",
            "part_num": "",
            "part_class": "",
            "part_class_desc": "",
            "description": "",
            "qty_per": "",
            "uom": "",
            "is_subassembly": "",
        })
        return rows
    visited.add(key)

    bom = epicor.get_bom(partnum, revision)

    if not bom.components:
        # No components - add a row indicating empty BOM
        rows.append({
            "level": level,
            "parent_part": bom.part_num,
            "parent_description": bom.description or "",
            "parent_revision": bom.revision or "",
            "seq": "",
            "part_num": "(no components)",
            "part_class": "",
            "part_class_desc": "",
            "description": "",
            "qty_per": "",
            "uom": "",
            "is_subassembly": "",
        })
        return rows

    for comp in bom.components:
        is_subasm = comp.pull_as_asm or comp.view_as_asm
        part_class, part_class_desc = epicor.get_part_class(comp.part_num)

        rows.append({
            "level": level,
            "parent_part": bom.part_num,
            "parent_description": bom.description or "",
            "parent_revision": bom.revision or "",
            "seq": comp.mtl_seq,
            "part_num": comp.part_num,
            "part_class": part_class or "",
            "part_class_desc": part_class_desc or "",
            "description": comp.description or "",
            "qty_per": comp.qty_per,
            "uom": comp.uom or "",
            "is_subassembly": "Y" if is_subasm else "N",
        })

        # Recursively add subassembly components
        if recursive and is_subasm:
            sub_rows = flatten_bom_for_csv(
                epicor, comp.part_num, revision=None,
                recursive=True, level=level + 1, visited=visited
            )
            rows.extend(sub_rows)

    return rows


def write_csv(rows: List[dict], filename: str) -> None:
    """Write BOM rows to a CSV file."""
    if not rows:
        print(f"No data to write to {filename}", file=sys.stderr)
        return

    fieldnames = [
        "level", "parent_part", "parent_description", "parent_revision",
        "seq", "part_num", "part_class", "part_class_desc", "description", "qty_per", "uom", "is_subassembly"
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {filename}", file=sys.stderr)


def main():
    # Parse arguments
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0 if sys.argv[1:] == ["--help"] else 1)

    partnum = sys.argv[1]
    revision = None
    recursive = False
    as_json = False
    csv_file = None

    i = 2
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == "--revision" and i + 1 < len(sys.argv):
            revision = sys.argv[i + 1]
            i += 2
        elif arg == "--recursive":
            recursive = True
            i += 1
        elif arg == "--json":
            as_json = True
            i += 1
        elif arg == "--csv" and i + 1 < len(sys.argv):
            csv_file = sys.argv[i + 1]
            i += 2
        else:
            print(f"Unknown argument: {arg}")
            sys.exit(1)

    # Connect to Epicor
    print(f"Connecting to Epicor...", file=sys.stderr)
    epicor = get_epicor_client()

    # CSV output
    if csv_file:
        rows = flatten_bom_for_csv(epicor, partnum, revision, recursive=recursive)
        write_csv(rows, csv_file)
        return

    # JSON output
    if as_json:
        if recursive:
            data = collect_bom_recursive(epicor, partnum, revision)
        else:
            bom = epicor.get_bom(partnum, revision)
            data = bom.to_dict()
        print(json.dumps(data, indent=2))
        return

    # Text output
    if recursive:
        print_bom_recursive(epicor, partnum, revision)
    else:
        bom = epicor.get_bom(partnum, revision)
        print_bom(bom)

        # Summary
        print()
        print(f"Total components: {len(bom.components)}")
        subasm_count = sum(1 for c in bom.components if c.pull_as_asm or c.view_as_asm)
        if subasm_count > 0:
            print(f"Subassemblies: {subasm_count} (use --recursive to expand)")


if __name__ == "__main__":
    main()
