"""
BOM Lookup: Query Bill of Materials from Epicor

Usage:
    python bom_lookup.py <part_number> [--revision REV] [--recursive] [--json] [--csv FILE] [--pdf FILE] [--engine ENGINE] [--parts]

Arguments:
    part_number     The assembly part number to look up

Options:
    --revision REV  Specific revision to query (default: approved/latest)
    --recursive     Show nested BOMs for subassemblies
    --json          Output as JSON instead of formatted text
    --csv FILE      Output to CSV file (includes 'level' column for hierarchy)
    --pdf FILE      Output assembly tree diagram to PDF file
    --engine ENGINE PDF engine: 'd2' (default) or 'graphviz'
    --parts         Include piece parts listed below each assembly (PDF only)
"""

import sys
import json
import csv
import shutil
import subprocess
from typing import Optional, Set, List, Tuple
from epicor_po_x2 import EpicorClient, BillOfMaterials, BOMComponent, EpicorError
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
        if bom.approved is not None:
            print(f"Approved: {bom.approved}")
        if bom.group_id:
            print(f"ECO Group: {bom.group_id}")
        print("=" * 70)
        print()

    if not bom.components:
        print(f"{prefix}  (No components found)")
        return

    # Print header
    if indent == 0:
        print(f"{'Seq':<6} {'Part Number':<25} {'Rev':<6} {'Qty':<10} {'UOM':<8} {'Supplier':<25} {'Description'}")
        print("-" * 120)

    for comp in bom.components:
        qty_str = f"{comp.qty_per:g}"
        uom_str = comp.uom or ""
        rev_str = (comp.revision or "")[:5]
        supplier_str = (comp.vendor_name or "")[:23]
        desc_str = (comp.description or "")[:30]

        # Mark subassemblies
        asm_marker = " [ASM]" if comp.pull_as_asm or comp.view_as_asm else ""

        print(f"{prefix}{comp.mtl_seq:<6} {comp.part_num:<25} {rev_str:<6} {qty_str:<10} {uom_str:<8} {supplier_str:<25} {desc_str}{asm_marker}")


def print_bom_recursive(epicor: EpicorClient, partnum: str, revision: Optional[str] = None,
                        indent: int = 0, ancestors: Optional[Set[str]] = None,
                        ancestor_list: Optional[List[str]] = None,
                        processed: Optional[Set[str]] = None) -> None:
    """Print a BOM with recursive expansion of subassemblies."""
    if ancestors is None:
        ancestors = set()
    if ancestor_list is None:
        ancestor_list = []
    if processed is None:
        processed = set()

    key = f"{partnum}:{revision or ''}"

    # Check for TRUE circular reference (part is an ancestor of itself)
    if key in ancestors:
        print()
        print("!" * 70, file=sys.stderr)
        print("! CIRCULAR REFERENCE DETECTED", file=sys.stderr)
        print(f"! Part {partnum} (rev {revision or 'none'}) is an ancestor of itself", file=sys.stderr)
        print("!", file=sys.stderr)

        print("! Ancestor chain with single-level BOMs:", file=sys.stderr)
        for i, p in enumerate(ancestor_list):
            pnum, prev = p.split(":", 1) if ":" in p else (p, None)
            print(f"!   {i+1}. {p}", file=sys.stderr)
            # Fetch single-level BOM for this part
            try:
                chain_bom = epicor.get_bom(pnum, prev if prev else None)
                if chain_bom.components:
                    for comp in chain_bom.components:
                        asm_marker = " [ASM]" if comp.pull_as_asm or comp.view_as_asm else ""
                        # Flag if this component is the circular part
                        if comp.part_num == partnum:
                            print(f"!      -> {comp.part_num} (qty: {comp.qty_per:g}){asm_marker} *** CAUSES LOOP ***", file=sys.stderr)
                        else:
                            print(f"!      -> {comp.part_num} (qty: {comp.qty_per:g}){asm_marker}", file=sys.stderr)
                else:
                    print(f"!      (no components)", file=sys.stderr)
            except Exception as e:
                print(f"!      (error fetching BOM: {e})", file=sys.stderr)
        print(f"!   {len(ancestor_list)+1}. {partnum}:{revision or ''} <- CIRCULAR", file=sys.stderr)
        print("!" * 70, file=sys.stderr)
        print()
        print("  " * indent + f"  (circular reference to {partnum})")
        return

    # Check if already processed (duplicate on same or different branch, not circular)
    if key in processed:
        print("  " * indent + f"  (duplicate - already expanded: {partnum})")
        return

    # Add to ancestors for this recursion path, and mark as processed
    ancestors.add(key)
    ancestor_list.append(key)
    processed.add(key)

    bom = epicor.get_bom(partnum, revision)
    print_bom(bom, indent)

    # Recursively print subassemblies
    for comp in bom.components:
        if comp.pull_as_asm or comp.view_as_asm:
            print()
            print("  " * (indent + 1) + f"--- Subassembly: {comp.part_num} ---")
            print_bom_recursive(epicor, comp.part_num, indent=indent + 1,
                                ancestors=ancestors, ancestor_list=ancestor_list, processed=processed)

    # Remove from ancestors when leaving this branch (but keep in processed)
    ancestors.remove(key)
    ancestor_list.pop()


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
                        ancestors: Optional[Set[str]] = None,
                        ancestor_list: Optional[List[str]] = None,
                        processed: Optional[Set[str]] = None) -> List[dict]:
    """
    Flatten BOM into a list of rows suitable for CSV export.

    Each row includes a 'level' field indicating hierarchy depth (0 = top level).
    """
    if ancestors is None:
        ancestors = set()
    if ancestor_list is None:
        ancestor_list = []
    if processed is None:
        processed = set()

    rows = []
    key = f"{partnum}:{revision or ''}"

    # Check for TRUE circular reference (part is an ancestor of itself)
    if key in ancestors:
        print()
        print("!" * 70, file=sys.stderr)
        print("! CIRCULAR REFERENCE DETECTED", file=sys.stderr)
        print(f"! Part {partnum} (rev {revision or 'none'}) is an ancestor of itself", file=sys.stderr)
        print("!", file=sys.stderr)

        print("! Ancestor chain with single-level BOMs:", file=sys.stderr)
        for p in ancestor_list:
            pnum, prev = p.split(":", 1) if ":" in p else (p, None)
            print(f"!   {p}", file=sys.stderr)
            # Fetch single-level BOM for this part
            try:
                chain_bom = epicor.get_bom(pnum, prev if prev else None)
                if chain_bom.components:
                    for comp in chain_bom.components:
                        asm_marker = " [ASM]" if comp.pull_as_asm or comp.view_as_asm else ""
                        # Flag if this component is the circular part
                        if comp.part_num == partnum:
                            print(f"!      -> {comp.part_num} (qty: {comp.qty_per:g}){asm_marker} *** CAUSES LOOP ***", file=sys.stderr)
                        else:
                            print(f"!      -> {comp.part_num} (qty: {comp.qty_per:g}){asm_marker}", file=sys.stderr)
                else:
                    print(f"!      (no components)", file=sys.stderr)
            except Exception as e:
                print(f"!      (error fetching BOM: {e})", file=sys.stderr)
        print(f"! Circular part: {partnum}:{revision or ''}", file=sys.stderr)
        print("!" * 70, file=sys.stderr)
        print()
        rows.append({
            "level": level,
            "parent_part": partnum,
            "parent_description": "(circular reference)",
            "parent_revision": revision or "",
            "approved": "",
            "group_id": "",
            "seq": "",
            "part_num": "",
            "revision": "",
            "part_class": "",
            "part_class_desc": "",
            "description": "",
            "qty_per": "",
            "uom": "",
            "is_subassembly": "",
            "supplier": "",
            "ref_category": "",
        })
        return rows

    # Check if already processed (duplicate, not circular)
    if key in processed:
        # Duplicate - already expanded elsewhere, just skip without adding rows
        return rows

    # Add to ancestors for this recursion path, and mark as processed
    ancestors.add(key)
    ancestor_list.append(key)
    processed.add(key)

    bom = epicor.get_bom(partnum, revision)

    if not bom.components:
        # No components - add a row indicating empty BOM
        rows.append({
            "level": level,
            "parent_part": bom.part_num,
            "parent_description": bom.description or "",
            "parent_revision": bom.revision or "",
            "approved": "Y" if bom.approved else "N" if bom.approved is False else "",
            "group_id": bom.group_id or "",
            "seq": "",
            "part_num": "(no components)",
            "revision": "",
            "part_class": "",
            "part_class_desc": "",
            "description": "",
            "qty_per": "",
            "uom": "",
            "is_subassembly": "",
            "supplier": "",
            "ref_category": "",
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
            "approved": "Y" if bom.approved else "N" if bom.approved is False else "",
            "group_id": bom.group_id or "",
            "seq": comp.mtl_seq,
            "part_num": comp.part_num,
            "revision": comp.revision or "",
            "part_class": part_class or "",
            "part_class_desc": part_class_desc or "",
            "description": comp.description or "",
            "qty_per": comp.qty_per,
            "uom": comp.uom or "",
            "is_subassembly": "Y" if is_subasm else "N",
            "supplier": comp.vendor_name or "",
            "ref_category": comp.ref_category or "",
        })

        # Recursively add subassembly components
        if recursive and is_subasm:
            sub_rows = flatten_bom_for_csv(
                epicor, comp.part_num, revision=None,
                recursive=True, level=level + 1,
                ancestors=ancestors, ancestor_list=ancestor_list, processed=processed
            )
            rows.extend(sub_rows)

    # Remove from ancestors when leaving this branch (but keep in processed)
    ancestors.remove(key)
    ancestor_list.pop()

    return rows


def write_csv(rows: List[dict], filename: str) -> None:
    """Write BOM rows to a CSV file."""
    if not rows:
        print(f"No data to write to {filename}", file=sys.stderr)
        return

    fieldnames = [
        "level", "parent_part", "parent_description", "parent_revision", "approved", "group_id",
        "seq", "part_num", "revision", "part_class", "part_class_desc", "description", "qty_per", "uom", "is_subassembly",
        "supplier", "ref_category"
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {filename}", file=sys.stderr)


def build_assembly_tree(epicor: EpicorClient, partnum: str, revision: Optional[str] = None,
                        ancestors: Optional[Set[str]] = None,
                        processed: Optional[Set[str]] = None,
                        parts: Optional[dict] = None) -> List[Tuple[str, str, str, str]]:
    """
    Build a list of edges for the assembly tree.

    Returns a list of tuples: (parent_id, parent_label, child_id, child_label)
    where id is a unique identifier and label is the display text.

    If parts dict is provided, it will be populated with piece parts per node:
    {node_id: [(part_num, qty_per, uom, description), ...]}
    """
    if ancestors is None:
        ancestors = set()
    if processed is None:
        processed = set()

    edges = []
    key = f"{partnum}:{revision or ''}"

    # Check for circular reference
    if key in ancestors:
        return edges

    # Check if already processed
    if key in processed:
        return edges

    ancestors.add(key)
    processed.add(key)

    try:
        bom = epicor.get_bom(partnum, revision)
        if not bom.components and revision:
            # BOM may not exist on this specific revision; try approved/latest
            bom = epicor.get_bom(partnum)
    except EpicorError:
        ancestors.remove(key)
        return edges

    parent_id = f"{partnum}_{revision or 'latest'}"
    parent_label = f"{partnum}\\n{bom.description or ''}"[:50] if bom.description else partnum

    # Find child assemblies and collect piece parts
    for comp in bom.components:
        if comp.pull_as_asm or comp.view_as_asm:
            child_id = f"{comp.part_num}_{comp.revision or 'latest'}"
            child_desc = (comp.description or "")[:30]
            child_label = f"{comp.part_num}\\n{child_desc}" if child_desc else comp.part_num

            edges.append((parent_id, parent_label, child_id, child_label))

            # Recurse into subassembly
            child_edges = build_assembly_tree(
                epicor, comp.part_num, comp.revision,
                ancestors=ancestors, processed=processed,
                parts=parts
            )
            edges.extend(child_edges)
        else:
            # Piece part - collect if parts dict is provided
            if parts is not None:
                parts.setdefault(parent_id, []).append(
                    (comp.part_num, comp.qty_per, comp.uom or "",
                     (comp.description or "")[:30])
                )

    ancestors.remove(key)
    return edges


def generate_d2_source(edges: List[Tuple[str, str, str, str]],
                       root_id: str, root_label: str,
                       parts: Optional[dict] = None) -> str:
    """Generate D2 source text from assembly tree edges."""
    lines = ["direction: down", ""]

    # Collect all unique nodes with their labels
    node_labels = {root_id: root_label}
    for parent_id, parent_label, child_id, child_label in edges:
        node_labels.setdefault(parent_id, parent_label)
        node_labels.setdefault(child_id, child_label)

    # Quote a D2 identifier (part numbers often contain hyphens)
    def q(node_id):
        return f'"{node_id}"' if not node_id.isidentifier() else node_id

    # Declare nodes
    for node_id, label in node_labels.items():
        display_label = label.replace("\\n", "\n")
        fill = "#90EE90" if node_id == root_id else "#ADD8E6"
        node_parts = parts.get(node_id, []) if parts else []

        lines.append(f"{q(node_id)}: {{")

        if node_parts:
            # Markdown label: assembly header + piece parts list
            label_lines = display_label.split("\n")
            lines.append("  label: |md")
            lines.append(f"    **{label_lines[0]}**\\")
            if len(label_lines) > 1:
                lines.append(f"    {label_lines[1]}")
            lines.append("")
            for pnum, qty, uom, desc in node_parts:
                qty_str = f"{qty:g}" if isinstance(qty, (int, float)) else str(qty)
                lines.append(f"    <sub>{qty_str} {uom} **{pnum}** {desc}</sub>\\")
            lines.append("  |")
        elif "\n" in display_label:
            # D2 block string syntax for multiline labels
            lines.append("  label: |")
            for lbl_line in display_label.split("\n"):
                lines.append(f"    {lbl_line}")
            lines.append("  |")
        else:
            lines.append(f'  label: "{display_label}"')

        lines.append(f'  style.fill: "{fill}"')
        lines.append("  style.border-radius: 8")
        lines.append("}")
        lines.append("")

    # Declare edges
    for parent_id, _, child_id, _ in edges:
        lines.append(f"{q(parent_id)} -> {q(child_id)}")

    lines.append("")
    return "\n".join(lines)


def generate_assembly_tree_pdf_d2(epicor: EpicorClient, partnum: str, revision: Optional[str],
                                   filename: str, show_parts: bool = False) -> None:
    """Generate a PDF showing the assembly tree hierarchy using D2 + ELK layout."""
    if not shutil.which("d2"):
        print("ERROR: d2 is not installed or not on PATH.", file=sys.stderr)
        print("Install it with: choco install d2  (Windows)", file=sys.stderr)
        print("Or see: https://d2lang.com/tour/install", file=sys.stderr)
        sys.exit(1)

    print(f"Building assembly tree for {partnum}...", file=sys.stderr)

    # Get the root BOM info
    try:
        root_bom = epicor.get_bom(partnum, revision)
    except EpicorError as e:
        print(f"ERROR: Failed to fetch BOM for {partnum}: {e}", file=sys.stderr)
        sys.exit(1)

    # Build the tree (optionally collecting piece parts)
    parts = {} if show_parts else None
    edges = build_assembly_tree(epicor, partnum, revision, parts=parts)

    root_id = f"{partnum}_{revision or 'latest'}"
    root_label = f"{partnum}\\n{root_bom.description or ''}"[:50] if root_bom.description else partnum

    # Generate D2 source
    d2_source = generate_d2_source(edges, root_id, root_label, parts=parts)

    # Remove .pdf extension if present (we'll add it ourselves)
    if filename.lower().endswith('.pdf'):
        filename = filename[:-4]

    # Write .d2 source file (kept for debugging, like .gv)
    d2_file = f"{filename}.d2"
    with open(d2_file, "w", encoding="utf-8") as f:
        f.write(d2_source)

    # Render to PDF using d2 with ELK layout
    pdf_file = f"{filename}.pdf"
    try:
        result = subprocess.run(
            ["d2", "--layout", "elk", d2_file, pdf_file],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"ERROR: d2 rendering failed:", file=sys.stderr)
            print(result.stderr, file=sys.stderr)
            print(f"D2 source saved to {d2_file}", file=sys.stderr)
            sys.exit(1)
        print(f"Assembly tree saved to {pdf_file}", file=sys.stderr)
    except Exception as e:
        print(f"ERROR: Failed to run d2: {e}", file=sys.stderr)
        print(f"D2 source saved to {d2_file}", file=sys.stderr)
        sys.exit(1)


def generate_assembly_tree_pdf(epicor: EpicorClient, partnum: str, revision: Optional[str],
                                filename: str, engine: str = "d2",
                                show_parts: bool = False) -> None:
    """Generate a PDF showing the assembly tree hierarchy."""
    if engine == "d2":
        generate_assembly_tree_pdf_d2(epicor, partnum, revision, filename, show_parts=show_parts)
        return

    try:
        from graphviz import Digraph
    except ImportError:
        print("ERROR: graphviz library not installed.", file=sys.stderr)
        print("Install it with: pip install graphviz", file=sys.stderr)
        print("You also need Graphviz installed on your system:", file=sys.stderr)
        print("  Windows: choco install graphviz  OR  download from https://graphviz.org/download/", file=sys.stderr)
        sys.exit(1)

    print(f"Building assembly tree for {partnum}...", file=sys.stderr)

    # Get the root BOM info
    try:
        root_bom = epicor.get_bom(partnum, revision)
    except EpicorError as e:
        print(f"ERROR: Failed to fetch BOM for {partnum}: {e}", file=sys.stderr)
        sys.exit(1)

    # Build the tree (optionally collecting piece parts)
    parts = {} if show_parts else None
    edges = build_assembly_tree(epicor, partnum, revision, parts=parts)

    # Create the graph
    dot = Digraph(comment=f'Assembly Tree: {partnum}')
    dot.attr(rankdir='TB')  # Top to bottom
    dot.attr(splines='ortho')  # Orthogonal routing with 90-degree turns
    dot.attr(nodesep='0.5')  # Horizontal spacing between nodes
    dot.attr(ranksep='0.4')  # Vertical spacing between ranks
    dot.attr(overlap='false')
    dot.attr(ordering='out')  # Preserve child order left-to-right
    dot.attr('node', shape='box', style='rounded,filled', fillcolor='lightblue', fontname='Arial')
    dot.attr('edge', color='#333333')

    # Collect node labels and parent→children mapping from edges
    root_id = f"{partnum}_{revision or 'latest'}"
    root_label = f"{partnum}\\n{root_bom.description or ''}"[:50] if root_bom.description else partnum

    node_labels = {root_id: root_label}
    children_map = {}  # parent_id → [child_id, ...]

    for parent_id, parent_label, child_id, child_label in edges:
        node_labels.setdefault(parent_id, parent_label)
        node_labels.setdefault(child_id, child_label)
        children_map.setdefault(parent_id, []).append(child_id)

    # Assign vertical alignment groups: each node's primary (middle) child
    # inherits the parent's group, keeping them vertically aligned. This
    # helps the layout engine position subtrees cleanly.
    groups = {}

    def assign_groups(node_id, group_name):
        groups[node_id] = group_name
        if node_id in children_map:
            child_ids = children_map[node_id]
            # Middle child inherits parent's group (centered vertical alignment)
            mid = len(child_ids) // 2
            for i, cid in enumerate(child_ids):
                if i == mid:
                    assign_groups(cid, group_name)
                else:
                    assign_groups(cid, cid)

    assign_groups(root_id, root_id)

    def html_escape(text):
        return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')

    def make_html_label(node_id, label):
        """Build a Graphviz HTML label with assembly header and optional parts list."""
        parts_list = parts.get(node_id, []) if parts else []
        label_lines = label.replace('\\n', '\n').split('\n')
        name = html_escape(label_lines[0])
        desc = html_escape(label_lines[1]) if len(label_lines) > 1 else ""

        rows = [f'<TR><TD ALIGN="LEFT"><B><FONT POINT-SIZE="11">{name}</FONT></B></TD></TR>']
        if desc:
            rows.append(f'<TR><TD ALIGN="LEFT"><FONT POINT-SIZE="9">{desc}</FONT></TD></TR>')

        if parts_list:
            rows.append('<TR><TD><BR/></TD></TR>')
            for pnum, qty, uom, pdesc in parts_list:
                qty_str = f"{qty:g}" if isinstance(qty, (int, float)) else str(qty)
                line = f"{qty_str} {html_escape(uom)} <B>{html_escape(pnum)}</B> {html_escape(pdesc)}"
                rows.append(f'<TR><TD ALIGN="LEFT"><FONT POINT-SIZE="7">{line}</FONT></TD></TR>')

        return '<' + '<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0">' + ''.join(rows) + '</TABLE>' + '>'

    # Add all visible nodes with group attributes
    for node_id, label in node_labels.items():
        grp = groups.get(node_id, node_id)
        fill = 'lightgreen' if node_id == root_id else 'lightblue'
        if show_parts:
            dot.node(node_id, make_html_label(node_id, label),
                     fillcolor=fill, group=grp, shape='box', style='rounded,filled')
        else:
            dot.node(node_id, label, fillcolor=fill, group=grp)

    # Add edges using invisible junction points for clean orthogonal fan-out.
    # Without junctions, N edges all leave a parent's south port and the ortho
    # router creates overlapping messy routes. With a junction point, only one
    # edge leaves the parent, and the small point fans out cleanly.
    for parent_id, child_ids in children_map.items():
        if len(child_ids) == 1:
            dot.edge(parent_id, child_ids[0], tailport='s', headport='n')
        else:
            mid = len(child_ids) // 2
            mid_grp = groups.get(child_ids[mid], child_ids[mid])
            jct_id = f"jct_{parent_id}"
            dot.node(jct_id, '', shape='none', width='0', height='0',
                     fixedsize='true', group=mid_grp)
            dot.edge(parent_id, jct_id, arrowhead='none', tailport='s')
            for child_id in child_ids:
                dot.edge(jct_id, child_id, headport='n')

    # Remove .pdf extension if present (graphviz adds it)
    if filename.lower().endswith('.pdf'):
        filename = filename[:-4]

    # Render to PDF
    try:
        dot.render(filename, format='pdf', cleanup=False)  # Keep .gv file for debugging
        print(f"Assembly tree saved to {filename}.pdf", file=sys.stderr)
    except Exception as e:
        print(f"ERROR: Failed to render PDF: {e}", file=sys.stderr)
        print("Make sure Graphviz is installed on your system.", file=sys.stderr)
        # Save the DOT source anyway for debugging
        dot.save(f"{filename}.gv")
        print(f"DOT source saved to {filename}.gv", file=sys.stderr)
        sys.exit(1)


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
    pdf_file = None
    engine = "d2"
    show_parts = False

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
        elif arg == "--pdf" and i + 1 < len(sys.argv):
            pdf_file = sys.argv[i + 1]
            i += 2
        elif arg == "--engine" and i + 1 < len(sys.argv):
            engine = sys.argv[i + 1].lower()
            if engine not in ("d2", "graphviz"):
                print(f"Unknown engine: {engine}. Choose 'd2' or 'graphviz'.", file=sys.stderr)
                sys.exit(1)
            i += 2
        elif arg == "--parts":
            show_parts = True
            i += 1
        else:
            print(f"Unknown argument: {arg}")
            sys.exit(1)

    # --parts uses graphviz by default (vector PDF with HTML labels)
    # unless --engine was explicitly set
    if show_parts and engine == "d2" and "--engine" not in sys.argv:
        engine = "graphviz"

    # Connect to Epicor
    print(f"Connecting to Epicor...", file=sys.stderr)
    epicor = get_epicor_client()

    try:
        # PDF output (assembly tree diagram)
        if pdf_file:
            generate_assembly_tree_pdf(epicor, partnum, revision, pdf_file, engine=engine, show_parts=show_parts)
            return

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

    except EpicorError as e:
        print(f"\nERROR: Failed to fetch BOM from Epicor", file=sys.stderr)
        print(f"  {e}", file=sys.stderr)
        if "401" in str(e):
            print(f"\n  This may be caused by:", file=sys.stderr)
            print(f"    - Invalid credentials", file=sys.stderr)
            print(f"    - Too many concurrent users/sessions", file=sys.stderr)
            print(f"    - API license limit reached", file=sys.stderr)
            print(f"    - Session timeout", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
