"""
Job Lookup: Query job POs and BOM (materials) from Epicor

Usage:
    python job_lookup.py <job_number> [--pos] [--bom]

    If neither --pos nor --bom is given, both are shown.

Examples:
    python job_lookup.py 000672
    python job_lookup.py 000672 --pos
    python job_lookup.py 000672 --bom
"""

import sys
from datetime import datetime
from typing import List, Optional

import config
from epicor_po_x2 import (
    EpicorClient, EpicorError,
    JobHeader, JobMaterial, POLineMatch,
)


def get_epicor_client() -> EpicorClient:
    return EpicorClient(
        base_url  = config.EPICOR_BASE_URL,
        company   = config.EPICOR_COMPANY,
        plant     = config.EPICOR_PLANT,
        api_key   = config.EPICOR_API_KEY,
        username  = config.EPICOR_USERNAME,
        password  = config.EPICOR_PASSWORD,
        learn_missing_getrows_params=True,
    )


def fmt_date(s: Optional[str]) -> str:
    if not s:
        return ""
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%m/%d/%Y")
    except Exception:
        return s[:10] if len(s) >= 10 else s


def fmt_qty(v) -> str:
    if v is None:
        return "?"
    f = float(v)
    return f"{int(f)}" if f == int(f) else f"{f:g}"


# ---------------------------------------------------------------------------
# Print job header
# ---------------------------------------------------------------------------

def print_header(header: Optional[JobHeader], job_num: str) -> None:
    print("=" * 70)
    print(f"Job: {job_num}")
    if header:
        if header.part_num:
            print(f"Part:        {header.part_num}")
        if header.description:
            print(f"Description: {header.description}")
        if header.prod_qty is not None:
            print(f"Qty:         {fmt_qty(header.prod_qty)} {header.uom or ''}")
        if header.start_date:
            print(f"Start:       {fmt_date(header.start_date)}")
        if header.due_date:
            print(f"Due:         {fmt_date(header.due_date)}")
        flags = []
        if header.released: flags.append("Released")
        if header.complete: flags.append("Complete")
        if header.closed:   flags.append("Closed")
        if flags:
            print(f"Status:      {', '.join(flags)}")
    print("=" * 70)


# ---------------------------------------------------------------------------
# Print POs
# ---------------------------------------------------------------------------

def print_pos(po_lines: List[POLineMatch]) -> None:
    if not po_lines:
        print("  (no POs found for this job)")
        return

    # Group by status
    open_lines   = [p for p in po_lines if p.status == "open"]
    closed_lines = [p for p in po_lines if p.status == "closed"]
    void_lines   = [p for p in po_lines if p.status == "void"]
    other_lines  = [p for p in po_lines if p.status not in ("open", "closed", "void")]

    def print_group(label: str, lines: List[POLineMatch]) -> None:
        if not lines:
            return
        print(f"\n  {label} ({len(lines)}):")
        print(f"  {'PO #':<8} {'Line':<5} {'Part':<20} {'Qty':>7} {'Rcvd':>7} {'Due':<12} Vendor")
        print("  " + "-" * 80)
        for p in lines:
            due    = fmt_date(p.due_date)
            qty    = fmt_qty(p.order_qty)
            rcvd   = fmt_qty(p.received_qty) if p.received_qty is not None else ""
            vendor = (p.vendor_name or "")[:28]
            part   = (p.part_num or "")[:18]
            print(f"  {p.po_num:<8} {p.po_line:<5} {part:<20} {qty:>7} {rcvd:>7} {due:<12} {vendor}")

    print_group("OPEN",   open_lines)
    print_group("CLOSED", closed_lines)
    print_group("VOID",   void_lines)
    print_group("OTHER",  other_lines)

    total_open = sum(p.order_qty or 0 for p in open_lines)
    print(f"\n  Total: {len(po_lines)} PO line(s)  |  {len(open_lines)} open"
          + (f" (qty {fmt_qty(total_open)})" if open_lines else ""))


# ---------------------------------------------------------------------------
# Print job BOM (materials)
# ---------------------------------------------------------------------------

def print_bom(materials: List[JobMaterial]) -> None:
    if not materials:
        print("  (no materials found for this job)")
        return

    # Group by assembly_seq
    by_asm: dict = {}
    for m in materials:
        by_asm.setdefault(m.assembly_seq, []).append(m)

    total_buy   = sum(1 for m in materials if m.buy_it)
    total_make  = sum(1 for m in materials if not m.buy_it)

    print(f"\n  {'Seq':<6} {'Mtl':<5} {'Part':<22} {'Req Qty':>9} {'Issued':>9} {'UOM':<5} {'B':<2} Description")
    print("  " + "-" * 100)

    for asm_seq in sorted(by_asm):
        mtls = by_asm[asm_seq]
        if len(by_asm) > 1:
            print(f"\n  [Assembly {asm_seq}]")
        for m in mtls:
            req    = fmt_qty(m.required_qty)
            issued = fmt_qty(m.issued_qty)
            buy    = "B" if m.buy_it else " "
            desc   = (m.description or "")[:38]
            pn     = m.part_num[:20]
            print(f"  {m.assembly_seq:<6} {m.mtl_seq:<5} {pn:<22} {req:>9} {issued:>9} {m.uom or '':<5} {buy:<2} {desc}")

    print(f"\n  Total: {len(materials)} material(s)  |  {total_buy} buy  |  {total_make} make")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = sys.argv[1:]

    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    job_num  = args[0].strip()
    show_pos = "--pos" in args or "--bom" not in args
    show_bom = "--bom" in args or "--pos" not in args

    print(f"Connecting to Epicor...", file=sys.stderr)
    client = get_epicor_client()

    try:
        if show_bom or True:   # always fetch header (it's free, comes with BOM)
            print(f"Fetching job {job_num}...", file=sys.stderr)
            header, materials = client.get_job(job_num)
            print_header(header, job_num)

        if show_bom:
            print(f"\nBill of Materials / Job Materials")
            print("-" * 70)
            print_bom(materials)

        if show_pos:
            print(f"\nPurchase Orders")
            print("-" * 70)
            print(f"Fetching POs for job {job_num}...", file=sys.stderr)
            po_lines = client.get_job_pos(job_num)
            print_pos(po_lines)

    except EpicorError as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        if "401" in str(e):
            print("  Check credentials / concurrent session limit.", file=sys.stderr)
        sys.exit(1)

    print()


if __name__ == "__main__":
    main()
