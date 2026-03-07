from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
import json
import re
import requests


# ---------------------------
# Hardcode your known required GetRows params here.
# If Epicor later demands more, we can "learn" them at runtime and optionally
# print/log the updated dict so you can paste them back here.
# ---------------------------
DEFAULT_GETROWS_EXTRAS: Dict[str, Any] = {
    "whereClausePOHeaderAttch": "",
    "whereClausePODetailAttch": "",
    "whereClausePORelAttch": "",
    "whereClausePORelTax": "",
    "whereClausePORelTGLC": "",
    "whereClausePODetailInsp": "",
    "whereClausePODetailTax": "",
    "whereClausePOMisc": "",
    "whereClausePODetailMiscTax": "",
    "whereClausePOHeadMisc": "",
    "whereClausePOHeaderMiscTax": "",
    "whereClausePOHeaderTax": "",
}

_MISSING_PARAM_RE = re.compile(r"Parameter\s+([A-Za-z0-9_]+)\s+is not found in the input object")


@dataclass(frozen=True)
class BOMComponent:
    """A single component in a Bill of Materials."""
    company: str
    parent_part: str            # Assembly/parent part number
    mtl_seq: int                # Material sequence number
    part_num: str               # Component part number
    description: Optional[str]  # Part description
    qty_per: float              # Quantity per parent assembly
    uom: Optional[str]          # Unit of measure
    revision: Optional[str]     # Revision number
    operation_seq: Optional[int]  # Related operation sequence
    pull_as_asm: bool           # Pull as assembly (subassembly)
    view_as_asm: bool           # View as assembly in BOM viewer
    fixed_qty: bool             # Fixed quantity vs per-unit
    vendor_num: Optional[int]   # Supplier/vendor number
    vendor_name: Optional[str]  # Supplier/vendor name
    ref_category: Optional[str] # Reference Category

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BillOfMaterials:
    """Bill of Materials for an assembly."""
    company: str
    part_num: str               # Assembly part number
    revision: Optional[str]     # Revision number
    description: Optional[str]  # Assembly description
    approved: Optional[bool]    # Whether the revision is approved
    group_id: Optional[str]     # ECO Group ID
    components: List[BOMComponent]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "company": self.company,
            "part_num": self.part_num,
            "revision": self.revision,
            "description": self.description,
            "approved": self.approved,
            "group_id": self.group_id,
            "components": [c.to_dict() for c in self.components],
        }


@dataclass(frozen=True)
class WhereUsedEntry:
    """An assembly that uses the specified component part."""
    company: str
    part_num: str               # The component being looked up
    assembly_part: str          # The parent assembly that uses this part
    assembly_description: Optional[str]
    revision: Optional[str]     # Parent assembly's revision in the BOM
    qty_per: float              # Qty of this component per assembly
    uom: Optional[str]          # Unit of measure
    pull_as_asm: bool           # Whether the component is pulled as a subassembly
    group_id: Optional[str]     # ECO Group ID

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PartInventory:
    """Inventory information for a part number."""
    company: str
    part_num: str
    warehouse: str
    bin_num: str
    lot_num: Optional[str]
    on_hand_qty: float
    job_num: Optional[str]          # Job number if allocated to a job
    dim_code: Optional[str]         # Dimension code if applicable

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PartQtySummary:
    """Summary of quantity on hand for a part number."""
    company: str
    part_num: str
    total_on_hand: float
    by_location: List[PartInventory]    # Breakdown by warehouse/bin/job

    def to_dict(self) -> Dict[str, Any]:
        return {
            "company": self.company,
            "part_num": self.part_num,
            "total_on_hand": self.total_on_hand,
            "by_location": [inv.to_dict() for inv in self.by_location],
        }


@dataclass(frozen=True)
class JobHeader:
    """Summary information about an Epicor job."""
    company: str
    job_num: str
    part_num: Optional[str]
    description: Optional[str]
    prod_qty: Optional[float]
    uom: Optional[str]
    start_date: Optional[str]
    due_date: Optional[str]
    released: Optional[bool]
    complete: Optional[bool]
    closed: Optional[bool]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class JobMaterial:
    """A material requirement line on a job (job BOM)."""
    company: str
    job_num: str
    assembly_seq: int
    mtl_seq: int
    part_num: str
    description: Optional[str]
    required_qty: float
    issued_qty: float
    uom: Optional[str]
    buy_it: bool            # True = purchased part, False = make/sub-contracted

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class POLineMatch:
    """Normalized result for downstream code (JSON-friendly via to_dict())."""
    company: str
    po_num: int
    po_line: int
    order_date: Optional[str]
    due_date: Optional[str]             # earliest DueDate from PORel records
    vendor_name: Optional[str]          # from POHeader vendor info

    part_num: str
    line_desc: Optional[str]
    order_qty: Optional[float]
    unit_cost: Optional[float]

    # Status / receipt-related info
    po_open: Optional[bool]             # from POHead.OpenOrder (if present)
    line_open: Optional[bool]           # from PODetail.OpenLine (if present)
    void_line: Optional[bool]           # from PODetail.VoidLine (if present)
    any_open_release: Optional[bool]    # from PORel.OpenRelease/OpenRel (if present)
    received_qty: Optional[float]       # sum of PORel.ReceivedQty (if present)

    status: str                         # "open" | "closed" | "void" | "unknown"
    received_complete: Optional[bool]   # True/False if we can infer; else None

    job_num: Optional[str] = None       # job number(s) from PORel.JobNum (if allocated)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class EpicorError(RuntimeError):
    pass


class EpicorClient:
    """
    Minimal Epicor REST client for POSvc.GetRows queries.

    - Uses hardcoded DEFAULT_GETROWS_EXTRAS by default.
    - If Epicor returns "Parameter X is not found...", it can auto-learn X and retry.
    """

    def __init__(
        self,
        base_url: str,
        company: str,
        api_key: str,
        username: str,
        password: str,
        plant: Optional[str] = None,
        timeout_s: int = 60,
        getrows_extras: Optional[Dict[str, Any]] = None,
        learn_missing_getrows_params: bool = True,
        max_learn_retries: int = 30,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.company = company
        self.api_key = api_key
        self.timeout_s = timeout_s

        self.learn_missing_getrows_params = learn_missing_getrows_params
        self.max_learn_retries = max_learn_retries

        # Start with hardcoded extras unless caller provides their own.
        self._getrows_extras: Dict[str, Any] = dict(getrows_extras) if getrows_extras is not None else dict(DEFAULT_GETROWS_EXTRAS)

        # Memoization caches for expensive API calls
        self._po_lines_cache: Dict[str, List[POLineMatch]] = {}
        self._qty_on_hand_cache: Dict[str, PartQtySummary] = {}
        self._part_description_cache: Dict[str, Optional[str]] = {}
        self._part_class_cache: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        self._vendor_name_cache: Dict[int, Optional[str]] = {}
        self._where_used_cache: Dict[str, List[WhereUsedEntry]] = {}

        self.session = requests.Session()
        self.session.auth = (username, password)

        call_settings: Dict[str, Any] = {"Company": company}
        if plant:
            call_settings["Plant"] = plant

        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "CallSettings": json.dumps(call_settings),
        }

    @property
    def getrows_extras(self) -> Dict[str, Any]:
        """Current GetRows extras (including any newly learned params)."""
        return dict(self._getrows_extras)

    @property
    def cache_stats(self) -> Dict[str, int]:
        """Return the number of cached entries for each cache type."""
        return {
            "po_lines": len(self._po_lines_cache),
            "qty_on_hand": len(self._qty_on_hand_cache),
            "part_description": len(self._part_description_cache),
            "part_class": len(self._part_class_cache),
            "vendor_name": len(self._vendor_name_cache),
            "where_used": len(self._where_used_cache),
        }

    def clear_cache(self) -> None:
        """Clear all memoization caches."""
        self._po_lines_cache.clear()
        self._qty_on_hand_cache.clear()
        self._part_description_cache.clear()
        self._part_class_cache.clear()
        self._vendor_name_cache.clear()
        self._where_used_cache.clear()

    def _post_json_raw(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = self.session.post(
            url,
            params={"api-key": self.api_key},
            headers=self.headers,
            json=payload,
            timeout=self.timeout_s,
        )
        if r.ok:
            return r.json()

        # Try to surface Epicor's structured error
        try:
            err = r.json()
            msg = err.get("ErrorMessage") or err.get("Message") or r.text
        except Exception:
            msg = r.text

        raise EpicorError(f"HTTP {r.status_code} calling {r.request.url}: {msg}")

    def _post_getrows_with_optional_learning(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST GetRows. If Epicor complains about missing input params, optionally add them and retry.
        """
        last_err: Optional[Exception] = None

        for _ in range(max(1, self.max_learn_retries + 1)):
            try:
                return self._post_json_raw(url, payload)
            except EpicorError as e:
                last_err = e
                if not self.learn_missing_getrows_params:
                    raise

                m = _MISSING_PARAM_RE.search(str(e))
                if not m:
                    raise

                missing = m.group(1)
                # Default safe value is empty string (these are typically whereClause* strings).
                if missing not in payload:
                    payload[missing] = ""
                    self._getrows_extras[missing] = ""
                    continue

                # If it's somehow present and still failing, stop.
                raise

        raise EpicorError(f"Exceeded max learn retries while calling GetRows. Last error: {last_err}")

    @staticmethod
    def _extract_tableset(resp_json: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        # Epicor often returns {"returnObj": {...}, "parameters": {...}}
        if "returnObj" in resp_json and isinstance(resp_json["returnObj"], dict):
            return resp_json["returnObj"], resp_json.get("parameters", {}) or {}
        return resp_json, resp_json.get("parameters", {}) or {}

    @staticmethod
    def _more_pages(parameters: Dict[str, Any]) -> bool:
        for k in ("morePages", "MorePages"):
            if k in parameters:
                return bool(parameters[k])
        return False

    @staticmethod
    def _get_bool(d: Dict[str, Any], key: str) -> Optional[bool]:
        v = d.get(key)
        return bool(v) if isinstance(v, bool) else None

    @staticmethod
    def _get_float(d: Dict[str, Any], key: str) -> Optional[float]:
        v = d.get(key)
        return float(v) if isinstance(v, (int, float)) else None

    def get_po_lines_by_partnum(
        self,
        partnum: str,
        page_size: int = 200,
        max_pages: int = 300,
    ) -> List[POLineMatch]:
        """
        Returns all PO detail lines whose PartNum exactly matches `partnum`,
        plus open/closed/received information where available.

        Status logic (best-effort):
          - void if VoidLine == True
          - open if POHead.OpenOrder OR PODetail.OpenLine OR any PORel open flag is True
          - else closed
        Received complete (best-effort):
          - if received_qty and order_qty are present: received_qty >= order_qty AND status != open
          - else None

        Results are memoized per partnum for the lifetime of this client instance.
        """
        # Check cache first
        if partnum in self._po_lines_cache:
            return self._po_lines_cache[partnum]

        url = f"{self.base_url}/api/v2/odata/{self.company}/Erp.BO.POSvc/GetRows"

        all_poheader: List[Dict[str, Any]] = []
        all_podetail: List[Dict[str, Any]] = []
        all_porel: List[Dict[str, Any]] = []

        for page in range(max_pages):
            payload: Dict[str, Any] = {
                "whereClausePOHeader": "",
                "whereClausePODetail": f"PartNum = '{partnum}'",
                "whereClausePORel": "",              # keep releases so we can compute received/open status
                "pageSize": int(page_size),
                "absolutePage": int(page),
                **self._getrows_extras,
            }

            resp = self._post_getrows_with_optional_learning(url, payload)
            tableset, out_params = self._extract_tableset(resp)

            pohead = tableset.get("POHeader") or tableset.get("POHeaders") or []
            podet = tableset.get("PODetail") or tableset.get("PODetails") or []
            porel = tableset.get("PORel") or tableset.get("PORels") or []

            if isinstance(pohead, list):
                all_poheader.extend([h for h in pohead if isinstance(h, dict)])
            if isinstance(podet, list):
                all_podetail.extend([d for d in podet if isinstance(d, dict) and d.get("PartNum") == partnum])
            if isinstance(porel, list):
                all_porel.extend([r for r in porel if isinstance(r, dict)])

            if not self._more_pages(out_params):
                break

        # Header map: PO num -> header
        header_by_po: Dict[int, Dict[str, Any]] = {}
        for h in all_poheader:
            pon = h.get("PONum") or h.get("PONUM")
            if isinstance(pon, int) and pon not in header_by_po:
                header_by_po[pon] = h

        # Releases grouped by (PONum, POLine)
        rels_by_line: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
        for r in all_porel:
            pon = r.get("PONum") or r.get("PONUM")
            pol = r.get("POLine")
            if isinstance(pon, int) and isinstance(pol, int):
                rels_by_line.setdefault((pon, pol), []).append(r)

        # Normalize + dedupe
        seen: set[Tuple[int, int]] = set()
        all_podetail.sort(key=lambda d: (d.get("PONUM", 0) or 0, d.get("POLine", 0) or 0))

        results: List[POLineMatch] = []

        for d in all_podetail:
            po_num = d.get("PONUM")
            po_line = d.get("POLine")
            if not isinstance(po_num, int) or not isinstance(po_line, int):
                continue

            key = (po_num, po_line)
            if key in seen:
                continue
            seen.add(key)

            h = header_by_po.get(po_num, {})
            po_open = self._get_bool(h, "OpenOrder")

            line_open = self._get_bool(d, "OpenLine")
            void_line = self._get_bool(d, "VoidLine")

            # Try to compute open release, received qty, and due date from releases (if fields exist)
            rels = rels_by_line.get(key, [])
            any_open_rel: Optional[bool] = None
            received_qty: Optional[float] = None
            due_date: Optional[str] = None

            job_nums: List[str] = []
            if rels:
                open_flags: List[bool] = []
                recv_sum = 0.0
                recv_any = False
                due_dates: List[str] = []

                for r in rels:
                    # Epicor environments differ; check both common names.
                    for open_key in ("OpenRelease", "OpenRel"):
                        v = r.get(open_key)
                        if isinstance(v, bool):
                            open_flags.append(v)
                            break

                    rq = r.get("ReceivedQty")
                    if isinstance(rq, (int, float)):
                        recv_sum += float(rq)
                        recv_any = True

                    # Capture due date
                    dd = r.get("DueDate")
                    if isinstance(dd, str) and dd:
                        due_dates.append(dd)

                    # Capture job number allocation
                    jn = r.get("JobNum")
                    if isinstance(jn, str) and jn.strip() and jn.strip() not in job_nums:
                        job_nums.append(jn.strip())

                if open_flags:
                    any_open_rel = any(open_flags)
                if recv_any:
                    received_qty = recv_sum
                if due_dates:
                    # Use earliest due date
                    due_dates.sort()
                    due_date = due_dates[0]

            # Normalize fields
            order_date = None
            od = h.get("OrderDate")
            if isinstance(od, str):
                order_date = od

            # Get vendor name from header (try multiple field names)
            vendor_name = None
            for vn_key in ("VendorNumName", "VendorName", "VendorID"):
                vn = h.get(vn_key)
                if isinstance(vn, str) and vn:
                    vendor_name = vn
                    break

            order_qty = self._get_float(d, "OrderQty")
            unit_cost = self._get_float(d, "UnitCost")

            line_desc = d.get("LineDesc") if isinstance(d.get("LineDesc"), str) else None

            # Status
            status = "unknown"
            if void_line is True:
                status = "void"
            else:
                open_indicators = [x for x in (po_open, line_open, any_open_rel) if x is not None]
                if any(open_indicators):
                    status = "open"
                elif open_indicators:
                    # all known indicators are False
                    status = "closed"

            # Received complete (best-effort)
            received_complete: Optional[bool] = None
            if received_qty is not None and order_qty is not None:
                # if status is open, it isn't "complete" even if quantities match (could be timing)
                received_complete = (received_qty >= (order_qty - 1e-9)) and (status != "open")

            results.append(
                POLineMatch(
                    company=self.company,
                    po_num=po_num,
                    po_line=po_line,
                    order_date=order_date,
                    due_date=due_date,
                    vendor_name=vendor_name,
                    part_num=str(d.get("PartNum") or ""),
                    line_desc=line_desc,
                    order_qty=order_qty,
                    unit_cost=unit_cost,
                    po_open=po_open,
                    line_open=line_open,
                    void_line=void_line,
                    any_open_release=any_open_rel,
                    received_qty=received_qty,
                    status=status,
                    received_complete=received_complete,
                    job_num=", ".join(job_nums) if job_nums else None,
                )
            )

        # Cache and return results
        self._po_lines_cache[partnum] = results
        return results

    def _get_odata(self, endpoint: str, filter_clause: str, select_fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Generic OData GET request with filtering.
        Returns list of records.
        """
        url = f"{self.base_url}/api/v2/odata/{self.company}/{endpoint}"

        params: Dict[str, Any] = {
            "api-key": self.api_key,
            "$filter": filter_clause,
        }
        if select_fields:
            params["$select"] = ",".join(select_fields)

        r = self.session.get(url, params=params, headers=self.headers, timeout=self.timeout_s)

        if r.ok:
            data = r.json()
            return data.get("value", [])

        try:
            err = r.json()
            msg = err.get("ErrorMessage") or err.get("Message") or r.text
        except Exception:
            msg = r.text

        raise EpicorError(f"HTTP {r.status_code} calling {r.request.url}: {msg}")

    def get_qty_on_hand(self, partnum: str) -> PartQtySummary:
        """
        Get quantity on hand for a part number.

        Tries multiple Epicor endpoints to find inventory data:
        1. PartWhse table (warehouse-level inventory)
        2. PartBin table (bin-level inventory)
        3. RcvDtl (Receipt Detail) for received but not yet put-away inventory
        4. Part service for summary quantities

        Returns a PartQtySummary with total on-hand qty and breakdown by location/job.

        Results are memoized per partnum for the lifetime of this client instance.
        """
        # Check cache first
        if partnum in self._qty_on_hand_cache:
            return self._qty_on_hand_cache[partnum]

        inventory_list: List[PartInventory] = []
        total_qty = 0.0

        # Query for job allocations (to associate inventory with jobs)
        job_allocations: Dict[str, str] = {}  # warehouse -> job_num
        try:
            alloc_records = self._get_odata(
                "Erp.PartAlloc",
                f"PartNum eq '{partnum}'",
                ["WarehouseCode", "BinNum", "JobNum", "AllocQty"]
            )
            for alloc in alloc_records:
                wh = alloc.get("WarehouseCode", "")
                job = alloc.get("JobNum", "")
                if job:
                    job_allocations[wh] = job
        except EpicorError:
            pass

        # 1. Try PartWhse (warehouse-level inventory)
        whse_endpoints = [
            "Erp.PartWhse",
            "Erp.BO.PartSvc/PartWhses",
        ]
        for endpoint in whse_endpoints:
            try:
                records = self._get_odata(
                    endpoint,
                    f"PartNum eq '{partnum}'",
                    ["Company", "PartNum", "WarehouseCode", "OnHandQty"]
                )
                for rec in records:
                    qty = rec.get("OnHandQty") or 0
                    if not isinstance(qty, (int, float)) or qty == 0:
                        continue
                    wh = str(rec.get("WarehouseCode", "") or "")
                    inventory_list.append(PartInventory(
                        company=self.company,
                        part_num=partnum,
                        warehouse=wh,
                        bin_num="",
                        lot_num=None,
                        on_hand_qty=float(qty),
                        job_num=job_allocations.get(wh),
                        dim_code=None,
                    ))
                    total_qty += float(qty)
                if records:
                    break
            except EpicorError:
                continue

        # 2. Check RcvDtl for received but not yet put-away inventory
        try:
            rcv_qty = self._get_received_qty(partnum)
            for wh, bin_num, qty in rcv_qty:
                if qty > 0:
                    inventory_list.append(PartInventory(
                        company=self.company,
                        part_num=partnum,
                        warehouse=wh,
                        bin_num=bin_num,
                        lot_num=None,
                        on_hand_qty=qty,
                        job_num=job_allocations.get(wh),
                        dim_code=None,
                    ))
                    total_qty += qty
        except EpicorError:
            pass

        result = PartQtySummary(
            company=self.company,
            part_num=partnum,
            total_on_hand=total_qty,
            by_location=inventory_list,
        )

        # Cache and return result
        self._qty_on_hand_cache[partnum] = result
        return result

    def _get_received_qty(self, partnum: str) -> List[Tuple[str, str, float]]:
        """
        Get quantities from Receipt Detail (RcvDtl) that are received but not yet in PartWhse.

        Returns list of (warehouse, bin, qty) tuples.
        """
        from datetime import datetime, timedelta

        url = f"{self.base_url}/api/v2/odata/{self.company}/Erp.BO.ReceiptSvc/GetRows"

        # Query recent receipts (last 90 days) - Epicor GetRows filters on parent table (RcvHead)
        # not child table (RcvDtl), so we filter by date and then filter PartNum in Python
        cutoff_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

        payload: Dict[str, Any] = {
            "whereClauseRcvHead": f"EntryDate >= '{cutoff_date}'",
            "whereClauseRcvDtl": "",
            "pageSize": 1000,
            "absolutePage": 0,
        }

        resp = self._post_getrows_with_optional_learning(url, payload)
        tableset, _ = self._extract_tableset(resp)

        rcv_dtl = tableset.get("RcvDtl") or []
        results: List[Tuple[str, str, float]] = []

        for r in rcv_dtl:
            if not isinstance(r, dict):
                continue
            if r.get("PartNum") != partnum:
                continue
            if not r.get("Received"):
                continue

            qty = r.get("OurQty") or r.get("ReceivedQty") or 0
            if not isinstance(qty, (int, float)) or qty <= 0:
                continue

            wh = str(r.get("WareHouseCode", "") or "")
            bn = str(r.get("BinNum", "") or "")
            results.append((wh, bn, float(qty)))

        return results

    def get_part_description(self, partnum: str) -> Optional[str]:
        """
        Get the description for a part number.

        Returns the PartDescription from the Part table, or None if not found.

        Results are memoized per partnum for the lifetime of this client instance.
        """
        # Check cache first (use 'in' to distinguish cached None from not-cached)
        if partnum in self._part_description_cache:
            return self._part_description_cache[partnum]

        result: Optional[str] = None
        try:
            records = self._get_odata(
                "Erp.BO.PartSvc/Parts",
                f"PartNum eq '{partnum}'",
                ["PartNum", "PartDescription"]
            )
            if records:
                result = records[0].get("PartDescription")
        except EpicorError:
            pass

        # Cache and return result
        self._part_description_cache[partnum] = result
        return result

    def get_part_class(self, partnum: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Get the part class (ClassID) and its description for a part number.

        Returns a tuple of (ClassID, ClassDescription) from the Part table,
        or (None, None) if not found.

        Results are memoized per partnum for the lifetime of this client instance.
        """
        # Check cache first
        if partnum in self._part_class_cache:
            return self._part_class_cache[partnum]

        result: Tuple[Optional[str], Optional[str]] = (None, None)
        try:
            records = self._get_odata(
                "Erp.BO.PartSvc/Parts",
                f"PartNum eq '{partnum}'",
                ["PartNum", "ClassID", "ClassDescription"]
            )
            if records:
                result = (
                    records[0].get("ClassID"),
                    records[0].get("ClassDescription")
                )
        except EpicorError:
            pass

        # Cache and return result
        self._part_class_cache[partnum] = result
        return result

    def get_vendor_name(self, vendor_num: int) -> Optional[str]:
        """
        Get the vendor name for a vendor number.

        Results are memoized per vendor_num for the lifetime of this client instance.
        """
        if vendor_num in self._vendor_name_cache:
            return self._vendor_name_cache[vendor_num]

        result: Optional[str] = None
        try:
            records = self._get_odata(
                "Erp.BO.VendorSvc/Vendors",
                f"VendorNum eq {vendor_num}",
                ["VendorNum", "Name", "VendorID"]
            )
            if records:
                result = records[0].get("Name") or records[0].get("VendorID")
        except Exception:
            # Don't let vendor lookup failures break the BOM fetch
            pass

        self._vendor_name_cache[vendor_num] = result
        return result

    def get_part_default_vendor(self, partnum: str) -> Tuple[Optional[int], Optional[str]]:
        """
        Get the default vendor for a part from PartPlant.

        Returns a tuple of (VendorNum, VendorName) or (None, None) if not found.
        """
        try:
            url = f"{self.base_url}/api/v2/odata/{self.company}/Erp.BO.PartSvc/GetByID"
            payload = {"partNum": partnum}
            resp = self._post_json_raw(url, payload)

            if "returnObj" in resp:
                plants = resp["returnObj"].get("PartPlant", [])
                if plants:
                    plant = plants[0]
                    vendor_num = plant.get("VendorNum")
                    vendor_name = plant.get("VendorNumName")
                    if vendor_num and vendor_num != 0:
                        return (vendor_num, vendor_name)
        except Exception:
            pass

        return (None, None)

    def get_bom(self, partnum: str, revision: Optional[str] = None) -> BillOfMaterials:
        """
        Get the Bill of Materials for an assembly part number.

        Queries Epicor's ECOMtl table via OData for BOM components.

        Args:
            partnum: The assembly part number
            revision: Optional revision number. If not provided, uses the approved
                     revision or the first available revision.

        Returns:
            BillOfMaterials containing all components.
        """
        # Get part description
        description = self.get_part_description(partnum)

        # If no revision specified, try to find the approved or latest revision
        if revision is None:
            revision = self._get_part_revision(partnum)

        components: List[BOMComponent] = []
        group_id: Optional[str] = None
        approved: Optional[bool] = None

        # Get revision approval status
        try:
            url = f"{self.base_url}/api/v2/odata/{self.company}/Erp.BO.PartRevSearchSvc/GetRows"
            where_clause = f"PartNum = '{partnum}'"
            if revision:
                where_clause += f" AND RevisionNum = '{revision}'"
            payload = {
                "whereClausePartRev": where_clause,
                "pageSize": 1,
                "absolutePage": 0,
            }
            resp = self._post_json_raw(url, payload)
            revs = resp.get("returnObj", {}).get("PartRev", [])
            if revs:
                approved = revs[0].get("Approved")
        except EpicorError:
            pass

        # Build OData filter clause
        filter_clause = f"PartNum eq '{partnum}'"
        if revision:
            filter_clause += f" and RevisionNum eq '{revision}'"

        # Query for BOM components from Engineering Workbench
        try:
            records = self._get_odata(
                "Erp.BO.EngWorkBenchSvc/ECOMtls",
                filter_clause,
                None  # Get all fields
            )

            # Parts may exist in multiple ECO groups - filter to most recent group
            if records:
                # Find all unique GroupIDs and pick the most recent one
                # GroupIDs are often date-based (e.g., "AS-2025-12-20" or "04142022")
                group_ids = sorted(set(r.get("GroupID", "") for r in records), reverse=True)
                group_id = group_ids[0] if group_ids else None

                # Filter records to only include the selected group
                if group_id:
                    records = [r for r in records if r.get("GroupID") == group_id]

            for rec in records:
                mtl_part = rec.get("MtlPartNum", "")
                if not mtl_part:
                    continue

                # Get description - try multiple field names
                comp_desc = (
                    rec.get("MtlPartNumPartDescription") or
                    rec.get("MtlPartDescription") or
                    rec.get("PartDescription")
                )
                if not comp_desc:
                    comp_desc = self.get_part_description(mtl_part)

                # Get UOM - try multiple field names
                uom = rec.get("UOMCode") or rec.get("IUM") or rec.get("MtlPartNumIUM")

                # Get vendor info - first try ECOMtl, then fall back to PartPlant
                vendor_num = rec.get("VendorNum")
                vendor_name = None
                try:
                    if vendor_num and vendor_num != 0:
                        vendor_num = int(vendor_num)
                        vendor_name = self.get_vendor_name(vendor_num)
                    else:
                        # No vendor in ECOMtl, try to get default vendor from PartPlant
                        vendor_num, vendor_name = self.get_part_default_vendor(mtl_part)
                except (ValueError, TypeError, EpicorError):
                    vendor_num = None
                    vendor_name = None

                # Get Reference Category
                ref_category = rec.get("RefCategory") or rec.get("ReferenceCategory")

                components.append(BOMComponent(
                    company=self.company,
                    parent_part=partnum,
                    mtl_seq=int(rec.get("MtlSeq", 0) or 0),
                    part_num=str(mtl_part),
                    description=comp_desc,
                    qty_per=float(rec.get("QtyPer", 0) or 0),
                    uom=uom,
                    revision=rec.get("RevisionNum"),
                    operation_seq=rec.get("RelatedOperation"),
                    pull_as_asm=bool(rec.get("PullAsAsm", False)),
                    view_as_asm=bool(rec.get("ViewAsAsm", False)),
                    fixed_qty=bool(rec.get("FixedQty", False)),
                    vendor_num=vendor_num,
                    vendor_name=vendor_name,
                    ref_category=ref_category,
                ))

        except EpicorError:
            # If ECOMtls fails, return empty BOM (data may not exist)
            pass

        # Sort components by material sequence
        components.sort(key=lambda c: c.mtl_seq)

        return BillOfMaterials(
            company=self.company,
            part_num=partnum,
            revision=revision,
            description=description,
            approved=approved,
            group_id=group_id,
            components=components,
        )

    def get_where_used(self, partnum: str) -> List[WhereUsedEntry]:
        """
        Find all assemblies that use the specified part number.

        Queries ECOMtls (engineering BOM) to find parent assemblies.
        One entry is returned per parent assembly (most-recent ECO group wins).
        Results are memoized per partnum for the lifetime of this client instance.
        """
        if partnum in self._where_used_cache:
            return self._where_used_cache[partnum]

        entries: List[WhereUsedEntry] = []

        try:
            records = self._get_odata(
                "Erp.BO.EngWorkBenchSvc/ECOMtls",
                f"MtlPartNum eq '{partnum}'",
                None,  # get all fields
            )

            # Group by parent assembly; pick the most-recent ECO group per assembly
            by_assembly: Dict[str, List[Dict[str, Any]]] = {}
            for rec in records:
                parent = rec.get("PartNum", "")
                if parent and parent != partnum:
                    by_assembly.setdefault(parent, []).append(rec)

            for parent_part, recs in by_assembly.items():
                recs.sort(key=lambda r: r.get("GroupID", "") or "", reverse=True)
                rec = recs[0]
                desc = self.get_part_description(parent_part)
                entries.append(WhereUsedEntry(
                    company=self.company,
                    part_num=partnum,
                    assembly_part=parent_part,
                    assembly_description=desc,
                    revision=rec.get("RevisionNum"),
                    qty_per=float(rec.get("QtyPer", 0) or 0),
                    uom=rec.get("UOMCode") or rec.get("IUM"),
                    pull_as_asm=bool(rec.get("PullAsAsm", False)),
                    group_id=rec.get("GroupID"),
                ))
        except EpicorError:
            pass

        self._where_used_cache[partnum] = entries
        return entries

    def get_job(self, job_num: str) -> Tuple[Optional[JobHeader], List[JobMaterial]]:
        """
        Get the header and material list (BOM) for a job.

        Uses JobEntrySvc/GetByID which returns the full job dataset including
        JobHead and JobMtl tables.

        Returns (JobHeader | None, list[JobMaterial]).
        """
        url = f"{self.base_url}/api/v2/odata/{self.company}/Erp.BO.JobEntrySvc/GetByID"
        resp = self._post_json_raw(url, {"jobNum": job_num})
        obj  = resp.get("returnObj", {})

        # ── Job header ──────────────────────────────────────────────────
        header: Optional[JobHeader] = None
        heads = obj.get("JobHead") or []
        if heads:
            h = heads[0]
            header = JobHeader(
                company     = self.company,
                job_num     = str(h.get("JobNum", job_num)),
                part_num    = h.get("PartNum") or None,
                description = h.get("PartDescription") or h.get("JobDescription") or None,
                prod_qty    = self._get_float(h, "ProdQty"),
                uom         = h.get("IUM") or h.get("UOMCode") or None,
                start_date  = h.get("StartDate") or h.get("ReqDueDate") or None,
                due_date    = h.get("DueDate") or h.get("ReqDueDate") or None,
                released    = self._get_bool(h, "JobReleased"),
                complete    = self._get_bool(h, "JobComplete"),
                closed      = self._get_bool(h, "JobClosed"),
            )

        # ── Job materials ────────────────────────────────────────────────
        materials: List[JobMaterial] = []
        for m in obj.get("JobMtl") or []:
            pn = str(m.get("PartNum", "") or "").strip()
            if not pn:
                continue
            materials.append(JobMaterial(
                company      = self.company,
                job_num      = job_num,
                assembly_seq = int(m.get("AssemblySeq", 0) or 0),
                mtl_seq      = int(m.get("MtlSeq", 0) or 0),
                part_num     = pn,
                description  = m.get("Description") or None,
                required_qty = float(m.get("RequiredQty", 0) or 0),
                issued_qty   = float(m.get("IssuedQty", 0) or 0),
                uom          = m.get("UOMCode") or m.get("IUM") or None,
                buy_it       = bool(m.get("BuyIt", False)),
            ))

        materials.sort(key=lambda m: (m.assembly_seq, m.mtl_seq))
        return header, materials

    def get_job_pos(self, job_num: str) -> List[POLineMatch]:
        """
        Get all PO lines associated with a job number.

        Queries POSvc/GetRows filtering PORel by JobNum so we capture every
        PO release linked to this job, then joins with POHeader and PODetail.
        """
        url = f"{self.base_url}/api/v2/odata/{self.company}/Erp.BO.POSvc/GetRows"

        all_poheader: List[Dict[str, Any]] = []
        all_podetail: List[Dict[str, Any]] = []
        all_porel:    List[Dict[str, Any]] = []

        page = 1
        while True:
            payload: Dict[str, Any] = {
                "whereClausePOHeader": "",
                "whereClausePODetail": "",
                "whereClausePORel":    f"JobNum = '{job_num}'",
                **{k: "" for k in self._getrows_extras
                   if k not in ("whereClausePOHeader", "whereClausePODetail", "whereClausePORel")},
                "pageSize":    200,
                "absolutePage": page,
            }

            resp = self._post_getrows_with_optional_learning(url, payload)
            tableset, parameters = self._extract_tableset(resp)

            all_poheader.extend(tableset.get("POHeader") or [])
            all_podetail.extend(tableset.get("PODetail") or [])
            all_porel.extend(tableset.get("PORel")    or [])

            if not self._more_pages(parameters):
                break
            page += 1
            if page > 50:
                break

        # Index headers by PONum
        header_by_po: Dict[int, Dict] = {}
        for h in all_poheader:
            pon = h.get("PONum") or h.get("PONUM")
            if isinstance(pon, int):
                header_by_po.setdefault(pon, h)

        # Index details by (PONum, POLine)
        detail_by_key: Dict[Tuple[int, int], Dict] = {}
        for d in all_podetail:
            pon = d.get("PONUM") or d.get("PONum")
            pol = d.get("POLine")
            if isinstance(pon, int) and isinstance(pol, int):
                detail_by_key.setdefault((pon, pol), d)

        # Build one POLineMatch per unique (PONum, POLine, PORelNum) release
        seen: set = set()
        results: List[POLineMatch] = []

        for r in all_porel:
            pon = r.get("PONum") or r.get("PONUM")
            pol = r.get("POLine")
            rel = r.get("PORelNum", 0)
            if not (isinstance(pon, int) and isinstance(pol, int)):
                continue

            key = (pon, pol, rel)
            if key in seen:
                continue
            seen.add(key)

            h = header_by_po.get(pon, {})
            d = detail_by_key.get((pon, pol), {})

            po_open   = self._get_bool(h, "OpenOrder")
            line_open = self._get_bool(d, "OpenLine")
            void_line = self._get_bool(d, "VoidLine")

            open_rel  = self._get_bool(r, "OpenRelease") or self._get_bool(r, "OpenRel")
            recv_qty  = self._get_float(r, "ReceivedQty")
            due_date  = r.get("DueDate") or None

            order_qty  = self._get_float(d, "OrderQty")
            unit_cost  = self._get_float(d, "UnitCost")
            order_date = h.get("OrderDate") or None

            vendor_name = (
                h.get("VendorNumName") or
                h.get("VendorName")   or
                h.get("VenName")      or None
            )

            if void_line:
                status = "void"
            elif po_open or line_open or open_rel:
                status = "open"
            elif po_open is False and line_open is False:
                status = "closed"
            else:
                status = "unknown"

            received_complete = None
            if recv_qty is not None and order_qty is not None:
                received_complete = (recv_qty >= order_qty - 1e-9) and (status != "open")

            results.append(POLineMatch(
                company          = self.company,
                po_num           = pon,
                po_line          = pol,
                order_date       = order_date,
                due_date         = due_date,
                vendor_name      = vendor_name,
                part_num         = str(d.get("PartNum") or ""),
                line_desc        = d.get("LineDesc") or d.get("CommentText") or None,
                order_qty        = order_qty,
                unit_cost        = unit_cost,
                po_open          = po_open,
                line_open        = line_open,
                void_line        = void_line,
                any_open_release = open_rel,
                received_qty     = recv_qty,
                status           = status,
                received_complete= received_complete,
            ))

        results.sort(key=lambda p: (p.po_num, p.po_line))
        return results

    def get_part_revisions(self, partnum: str) -> list:
        """
        Return all PartRev rows for *partnum* from Epicor.

        Each item is a dict with keys: RevisionNum, Approved, EffectiveDate, ApprovedDate.
        Returns [] on any error or if the part is not found.
        """
        try:
            url = f"{self.base_url}/api/v2/odata/{self.company}/Erp.BO.PartSvc/GetByID"
            resp = self.session.post(
                url,
                params={"api-key": self.api_key},
                json={"partNum": partnum},
                verify=False,
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json().get("returnObj", {}).get("PartRev", [])
        except Exception:
            return []

    def _get_part_revision(self, partnum: str) -> Optional[str]:
        """
        Get the latest approved revision for a part.

        Primary: POST to PartSvc/GetByID (avoids OData $filter encoding bug).
        Returns the latest approved revision by EffectiveDate, or latest unapproved if none approved.
        """
        # Primary: PartSvc/GetByID POST — returns PartRev rows without OData $filter issues
        try:
            url = f"{self.base_url}/api/v2/odata/{self.company}/Erp.BO.PartSvc/GetByID"
            resp = self.session.post(
                url,
                params={"api-key": self.api_key},
                json={"partNum": partnum},
                verify=False,
                timeout=15,
            )
            resp.raise_for_status()
            part_revs = resp.json().get("returnObj", {}).get("PartRev", [])
            if part_revs:
                approved = [r for r in part_revs if r.get("Approved")]
                candidates = approved if approved else part_revs
                candidates.sort(key=lambda r: r.get("EffectiveDate", "") or "", reverse=True)
                return candidates[0].get("RevisionNum")
        except Exception:
            pass

        # Fallback: OData endpoints (may fail silently due to $filter encoding bug)
        for endpoint in [
            "Erp.BO.PartSvc/PartRevs",
            "Erp.BO.EngWorkBenchSvc/EcoRevs",
        ]:
            try:
                records = self._get_odata(
                    endpoint,
                    f"PartNum eq '{partnum}'",
                    ["PartNum", "RevisionNum", "Approved", "EffectiveDate"],
                )
                if not records:
                    continue
                approved = [r for r in records if r.get("Approved")]
                candidates = approved if approved else records
                candidates.sort(key=lambda r: r.get("EffectiveDate", "") or "", reverse=True)
                return candidates[0].get("RevisionNum")
            except EpicorError:
                continue

        return None
