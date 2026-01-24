from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
import json
import re
import requests


_MISSING_PARAM_RE = re.compile(r"Parameter\s+([A-Za-z0-9_]+)\s+is not found in the input object")


@dataclass(frozen=True)
class POLineMatch:
    """A normalized PO line result (JSON-friendly via asdict())."""
    company: str
    po_num: int
    po_line: int
    order_date: Optional[str]      # ISO-ish string from Epicor (can be None)
    part_num: str
    line_desc: Optional[str]
    order_qty: Optional[float]
    unit_cost: Optional[float]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class EpicorError(RuntimeError):
    pass


class EpicorClient:
    """
    Minimal Epicor REST v2 client for POSvc.GetRows-based queries.
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
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.company = company
        self.api_key = api_key
        self.timeout_s = timeout_s

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

        # Cache for "extra required params" Epicor demands for GetRows
        self._getrows_extras: Dict[str, Any] = {}

    def _post_json(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
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
        except Exception:
            raise EpicorError(f"HTTP {r.status_code} calling {r.request.url}: {r.text}") from None

        msg = err.get("ErrorMessage") or err.get("Message") or r.text
        raise EpicorError(f"HTTP {r.status_code} calling {r.request.url}: {msg}")

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

    def _ensure_getrows_extras(self, partnum: str, page_size: int) -> None:
        """
        Learns any missing required GetRows parameters once and caches them.
        """
        if self._getrows_extras:
            return

        url = f"{self.base_url}/api/v2/odata/{self.company}/Erp.BO.POSvc/GetRows"

        base_payload = {
            "whereClausePOHeader": "",
            "whereClausePODetail": f"PartNum = '{partnum}'",
            "whereClausePORel": "",
            "pageSize": int(page_size),
            "absolutePage": 0,
        }

        # Probe and learn missing params until the call succeeds
        for _ in range(50):  # safety cap
            try:
                self._post_json(url, {**base_payload, **self._getrows_extras})
                return  # success; extras are sufficient (maybe empty)
            except EpicorError as e:
                m = _MISSING_PARAM_RE.search(str(e))
                if not m:
                    # Not a missing-param error; re-raise
                    raise
                missing = m.group(1)
                # Most of these are whereClause* strings; empty string is the safe default.
                self._getrows_extras[missing] = ""

        raise EpicorError("Failed to learn required GetRows parameters (too many missing-param retries).")

    def get_po_lines_by_partnum(
        self,
        partnum: str,
        page_size: int = 200,
        max_pages: int = 300,
    ) -> List[POLineMatch]:
        """
        Returns all PO detail lines whose PartNum exactly matches `partnum`.
        Uses POSvc.GetRows server-side filtering (Epicor whereClause), not OData $filter.
        """
        self._ensure_getrows_extras(partnum=partnum, page_size=page_size)

        url = f"{self.base_url}/api/v2/odata/{self.company}/Erp.BO.POSvc/GetRows"

        all_poheader: List[Dict[str, Any]] = []
        all_podetail: List[Dict[str, Any]] = []

        for page in range(max_pages):
            payload = {
                "whereClausePOHeader": "",
                "whereClausePODetail": f"PartNum = '{partnum}'",
                "whereClausePORel": "",
                "pageSize": int(page_size),
                "absolutePage": int(page),
                **self._getrows_extras,
            }

            resp = self._post_json(url, payload)
            tableset, out_params = self._extract_tableset(resp)

            pohead = tableset.get("POHeader") or tableset.get("POHeaders") or []
            podet = tableset.get("PODetail") or tableset.get("PODetails") or []

            if isinstance(pohead, list):
                all_poheader.extend([h for h in pohead if isinstance(h, dict)])
            if isinstance(podet, list):
                # Defensive: keep only exact matches
                all_podetail.extend([d for d in podet if isinstance(d, dict) and d.get("PartNum") == partnum])

            if not self._more_pages(out_params):
                break

        # Map header OrderDate by PO number
        header_by_po: Dict[int, Dict[str, Any]] = {}
        for h in all_poheader:
            pon = h.get("PONum")
            if isinstance(pon, int) and pon not in header_by_po:
                header_by_po[pon] = h

        # Normalize + dedupe by (PONUM, POLine)
        seen: set[Tuple[int, int]] = set()
        results: List[POLineMatch] = []

        # Sort for stable output
        all_podetail.sort(key=lambda d: (d.get("PONUM", 0) or 0, d.get("POLine", 0) or 0))

        for d in all_podetail:
            po_num = d.get("PONUM")
            po_line = d.get("POLine")
            if not isinstance(po_num, int) or not isinstance(po_line, int):
                continue

            key = (po_num, po_line)
            if key in seen:
                continue
            seen.add(key)

            order_date = None
            h = header_by_po.get(po_num)
            if h:
                od = h.get("OrderDate")
                if isinstance(od, str):
                    order_date = od

            # Convert numeric fields safely
            qty = d.get("OrderQty")
            cost = d.get("UnitCost")
            qty_f = float(qty) if isinstance(qty, (int, float)) else None
            cost_f = float(cost) if isinstance(cost, (int, float)) else None

            results.append(
                POLineMatch(
                    company=self.company,
                    po_num=po_num,
                    po_line=po_line,
                    order_date=order_date,
                    part_num=str(d.get("PartNum") or ""),
                    line_desc=d.get("LineDesc") if isinstance(d.get("LineDesc"), str) else None,
                    order_qty=qty_f,
                    unit_cost=cost_f,
                )
            )

        return results
