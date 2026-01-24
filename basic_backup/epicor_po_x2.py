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
class POLineMatch:
    """Normalized result for downstream code (JSON-friendly via to_dict())."""
    company: str
    po_num: int
    po_line: int
    order_date: Optional[str]

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
        """
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

            # Try to compute open release and received qty from releases (if fields exist)
            rels = rels_by_line.get(key, [])
            any_open_rel: Optional[bool] = None
            received_qty: Optional[float] = None

            if rels:
                open_flags: List[bool] = []
                recv_sum = 0.0
                recv_any = False

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

                if open_flags:
                    any_open_rel = any(open_flags)
                if recv_any:
                    received_qty = recv_sum

            # Normalize fields
            order_date = None
            od = h.get("OrderDate")
            if isinstance(od, str):
                order_date = od

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
                )
            )

        return results
