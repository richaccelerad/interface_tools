import argparse
import json
import sys
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests


def _append_api_key_to_url(url: str, api_key: str) -> str:
    """If Epicor returns @odata.nextLink without api-key, add it."""
    if not api_key:
        return url
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    if "api-key" not in qs:
        qs["api-key"] = [api_key]
    new_query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def fetch_odata_all(session: requests.Session, url: str, params: dict | None = None, api_key_qs: str | None = None):
    """
    Fetches an OData feed and follows @odata.nextLink if present.
    Returns: (count, rows)
    """
    rows = []
    count = None

    next_url = url
    next_params = params

    while next_url:
        r = session.get(next_url, params=next_params, timeout=60)
        r.raise_for_status()
        data = r.json()

        # First page may have @odata.count if $count=true
        if count is None and "@odata.count" in data:
            count = data["@odata.count"]

        if "value" in data and isinstance(data["value"], list):
            rows.extend(data["value"])
        else:
            # Some endpoints return a single entity instead of a feed
            return count, data

        next_link = data.get("@odata.nextLink")
        if next_link:
            # nextLink often includes all query options except api-key; add if needed
            next_url = _append_api_key_to_url(next_link, api_key_qs or "")
            next_params = None  # nextLink is already a full URL
        else:
            next_url = None

    return count, rows


def make_session(api_key: str, api_key_mode: str, company: str, plant: str | None):
    s = requests.Session()
    headers = {
        "Accept": "application/json",
    }

    # API key: either query string (api-key=...) or header (x-api-key)
    if api_key_mode == "header":
        headers["x-api-key"] = api_key  # commonly used with Epicor REST v2
    elif api_key_mode == "query":
        pass
    else:
        raise ValueError("api_key_mode must be 'query' or 'header'")

    # CallSettings header to force Company/Plant context (when needed)
    if plant:
        headers["CallSettings"] = json.dumps({"Company": company, "Plant": plant})

    s.headers.update(headers)
    return s


def po_details_with_callsettings(base: str, company: str, api_key: str, api_key_mode: str, plant: str | None, partnum: str):
    """
    Queries Erp.BO.POSvc/PODetails filtered by PartNum, optionally forcing Plant via CallSettings.
    """
    url = f"{base}/api/v2/odata/{company}/Erp.BO.POSvc/PODetails"
    params = {
        "$filter": f"PartNum eq '{partnum}'",
        "$count": "true",
        "$top": "1000",
        # Use the exact property name casing you see in the payload:
        # "$orderby": "PONUM desc",
        "$select": "Company,PONUM,POLine,PartNum,VenPartNum,LineDesc,UnitCost,OrderQty,ChangeDate",
    }

    # Add api-key in query string if that's your mode
    if api_key_mode == "query":
        params["api-key"] = api_key

    s = make_session(api_key, api_key_mode, company, plant)
    count, rows = fetch_odata_all(s, url, params=params, api_key_qs=(api_key if api_key_mode == "query" else None))
    return count, rows


def baq_data(base: str, company: str, api_key: str, api_key_mode: str, plant: str | None, baq_name: str, partnum: str):
    """
    Queries a BAQ via .../api/v2/odata/{Company}/BaqSvc/{BaqName}/Data

    You must create the BAQ in Epicor first. Recommended BAQ:
      - Join POHeader + PODetail
      - Filter on PODetail.PartNum = parameter (e.g., PartNum)
      - Select fields you need (PONum, POLine, OrderDate, VendorID, etc.)
    """
    url = f"{base}/api/v2/odata/{company}/BaqSvc/{baq_name}/Data"

    # Two common patterns:
    # (A) BAQ parameter passed as query param (if your BAQ defines it as a prompt/parameter)
    # (B) OData $filter against BAQ output columns
    #
    # Start with (A):
    params = {
        "PartNum": partnum,   # if your BAQ parameter is named PartNum
        "$count": "true",
        "$top": "1000",
    }

    if api_key_mode == "query":
        params["api-key"] = api_key

    s = make_session(api_key, api_key_mode, company, plant)
    count, rows = fetch_odata_all(s, url, params=params, api_key_qs=(api_key if api_key_mode == "query" else None))
    return count, rows


def main():
    ap = argparse.ArgumentParser(description="Epicor PO lookup by PartNum (CallSettings or BAQ).")
    ap.add_argument("--base", required=True, help="Example: https://ausmtsapp01.epicorsaas.com/saas205")
    ap.add_argument("--company", required=True, help="Company ID, e.g. 19593")
    ap.add_argument("--api-key", required=True, help="Epicor REST v2 API key")
    ap.add_argument("--api-key-mode", choices=["query", "header"], default="query",
                    help="Use 'query' to send api-key=... or 'header' to send x-api-key: ...")
    ap.add_argument("--plant", default=None, help="Optional Plant/Site to force via CallSettings header")
    ap.add_argument("--partnum", required=True, help="Part number to search for in PODetails")
    ap.add_argument("--mode", choices=["callsettings", "baq"], default="callsettings",
                    help="callsettings = query PODetails with optional Plant context. baq = query BAQ endpoint.")
    ap.add_argument("--baq-name", default=None, help="Required if --mode baq (your BAQ ID/name)")
    args = ap.parse_args()

    try:
        if args.mode == "callsettings":
            count, rows = po_details_with_callsettings(
                base=args.base.rstrip("/"),
                company=args.company,
                api_key=args.api_key,
                api_key_mode=args.api_key_mode,
                plant=args.plant,
                partnum=args.partnum,
            )
        else:
            if not args.baq_name:
                print("ERROR: --baq-name is required when --mode baq", file=sys.stderr)
                sys.exit(2)
            count, rows = baq_data(
                base=args.base.rstrip("/"),
                company=args.company,
                api_key=args.api_key,
                api_key_mode=args.api_key_mode,
                plant=args.plant,
                baq_name=args.baq_name,
                partnum=args.partnum,
            )

        # Pretty print a compact summary
        if isinstance(rows, list):
            print(f"@odata.count = {count}  (rows returned = {len(rows)})")
            for r in rows[:50]:
                # Handle both PODetails rows (PONUM) and BAQ rows (unknown cols)
                pon = r.get("PONUM") or r.get("PONum") or r.get("PONumber") or r.get("PONum_PONum")
                pol = r.get("POLine") or r.get("POline")
                print(f"PO={pon}  Line={pol}  Part={r.get('PartNum')}  Desc={r.get('LineDesc')}")
        else:
            print(json.dumps(rows, indent=2))

    except requests.HTTPError as e:
        print("HTTP ERROR:", e, file=sys.stderr)
        try:
            print("Response body:", e.response.text, file=sys.stderr)
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()