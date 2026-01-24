import json
import re
import requests

# ---------------------------
# CONFIG (edit these)
# ---------------------------
BASE_URL = "https://ausmtsapp01.epicorsaas.com/saas205"
COMPANY  = "19593"
PLANT    = "MfgSys"

API_KEY = "1UYtFXnhV0Z8afafsUUFfAoeMx5XKlfaEJIVrI1pDG8Lr"
USERNAME = "19593RKowalczyk"
PASSWORD = "dirtRockAlbum33"

PARTNUM   = "100100"
PAGE_SIZE = 200
MAX_PAGES = 300         # safety
MAX_LEARN_RETRIES = 30  # safety (how many missing params we'll auto-add)
# ---------------------------

MISSING_PARAM_RE = re.compile(r"Parameter\s+([A-Za-z0-9_]+)\s+is not found in the input object")

def extract_tableset(resp_json: dict):
    # Epicor often returns {"returnObj": {...}, "parameters": {...}}
    if isinstance(resp_json, dict) and "returnObj" in resp_json:
        return resp_json["returnObj"], resp_json.get("parameters", {})
    return resp_json, resp_json.get("parameters", {}) if isinstance(resp_json, dict) else ({}, {})

def get_more_pages(parameters: dict) -> bool:
    for k in ("morePages", "MorePages"):
        if k in parameters:
            return bool(parameters[k])
    return False

def build_base_payload(partnum: str, page_size: int, absolute_page: int) -> dict:
    # Start with the ones we already know about; we’ll auto-add the rest if Epicor complains.
    return {
        "whereClausePOHeader": "",
        "whereClausePODetail": f"PartNum = '{partnum}'",
        "whereClausePORel": "",
        "pageSize": int(page_size),
        "absolutePage": int(absolute_page),
    }

def learn_required_params(session: requests.Session, url: str, headers: dict) -> dict:
    """
    Probe GetRows page 0 and auto-add any missing parameters Epicor demands.
    Returns a dict of extra required params with default values (usually "").
    """
    extras = {}
    payload = build_base_payload(PARTNUM, PAGE_SIZE, 0) | extras

    for _ in range(MAX_LEARN_RETRIES):
        r = session.post(url, params={"api-key": API_KEY}, headers=headers, json=payload, timeout=60)

        if r.ok:
            return extras

        # Try to parse Epicor error response
        try:
            err = r.json()
            msg = err.get("ErrorMessage", "") or ""
        except Exception:
            msg = r.text

        m = MISSING_PARAM_RE.search(msg)
        if not m:
            # Not the “missing parameter” case; surface details and stop.
            print("STATUS:", r.status_code)
            print("REQUEST URL:", r.request.url)
            print("RESPONSE:", r.text)
            r.raise_for_status()

        missing_name = m.group(1)

        # Default: most of these are whereClause* strings; if Epicor asks for something else,
        # we’ll still add empty string (works for whereClause params).
        extras[missing_name] = ""
        payload = build_base_payload(PARTNUM, PAGE_SIZE, 0) | extras
        print(f"Learned required param: {missing_name}")

    raise RuntimeError(f"Exceeded MAX_LEARN_RETRIES={MAX_LEARN_RETRIES} while learning required params.")

def main():
    url = f"{BASE_URL}/api/v2/odata/{COMPANY}/Erp.BO.POSvc/GetRows"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "CallSettings": json.dumps({"Company": COMPANY, "Plant": PLANT}),
    }

    session = requests.Session()
    session.auth = (USERNAME, PASSWORD)

    # Step 1: Learn any extra required parameters Epicor demands (e.g., whereClausePOHeaderAttch)
    extras = learn_required_params(session, url, headers)
    print(f"Extra params learned: {len(extras)}")

    # Step 2: Page through results
    all_podetail = []
    all_poheader = []

    for page in range(MAX_PAGES):
        payload = build_base_payload(PARTNUM, PAGE_SIZE, page) | extras

        r = session.post(url, params={"api-key": API_KEY}, headers=headers, json=payload, timeout=60)
        if not r.ok:
            print("STATUS:", r.status_code)
            print("REQUEST URL:", r.request.url)
            print("RESPONSE:", r.text)
            r.raise_for_status()

        tableset, out_params = extract_tableset(r.json())

        pohead = tableset.get("POHeader") or tableset.get("POHeaders") or []
        podet  = tableset.get("PODetail") or tableset.get("PODetails") or []

        if isinstance(pohead, list):
            all_poheader.extend(pohead)
        if isinstance(podet, list):
            all_podetail.extend([d for d in podet if d.get("PartNum") == PARTNUM])

        if not get_more_pages(out_params):
            break

    # Step 3: Summarize results
    header_by_po = {}
    for h in all_poheader:
        pon = h.get("PONum") or h.get("PONUM")
        if pon is not None and pon not in header_by_po:
            header_by_po[pon] = h

    seen = set()
    rows = sorted(all_podetail, key=lambda d: (d.get("PONUM", 0), d.get("POLine", 0)))

    for d in rows:
        key = (d.get("PONUM"), d.get("POLine"))
        if key in seen:
            continue
        seen.add(key)

        ponum = d.get("PONUM")
        poline = d.get("POLine")
        desc = d.get("LineDesc")
        qty = d.get("OrderQty")
        cost = d.get("UnitCost")

        order_date = None
        h = header_by_po.get(ponum)
        if h:
            order_date = h.get("OrderDate")

        print(f"PONUM={ponum}  OrderDate={order_date}  Line={poline}  Qty={qty}  UnitCost={cost}  Desc={desc}")

    print()
    print(f"Matched PODetail rows: {len(seen)}")
    print(f"Unique POs: {len(set(k[0] for k in seen))}")

if __name__ == "__main__":
    main()
