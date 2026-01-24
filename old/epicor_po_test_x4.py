import json
import requests
import xml.etree.ElementTree as ET

# ---------------------------
# CONFIG (edit these)
# ---------------------------
BASE_URL = "https://ausmtsapp01.epicorsaas.com/saas205"
COMPANY  = "19593"
PLANT    = "MfgSys"

API_KEY = "1UYtFXnhV0Z8afafsUUFfAoeMx5XKlfaEJIVrI1pDG8Lr"
USERNAME = "19593RKowalczyk"
PASSWORD = "dirtRockAlbum33"
    

PARTNUM  = "100100"      # change as needed
PAGE_SIZE = 200          # tune as needed
# ---------------------------

EDM_NS = "{http://docs.oasis-open.org/odata/ns/edm}"

def get_posvc_getrows_params(session: requests.Session) -> list[tuple[str, str, str]]:
    """Return [(name, edm_type, mode)] for POSvc Action Name='GetRows'."""
    meta_url = f"{BASE_URL}/api/v2/odata/{COMPANY}/Erp.BO.POSvc/$metadata"
    r = session.get(meta_url, params={"api-key": API_KEY}, headers={"Accept": "application/xml"}, timeout=60)
    r.raise_for_status()

    root = ET.fromstring(r.text)

    # Find Action Name="GetRows" (any schema)
    for action in root.findall(f".//{EDM_NS}Action[@Name='GetRows']"):
        out = []
        for p in action.findall(f"{EDM_NS}Parameter"):
            out.append((p.attrib.get("Name",""), p.attrib.get("Type",""), p.attrib.get("Mode","In")))
        return out

    raise RuntimeError("Could not find Action Name='GetRows' in Erp.BO.POSvc $metadata.")

def default_for_edm(edm_type: str):
    if edm_type == "Edm.String":
        return ""
    if edm_type == "Edm.Int32":
        return 0
    if edm_type == "Edm.Boolean":
        return False
    return None

def extract_tableset(resp_json: dict):
    """Epicor often returns {'returnObj': {...}, 'parameters': {...}}."""
    if isinstance(resp_json, dict) and "returnObj" in resp_json:
        return resp_json["returnObj"], resp_json.get("parameters", {})
    return resp_json, resp_json.get("parameters", {}) if isinstance(resp_json, dict) else ({}, {})

def more_pages(parameters: dict) -> bool:
    for k in ("morePages", "MorePages"):
        if k in parameters:
            return bool(parameters[k])
    return False

def main():
    session = requests.Session()
    session.auth = (USERNAME, PASSWORD)

    # Epicor call settings (common to include)
    session.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json",
        "CallSettings": json.dumps({"Company": COMPANY, "Plant": PLANT}),
    })

    # 1) Discover GetRows parameter names from metadata
    params = get_posvc_getrows_params(session)

    # Build payload with defaults for ALL input params
    payload = {}
    for name, typ, mode in params:
        if not name:
            continue
        if mode.lower() == "out":
            continue
        payload[name] = default_for_edm(typ)

    # Helper: set a param if it exists (case-insensitive match)
    def set_if_present(param_name: str, value):
        for k in payload.keys():
            if k.lower() == param_name.lower():
                payload[k] = value
                return True
        return False

    # 2) Fill required where clauses + paging
    # Important: include whereClausePOHeader even if blank (your error indicates it's required).
    set_if_present("whereClausePOHeader", "")                     # no header filter
    set_if_present("whereClausePORel", "")                        # no release filter
    set_if_present("whereClausePODetail", f"PartNum = '{PARTNUM}'")

    set_if_present("pageSize", int(PAGE_SIZE))
    set_if_present("absolutePage", 0)

    # Sanity check: if your environment expects whereClausePOHeader, make sure it’s present now
    if not any(k.lower() == "whereclausepoheader" for k in payload.keys()):
        raise RuntimeError(f"POSvc.GetRows in your environment does not expose whereClausePOHeader. "
                           f"Params seen: {list(payload.keys())}")

    url = f"{BASE_URL}/api/v2/odata/{COMPANY}/Erp.BO.POSvc/GetRows"

    # 3) Page through results
    all_podetail = []
    all_poheader = []
    page = 0

    while True:
        # update absolutePage if present
        for k in payload.keys():
            if k.lower() == "absolutepage":
                payload[k] = page

        r = session.post(url, params={"api-key": API_KEY}, json=payload, timeout=60)
        if not r.ok:
            print("STATUS:", r.status_code)
            print("REQUEST URL:", r.request.url)
            print(r.text)
            r.raise_for_status()

        tableset, out_params = extract_tableset(r.json())

        pohead = tableset.get("POHeader") or tableset.get("POHeaders") or []
        podet  = tableset.get("PODetail") or tableset.get("PODetails") or []

        if isinstance(pohead, list):
            all_poheader.extend(pohead)
        if isinstance(podet, list):
            all_podetail.extend([d for d in podet if d.get("PartNum") == PARTNUM])

        if not more_pages(out_params):
            break

        page += 1
        if page > 500:
            raise RuntimeError("Paging safety stop (>500 pages).")

    # 4) Summarize results
    header_by_ponum = {}
    for h in all_poheader:
        pon = h.get("PONum") or h.get("PONUM")
        if pon is not None and pon not in header_by_ponum:
            header_by_ponum[pon] = h

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
        h = header_by_ponum.get(ponum)
        if h:
            order_date = h.get("OrderDate")

        print(f"PONUM={ponum}  OrderDate={order_date}  Line={poline}  Qty={qty}  UnitCost={cost}  Desc={desc}")

    print()
    print(f"Matched PODetail rows: {len(seen)}")
    print(f"Unique POs: {len(set(k[0] for k in seen))}")

if __name__ == "__main__":
    main()
