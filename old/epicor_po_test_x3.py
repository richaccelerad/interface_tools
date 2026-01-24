import json
import getpass
import requests
import xml.etree.ElementTree as ET

BASE = "https://ausmtsapp01.epicorsaas.com/saas205"
COMPANY = "19593"
PLANT = "MfgSys"

EDMX = "{http://docs.oasis-open.org/odata/ns/edmx}"
EDM  = "{http://docs.oasis-open.org/odata/ns/edm}"

def find_getrows_params(metadata_xml: str):
    """
    Return list of (name, type, mode) for Action Name='GetRows'
    """
    root = ET.fromstring(metadata_xml)
    for action in root.findall(f".//{EDM}Action[@Name='GetRows']"):
        out = []
        for p in action.findall(f"{EDM}Parameter"):
            out.append((p.attrib.get("Name"), p.attrib.get("Type"), p.attrib.get("Mode", "In")))
        return out
    raise RuntimeError("GetRows action not found in POSvc $metadata.")

def default_for_edm(edm_type: str):
    if edm_type == "Edm.String":
        return ""
    if edm_type == "Edm.Int32":
        return 0
    if edm_type == "Edm.Boolean":
        return False
    # fall back
    return None

def extract_tableset(resp_json: dict):
    # Epicor often returns {"returnObj": {...}, "parameters": {...}}
    if isinstance(resp_json, dict) and "returnObj" in resp_json:
        return resp_json["returnObj"], resp_json.get("parameters", {})
    return resp_json, resp_json.get("parameters", {}) if isinstance(resp_json, dict) else ({}, {})

def get_more_pages(parameters: dict):
    # seen variants: morePages / MorePages
    for k in ("morePages", "MorePages"):
        if k in parameters:
            return bool(parameters[k])
    return False

def main():
    #api_key = getpass.getpass("Epicor API key (input hidden): ").strip()
    api_key = "1UYtFXnhV0Z8afafsUUFfAoeMx5XKlfaEJIVrI1pDG8Lr"
    
    #username = input("Epicor username: ").strip()
    #password = getpass.getpass("Epicor password: ")
    username = "19593RKowalczyk"
    password = "dirtRockAlbum33"
    
    partnum  = input("PartNum (e.g. 100100): ").strip()
    partnum = '100100'

    auth = (username, password)

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "CallSettings": json.dumps({"Company": COMPANY, "Plant": PLANT}),
    }

    # 1) Pull POSvc metadata so we don't guess parameter names
    meta_url = f"{BASE}/api/v2/odata/{COMPANY}/Erp.BO.POSvc/$metadata"
    meta_r = requests.get(meta_url, params={"api-key": api_key}, headers={"Accept": "application/xml"}, auth=auth, timeout=60)
    meta_r.raise_for_status()

    params = find_getrows_params(meta_r.text)

    # Build payload with defaults
    payload = {}
    for name, typ, mode in params:
        if not name or mode.lower() == "out":
            continue
        payload[name] = default_for_edm(typ)

    # Fill in the important ones (names vary a bit, so match by substring)
    def set_if_present(substr: str, value):
        for k in payload.keys():
            if substr.lower() in k.lower():
                payload[k] = value

    set_if_present("whereClausePODetail", f"PartNum = '{partnum}'")
    set_if_present("pageSize", 200)
    set_if_present("absolutePage", 0)

    # 2) Call GetRows and page
    url = f"{BASE}/api/v2/odata/{COMPANY}/Erp.BO.POSvc/GetRows"

    all_detail_rows = []
    all_header_rows = []

    page = 0
    while True:
        set_if_present("absolutePage", page)

        r = requests.post(url, params={"api-key": api_key}, headers=headers, json=payload, auth=auth, timeout=60)
        if not r.ok:
            print("STATUS:", r.status_code)
            print("REQUEST URL:", r.request.url)
            print(r.text)
            r.raise_for_status()

        tableset, out_params = extract_tableset(r.json())

        # Common table names in the tableset
        pohead = tableset.get("POHeader") or tableset.get("POHeaders") or []
        podet  = tableset.get("PODetail") or tableset.get("PODetails") or []

        if isinstance(pohead, list):
            all_header_rows.extend(pohead)
        if isinstance(podet, list):
            # keep only matching part rows (sometimes GetRows can return more)
            all_detail_rows.extend([x for x in podet if x.get("PartNum") == partnum])

        if not get_more_pages(out_params):
            break
        page += 1
        if page > 500:
            raise RuntimeError("Paging safety stop: exceeded 500 pages.")

    # 3) Print results (dedupe by PONUM+POLine)
    header_by_po = {}
    for h in all_header_rows:
        # Epicor often uses PONum in POHeader
        pon = h.get("PONum") or h.get("PONUM")
        if pon is not None and pon not in header_by_po:
            header_by_po[pon] = h

    seen = set()
    rows = sorted(all_detail_rows, key=lambda d: (d.get("PONUM", 0), d.get("POLine", 0)))
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
        if ponum in header_by_po:
            order_date = header_by_po[ponum].get("OrderDate")

        print(f"PONUM={ponum}  OrderDate={order_date}  Line={poline}  Qty={qty}  UnitCost={cost}  Desc={desc}")

    print()
    print(f"Matched PODetail rows: {len(seen)}")
    print(f"Unique POs: {len(set(k[0] for k in seen))}")

if __name__ == "__main__":
    main()
