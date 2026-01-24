import json
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

PARTNUM  = "100100"   # change as needed
PAGE_SIZE = 200
MAX_PAGES = 200       # safety
# ---------------------------

def extract_tableset(resp_json: dict):
    # Epicor often returns {"returnObj": {...}, "parameters": {...}}
    if isinstance(resp_json, dict) and "returnObj" in resp_json:
        return resp_json["returnObj"], resp_json.get("parameters", {})
    return resp_json, resp_json.get("parameters", {}) if isinstance(resp_json, dict) else ({}, {})

def get_more_pages(parameters: dict) -> bool:
    # Seen variants in Epicor responses
    for k in ("morePages", "MorePages"):
        if k in parameters:
            return bool(parameters[k])
    return False

def main():
    url = f"{BASE_URL}/api/v2/odata/{COMPANY}/Erp.BO.POSvc/GetRows"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "CallSettings": json.dumps({"Company": COMPANY, "Plant": PLANT}),
    }

    session = requests.Session()
    session.auth = (USERNAME, PASSWORD)

    all_podetail = []
    all_poheader = []

    for page in range(MAX_PAGES):
        payload = {
            # Epicor requires these keys to exist, even if blank
            "whereClausePOHeader": "",
            "whereClausePODetail": f"PartNum = '{PARTNUM}'",
            "whereClausePORel": "",

            "pageSize": int(PAGE_SIZE),
            "absolutePage": int(page),
        }

        r = session.post(
            url,
            params={"api-key": API_KEY},
            headers=headers,
            json=payload,
            timeout=60
        )

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
            # Keep only matching part rows (defensive)
            all_podetail.extend([d for d in podet if d.get("PartNum") == PARTNUM])

        if not get_more_pages(out_params):
            break

    # Index headers by PO number for OrderDate (if present)
    header_by_po = {}
    for h in all_poheader:
        pon = h.get("PONum") or h.get("PONUM")
        if pon is not None and pon not in header_by_po:
            header_by_po[pon] = h

    # Print deduped detail rows
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
        if ponum in header_by_po:
            order_date = header_by_po[ponum].get("OrderDate")

        print(f"PONUM={ponum}  OrderDate={order_date}  Line={poline}  Qty={qty}  UnitCost={cost}  Desc={desc}")

    print()
    print(f"Matched PODetail rows: {len(seen)}")
    print(f"Unique POs: {len(set(k[0] for k in seen))}")

if __name__ == "__main__":
    main()
