import json
import getpass
import requests

BASE = "https://ausmtsapp01.epicorsaas.com/saas205"
COMPANY = "19593"
PLANT = "MfgSys"
API_KEY = "1UYtFXnhV0Z8afafsUUFfAoeMx5XKlfaEJIVrI1pDG8Lr"

PARTNUM = "100100"

def get_all_pages(url, *, params, headers, auth, timeout=60):
    """Fetch all pages following @odata.nextLink if present."""
    out = []
    while True:
        r = requests.get(url, params=params, headers=headers, auth=auth, timeout=timeout)
        if not r.ok:
            print("REQUEST URL:", r.request.url)
            print("STATUS:", r.status_code)
            print(r.text)
            r.raise_for_status()

        data = r.json()
        out.extend(data.get("value", []))

        next_link = data.get("@odata.nextLink")
        if not next_link:
            return out

        # nextLink is usually a full URL; carry on without params.
        url = next_link
        params = None

def main():
    #username = input("Epicor username: ")
    #password = getpass.getpass("Epicor password: ")
    username = "19593RKowalczyk"
    password = "dirtRockAlbum33"

    auth = (username, password)


    headers = {
        "Accept": "application/json",
        "CallSettings": json.dumps({"Company": COMPANY, "Plant": PLANT}),
    }

    # Step 1: Find PO headers that have ANY PODetail with this PartNum
    po_url = f"{BASE}/api/v2/odata/{COMPANY}/Erp.BO.POSvc/POes"
    po_params = {
        "$select": "PONum,OrderDate",
        "$filter": f"PODetails/any(d:d/PartNum eq '{PARTNUM}')",
        "$orderby": "OrderDate desc",
        "$top": "200",
        "api-key": API_KEY,
    }

    po_headers = get_all_pages(po_url, params=po_params, headers=headers, auth=auth)
    print(f"Found {len(po_headers)} PO headers with PartNum {PARTNUM}")

    # Step 2: For each PO, fetch ONLY the matching PODetail lines
    for po in po_headers:
        ponum = po["PONum"]
        details_url = f"{BASE}/api/v2/odata/{COMPANY}/Erp.BO.POSvc/POes('{COMPANY}',{ponum})/PODetails"
        details_params = {
            "$select": "PONUM,POLine,PartNum,LineDesc,OrderQty,UnitCost",
            "$filter": f"PartNum eq '{PARTNUM}'",
            "$top": "1000",
            "api-key": API_KEY,
        }
        lines = get_all_pages(details_url, params=details_params, headers=headers, auth=auth)

        for ln in lines:
            print(
                f"PONum={ln.get('PONUM')}  POLine={ln.get('POLine')}  "
                f"Qty={ln.get('OrderQty')}  UnitCost={ln.get('UnitCost')}  "
                f"Desc={ln.get('LineDesc')}"
            )

if __name__ == "__main__":
    main()
