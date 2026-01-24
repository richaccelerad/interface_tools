import requests
import getpass


BASE = "https://ausmtsapp01.epicorsaas.com/saas205"
COMPANY = "19593"
API_KEY = "1UYtFXnhV0Z8afafsUUFfAoeMx5XKlfaEJIVrI1pDG8Lr"

#USERNAME = input("Epicor username: ")
#PASSWORD = getpass.getpass("Epicor password: ")

USERNAME = "19593RKowalczyk"
PASSWORD = "dirtRockAlbum33"


PONUM = 32210  # <-- put one of the "missing" PO numbers here

#url = f"{BASE}/api/v2/odata/{COMPANY}/Erp.BO.POSvc/POes('{COMPANY}',{PONUM})/PODetails"
url = f"{BASE}/api/v2/odata/{COMPANY}/Erp.BO.POSvc/POes/PODetails"

if(False):
    params = {
        "$select": "PONUM,POLine,PartNum,VenPartNum,LineDesc,OrderQty,UnitCost",
        "api-key": API_KEY,
    }

params = {
    "$filter": "(PartNum eq '100100' or VenPartNum eq '100100')",
    "$count": "true",
    "$top": "1000",
    "$select": "Company,PONUM,POLine,PartNum,VenPartNum,LineDesc,OrderQty,UnitCost,ChangeDate",
    "api-key": API_KEY,
}

r = requests.get(url, params=params, auth=(USERNAME, PASSWORD), timeout=60)
print("STATUS:", r.status_code)
print(r.text[:1000])
r.raise_for_status()

data = r.json()
print("Rows:", len(data.get("value", [])))
for row in data.get("value", []):
    print(row.get("PONUM"), row.get("POLine"), row.get("PartNum"), row.get("VenPartNum"), row.get("LineDesc"))
