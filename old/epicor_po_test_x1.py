import json
import getpass
import requests

BASE = "https://ausmtsapp01.epicorsaas.com/saas205"
COMPANY = "19593"
API_KEY = "1UYtFXnhV0Z8afafsUUFfAoeMx5XKlfaEJIVrI1pDG8Lr"

#USERNAME = input("Epicor username: ")
#PASSWORD = getpass.getpass("Epicor password: ")

USERNAME = "19593RKowalczyk"
PASSWORD = "dirtRockAlbum33"

PARTNUM = "100100"
PLANT = "MfgSys"  # from your PlantSvc/List output

url = f"{BASE}/api/v2/odata/{COMPANY}/Erp.BO.POSvc/PODetails"

params = {
    "$filter": f"PartNum eq '{PARTNUM}'",
    "$count": "true",
    "$top": "1000",
    # Use exact property casing from your payload:
    "$select": "Company,PONUM,POLine,PartNum,LineDesc,OrderQty,UnitCost,ChangeDate",
    "api-key": API_KEY,
}

headers = {
    "Accept": "application/json",
    "CallSettings": json.dumps({"Company": COMPANY, "Plant": PLANT}),
}

r = requests.get(url, params=params, headers=headers, auth=(USERNAME, PASSWORD), timeout=60)

print("REQUEST URL:", r.request.url)
print("STATUS:", r.status_code)
if not r.ok:
    print(r.text)
    r.raise_for_status()

data = r.json()
print("@odata.count:", data.get("@odata.count"))
for row in data.get("value", []):
    print(row.get("PONUM"), row.get("POLine"), row.get("PartNum"), row.get("ChangeDate"))
