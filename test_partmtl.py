"""Test different methods to access PartMtl data."""
from epicor_po_x2 import EpicorClient, EpicorError
import config
import json

c = EpicorClient(
    base_url=config.EPICOR_BASE_URL,
    company=config.EPICOR_COMPANY,
    api_key=config.EPICOR_API_KEY,
    username=config.EPICOR_USERNAME,
    password=config.EPICOR_PASSWORD,
    plant=config.EPICOR_PLANT,
)

partnum = "102892"
print(f"Testing PartMtl access for part: {partnum}")
print("=" * 60)

# Method 1: Try PartSvc/GetByID to get full part record with children
print("\n1. PartSvc/GetByID:")
try:
    url = f"{c.base_url}/api/v2/odata/{c.company}/Erp.BO.PartSvc/GetByID"
    payload = {"partNum": partnum}
    resp = c._post_json_raw(url, payload)
    if "returnObj" in resp:
        keys = list(resp["returnObj"].keys())
        print(f"   returnObj keys: {keys}")
        for k, v in resp["returnObj"].items():
            if isinstance(v, list) and len(v) > 0:
                print(f"   {k}: {len(v)} records")
                if k == "PartMtl" and v:
                    print(f"      First PartMtl: {v[0].get('MtlPartNum')}")
except EpicorError as e:
    print(f"   Error: {e}")

# Method 2: Try BOMSearchSvc/GetRows
print("\n2. BOMSearchSvc/GetRows:")
try:
    url = f"{c.base_url}/api/v2/odata/{c.company}/Erp.BO.BOMSearchSvc/GetRows"
    payload = {
        "whereClauseBOMHead": f"PartNum = '{partnum}'",
        "whereClauseBOMMtl": "",
        "pageSize": 100,
        "absolutePage": 0,
    }
    resp = c._post_json_raw(url, payload)
    print(f"   Response keys: {list(resp.keys())}")
    if "returnObj" in resp:
        print(f"   returnObj keys: {list(resp['returnObj'].keys())}")
        for k, v in resp["returnObj"].items():
            if isinstance(v, list):
                print(f"   {k}: {len(v)} records")
except EpicorError as e:
    print(f"   Error: {e}")

# Method 3: Try to get part revisions to see what revision has the BOM
print("\n3. Part Revisions for 102892:")
try:
    records = c._get_odata(
        "Erp.BO.EngWorkBenchSvc/EcoRevs",
        f"PartNum eq '{partnum}'",
        ["PartNum", "RevisionNum", "Approved", "EffectiveDate", "CheckedOut"]
    )
    print(f"   EcoRevs: {len(records)} records")
    for r in records:
        print(f"      Rev: {r.get('RevisionNum')}, Approved: {r.get('Approved')}")
except EpicorError as e:
    print(f"   EcoRevs Error: {e}")

# Method 4: Try querying with a specific revision
print("\n4. ECOMtls with different revisions:")
for rev in ["", "A", "X1", "01"]:
    try:
        filter_clause = f"PartNum eq '{partnum}'"
        if rev:
            filter_clause += f" and RevisionNum eq '{rev}'"
        records = c._get_odata(
            "Erp.BO.EngWorkBenchSvc/ECOMtls",
            filter_clause,
            ["PartNum", "MtlPartNum", "RevisionNum"]
        )
        print(f"   Rev '{rev or '(none)'}': {len(records)} records")
    except EpicorError as e:
        print(f"   Rev '{rev or '(none)'}': Error - {e}")
