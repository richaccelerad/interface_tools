"""Test PartSvc/GetRows with specific where clauses."""
from epicor_po_x2 import EpicorClient, EpicorError
import config

c = EpicorClient(
    base_url=config.EPICOR_BASE_URL,
    company=config.EPICOR_COMPANY,
    api_key=config.EPICOR_API_KEY,
    username=config.EPICOR_USERNAME,
    password=config.EPICOR_PASSWORD,
    plant=config.EPICOR_PLANT,
)

partnum = "102892"
print(f"Testing PartSvc/GetRows for {partnum}")
print("=" * 60)

# Method 1: Try with whereClausePart matching the part
print("\n1. GetRows with whereClausePart (not PartMtl):")
try:
    url = f"{c.base_url}/api/v2/odata/{c.company}/Erp.BO.PartSvc/GetRows"
    payload = {
        "whereClausePart": f"PartNum = '{partnum}'",
        "whereClausePartMtl": "",  # Empty - let it return all related PartMtl
        "pageSize": 100,
        "absolutePage": 0,
    }
    resp = c._post_getrows_with_optional_learning(url, payload)
    print(f"   Response keys: {list(resp.keys())}")
    if "returnObj" in resp:
        print(f"   returnObj keys: {list(resp['returnObj'].keys())}")
        for k, v in resp["returnObj"].items():
            if isinstance(v, list) and len(v) > 0:
                print(f"   {k}: {len(v)} records")
except EpicorError as e:
    print(f"   Error: {e}")

# Method 2: Check if PartMtl is even a valid table in this service
print("\n2. List available 'Mtl' tables in Part service response:")
try:
    url = f"{c.base_url}/api/v2/odata/{c.company}/Erp.BO.PartSvc/GetRows"
    payload = {
        "whereClausePart": f"PartNum = '{partnum}'",
        "pageSize": 1,
        "absolutePage": 0,
    }
    resp = c._post_getrows_with_optional_learning(url, payload)
    if "returnObj" in resp:
        mtl_keys = [k for k in resp["returnObj"].keys() if "mtl" in k.lower()]
        print(f"   Tables with 'mtl' in name: {mtl_keys}")
        all_keys = list(resp["returnObj"].keys())
        print(f"   All tables: {all_keys}")
except EpicorError as e:
    print(f"   Error: {e}")

# Method 3: Try BillOfMtlSearchSvc
print("\n3. BillOfMtlSearchSvc/GetFullTreeByPart:")
try:
    url = f"{c.base_url}/api/v2/odata/{c.company}/Erp.BO.BillOfMtlSearchSvc/GetFullTreeByPart"
    payload = {
        "partNum": partnum,
        "revisionNum": "X1",
    }
    resp = c._post_json_raw(url, payload)
    print(f"   Response keys: {list(resp.keys())}")
    if "returnObj" in resp:
        print(f"   returnObj type: {type(resp['returnObj'])}")
        if isinstance(resp['returnObj'], dict):
            for k, v in resp["returnObj"].items():
                if isinstance(v, list):
                    print(f"   {k}: {len(v)} records")
except EpicorError as e:
    print(f"   Error: {e}")

# Method 4: Try BOMWhereUsed
print("\n4. BOMWhereUsedSvc/GetRows:")
try:
    url = f"{c.base_url}/api/v2/odata/{c.company}/Erp.BO.BOMWhereUsedSvc/GetRows"
    payload = {
        "whereClauseBOMWhereUsed": f"PartNum = '{partnum}'",
        "pageSize": 100,
        "absolutePage": 0,
    }
    resp = c._post_json_raw(url, payload)
    print(f"   Response keys: {list(resp.keys())}")
except EpicorError as e:
    print(f"   Error: {e}")
