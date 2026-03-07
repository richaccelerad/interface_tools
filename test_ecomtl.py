"""Test ECOMtl access with different queries."""
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

print("Testing ECOMtl queries...")
print()

# Test 1: Query for specific part
print("1. ECOMtls for PartNum eq '102892':")
try:
    records = c._get_odata("Erp.BO.EngWorkBenchSvc/ECOMtls", "PartNum eq '102892'", None)
    print(f"   Found {len(records)} records")
    if records:
        print(f"   First record: {records[0]}")
except EpicorError as e:
    print(f"   Error: {e}")

# Test 2: Query for a subassembly part from the old output
print()
print("2. ECOMtls for PartNum eq '102890' (subassembly from old output):")
try:
    records = c._get_odata("Erp.BO.EngWorkBenchSvc/ECOMtls", "PartNum eq '102890'", None)
    print(f"   Found {len(records)} records")
except EpicorError as e:
    print(f"   Error: {e}")

# Test 3: Get ANY ECOMtl records (limit to 5)
print()
print("3. First 5 ECOMtl records (any part):")
try:
    records = c._get_odata("Erp.BO.EngWorkBenchSvc/ECOMtls", "Company eq '19593'", ["PartNum", "MtlPartNum", "MtlSeq"])
    print(f"   Found {len(records)} total records")
    for r in records[:5]:
        print(f"   - Parent: {r.get('PartNum')}, Component: {r.get('MtlPartNum')}")
except EpicorError as e:
    print(f"   Error: {e}")

# Test 4: Try EngWorkBenchSvc/GetList method (POST)
print()
print("4. Trying EngWorkBenchSvc/GetList (POST method):")
try:
    url = f"{c.base_url}/api/v2/odata/{c.company}/Erp.BO.EngWorkBenchSvc/GetList"
    payload = {
        "whereClause": f"PartNum = '102892'",
        "pageSize": 100,
        "absolutePage": 0,
    }
    resp = c._post_json_raw(url, payload)
    print(f"   Response keys: {list(resp.keys())}")
    if "returnObj" in resp:
        print(f"   returnObj keys: {list(resp['returnObj'].keys())}")
except EpicorError as e:
    print(f"   Error: {e}")
