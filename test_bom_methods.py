"""Test additional BOM access methods."""
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
print(f"Testing BOM methods for part: {partnum}")
print("=" * 60)

# Method 1: Try Erp.BO.PartRevSearchSvc
print("\n1. PartRevSearchSvc/GetRows:")
try:
    url = f"{c.base_url}/api/v2/odata/{c.company}/Erp.BO.PartRevSearchSvc/GetRows"
    payload = {
        "whereClausePartRev": f"PartNum = '{partnum}'",
        "pageSize": 100,
        "absolutePage": 0,
    }
    resp = c._post_json_raw(url, payload)
    print(f"   Response keys: {list(resp.keys())}")
    if "returnObj" in resp:
        for k, v in resp["returnObj"].items():
            if isinstance(v, list) and len(v) > 0:
                print(f"   {k}: {len(v)} records")
                if v:
                    print(f"      Sample: {v[0]}")
except EpicorError as e:
    print(f"   Error: {e}")

# Method 2: Check what methods are available on EngWorkBenchSvc
print("\n2. EngWorkBenchSvc/GetBOMList:")
try:
    url = f"{c.base_url}/api/v2/odata/{c.company}/Erp.BO.EngWorkBenchSvc/GetMtlList"
    payload = {
        "partNum": partnum,
        "revisionNum": "",
    }
    resp = c._post_json_raw(url, payload)
    print(f"   Response: {resp}")
except EpicorError as e:
    print(f"   Error: {e}")

# Method 3: Try using REST API instead of OData
print("\n3. REST API call to PartMtl:")
try:
    # Try REST endpoint format
    url = f"{c.base_url}/api/v1/Erp.BO.PartSvc/PartMtls"
    import requests
    r = requests.get(
        url,
        params={"$filter": f"PartNum eq '{partnum}'", "api-key": c.api_key},
        auth=(config.EPICOR_USERNAME, config.EPICOR_PASSWORD),
        headers={"Accept": "application/json"},
        timeout=30
    )
    print(f"   Status: {r.status_code}")
    if r.ok:
        data = r.json()
        print(f"   Response: {data}")
    else:
        print(f"   Error: {r.text[:200]}")
except Exception as e:
    print(f"   Error: {e}")

# Method 4: Try v2 REST format
print("\n4. REST v2 PartMtl:")
try:
    url = f"{c.base_url}/api/v2/Erp.BO.PartSvc/PartMtls"
    import requests
    r = requests.get(
        url,
        params={"$filter": f"PartNum eq '{partnum}'", "api-key": c.api_key},
        auth=(config.EPICOR_USERNAME, config.EPICOR_PASSWORD),
        headers={"Accept": "application/json"},
        timeout=30
    )
    print(f"   Status: {r.status_code}")
    if r.ok:
        data = r.json()
        print(f"   Records: {len(data.get('value', []))}")
    else:
        print(f"   Error: {r.text[:200]}")
except Exception as e:
    print(f"   Error: {e}")

# Method 5: Check what the working part 102890 looks like in PartSvc/GetByID
print("\n5. PartSvc/GetByID for working part 102890:")
try:
    url = f"{c.base_url}/api/v2/odata/{c.company}/Erp.BO.PartSvc/GetByID"
    payload = {"partNum": "102890"}
    resp = c._post_json_raw(url, payload)
    if "returnObj" in resp:
        keys_with_data = [k for k, v in resp["returnObj"].items() if isinstance(v, list) and len(v) > 0]
        print(f"   Tables with data: {keys_with_data}")
except EpicorError as e:
    print(f"   Error: {e}")
