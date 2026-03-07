"""Test EngWorkBench methods for unapproved revision BOM."""
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
revision = "X1"
print(f"Testing EngWorkBench for {partnum} rev {revision}")
print("=" * 60)

# Method 1: GetDatasetForTree
print("\n1. EngWorkBenchSvc/GetDatasetForTree:")
try:
    url = f"{c.base_url}/api/v2/odata/{c.company}/Erp.BO.EngWorkBenchSvc/GetDatasetForTree"
    payload = {
        "ipPartNum": partnum,
        "ipRevisionNum": revision,
        "ipAltMethod": "",
        "ipAsOfDate": "2026-01-28",
        "ipCompleteTree": True,
        "ipReturn": True,
        "ipGetDatasetForTree": True,
        "ipUseMethodForParts": True,
    }
    resp = c._post_json_raw(url, payload)
    print(f"   Response keys: {list(resp.keys())}")
    if "returnObj" in resp:
        print(f"   returnObj keys: {list(resp['returnObj'].keys())}")
        for k, v in resp["returnObj"].items():
            if isinstance(v, list) and len(v) > 0:
                print(f"   {k}: {len(v)} records")
                if "Mtl" in k and v:
                    print(f"      First: PartNum={v[0].get('PartNum')}, MtlPartNum={v[0].get('MtlPartNum')}")
except EpicorError as e:
    print(f"   Error: {e}")

# Method 2: GetECOMtl
print("\n2. EngWorkBenchSvc/GetECOMtl:")
try:
    url = f"{c.base_url}/api/v2/odata/{c.company}/Erp.BO.EngWorkBenchSvc/GetECOMtl"
    payload = {
        "partNum": partnum,
        "revisionNum": revision,
        "altMethod": "",
    }
    resp = c._post_json_raw(url, payload)
    print(f"   Response: {resp}")
except EpicorError as e:
    print(f"   Error: {e}")

# Method 3: GetByID on EngWorkBench
print("\n3. EngWorkBenchSvc/GetByID:")
try:
    url = f"{c.base_url}/api/v2/odata/{c.company}/Erp.BO.EngWorkBenchSvc/GetByID"
    payload = {
        "partNum": partnum,
        "revisionNum": revision,
        "altMethod": "",
        "processMfgID": "",
    }
    resp = c._post_json_raw(url, payload)
    print(f"   Response keys: {list(resp.keys())}")
    if "returnObj" in resp:
        print(f"   returnObj keys: {list(resp['returnObj'].keys())}")
        for k, v in resp["returnObj"].items():
            if isinstance(v, list) and len(v) > 0:
                print(f"   {k}: {len(v)} records")
                if "Mtl" in k and v:
                    for rec in v[:3]:
                        print(f"      MtlPartNum: {rec.get('MtlPartNum')}, MtlSeq: {rec.get('MtlSeq')}")
except EpicorError as e:
    print(f"   Error: {e}")
