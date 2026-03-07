"""Debug script to see raw BOM data from Epicor."""
from epicor_po_x2 import EpicorClient
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

print(f"Querying BOM for part: {partnum}")
print("=" * 60)

# Try PartMtl without revision filter
print("\n1. PartMtl (no revision filter):")
try:
    records = c._get_odata(
        "Erp.BO.PartSvc/PartMtls",
        f"PartNum eq '{partnum}'",
        None
    )
    print(f"   Found {len(records)} records")
    if records:
        print(f"   First record keys: {list(records[0].keys())}")
        for r in records[:5]:
            print(f"   - MtlPartNum: {r.get('MtlPartNum')}, Rev: {r.get('RevisionNum')}")
except Exception as e:
    print(f"   Error: {e}")

# Try ECOMtl without revision filter
print("\n2. ECOMtl (no revision filter):")
try:
    records = c._get_odata(
        "Erp.BO.EngWorkBenchSvc/ECOMtls",
        f"PartNum eq '{partnum}'",
        None
    )
    print(f"   Found {len(records)} records")
    if records:
        for r in records[:5]:
            print(f"   - MtlPartNum: {r.get('MtlPartNum')}, Rev: {r.get('RevisionNum')}")
except Exception as e:
    print(f"   Error: {e}")

# Check what revisions exist
print("\n3. Available revisions (PartRev):")
try:
    records = c._get_odata(
        "Erp.BO.PartSvc/PartRevs",
        f"PartNum eq '{partnum}'",
        ["PartNum", "RevisionNum", "Approved", "EffectiveDate"]
    )
    print(f"   Found {len(records)} revisions")
    for r in records:
        print(f"   - Rev: {r.get('RevisionNum')}, Approved: {r.get('Approved')}, Effective: {r.get('EffectiveDate')}")
except Exception as e:
    print(f"   Error: {e}")

print("\n" + "=" * 60)
print("Done")
