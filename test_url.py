"""Test script to show URLs without making requests."""
import config

print("=== Config Values ===")
print(f"EPICOR_BASE_URL: {config.EPICOR_BASE_URL}")
print(f"EPICOR_COMPANY: {config.EPICOR_COMPANY}")
print(f"EPICOR_API_KEY: {config.EPICOR_API_KEY[:20]}... (truncated)")
print(f"EPICOR_PLANT: {config.EPICOR_PLANT}")
print()

base_url = config.EPICOR_BASE_URL.rstrip("/")
company = config.EPICOR_COMPANY
api_key = config.EPICOR_API_KEY

print("=== OLD Method (OData GET - was failing with 401) ===")
old_url = f"{base_url}/api/v2/odata/{company}/Erp.BO.PartSvc/PartMtls"
print(f"Full: {old_url}?api-key={api_key[:10]}...&$filter=...")
print(f"Method: GET")
print(f"Problem: Direct OData entity access may not be permitted")
print()

print("=== NEW Method (GetRows POST - matches working PO lookup) ===")
new_url = f"{base_url}/api/v2/odata/{company}/Erp.BO.PartSvc/GetRows"
print(f"Full: {new_url}?api-key={api_key[:10]}...")
print(f"Method: POST with JSON body")
print(f"API key starts with: {api_key[:20]}")
print(f"API key is NOT 'MfgSys': {api_key != 'MfgSys'}")
print()

print("=== Verification ===")
print(f"API key length: {len(api_key)} chars (should be ~40+, not 6)")
print(f"Plant value: '{config.EPICOR_PLANT}' (this is NOT the API key)")
