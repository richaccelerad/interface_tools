from epicor_po_x2 import EpicorClient
import config

client = EpicorClient(
    base_url=config.EPICOR_BASE_URL,
    company=config.EPICOR_COMPANY,
    plant=config.EPICOR_PLANT,
    api_key=config.EPICOR_API_KEY,
    username=config.EPICOR_USERNAME,
    password=config.EPICOR_PASSWORD,
    learn_missing_getrows_params=True,   # fallback on future Epicor changes
)

rows = client.get_po_lines_by_partnum("100100")

# Consume results cleanly
open_pos = [r for r in rows if r.status == "open"]
closed_pos = [r for r in rows if r.status == "closed"]

print(len(rows), len(open_pos), len(closed_pos))
for each in rows:
    print(each.to_dict())
