from epicor_po_x2 import EpicorClient

client = EpicorClient(
    base_url="https://ausmtsapp01.epicorsaas.com/saas205",
    company="19593",
    plant="MfgSys",
    api_key = "1UYtFXnhV0Z8afafsUUFfAoeMx5XKlfaEJIVrI1pDG8Lr",
    username = "19593RKowalczyk",
    password = "dirtRockAlbum33",
    learn_missing_getrows_params=True,   # fallback on future Epicor changes
)

rows = client.get_po_lines_by_partnum("100100")

# Consume results cleanly
open_pos = [r for r in rows if r.status == "open"]
closed_pos = [r for r in rows if r.status == "closed"]

print(len(rows), len(open_pos), len(closed_pos))
for each in rows:
    print(each.to_dict())
