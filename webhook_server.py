"""
webhook_server.py — FastAPI webhook listener for Monday.com → Epicor sync

Monday.com fires a POST to this server whenever an item is created (or the
part-number column changes).  The server echoes Monday's challenge, then
enriches the item with Epicor data in a background task so the 200 response
is always returned within Monday's timeout window.

Start locally:
    uvicorn webhook_server:app --reload

On Render.com (set as the start command):
    uvicorn webhook_server:app --host 0.0.0.0 --port $PORT

Webhook URL to register in Monday.com:
    https://<your-app>.onrender.com/webhook/<MONDAY_WEBHOOK_SECRET>
"""

from __future__ import annotations

import logging
import os
import sys

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import JSONResponse

import config
from main import get_column_ids, get_epicor_client, get_monday_client, process_item
from monday_client import MondayClient

# ---------------------------------------------------------------------------
# Logging — goes to stdout so Render captures it in the log stream
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level caches (warm across requests within the same process)
# ---------------------------------------------------------------------------

# board_id -> part-number column id (or None if item name is used)
_partnum_col_cache: dict[str, str | None] = {}

# board_id -> column_ids dict (mapping key -> Monday column id)
_column_ids_cache: dict[str, dict[str, str]] = {}


def _get_webhook_secret() -> str:
    """Read the webhook secret from env var or config."""
    return os.environ.get("MONDAY_WEBHOOK_SECRET", "") or getattr(config, "MONDAY_WEBHOOK_SECRET", "")


def _get_partnum_col_id(monday: MondayClient, board_id: str) -> str | None:
    """Return (and cache) the Monday column ID used for part numbers on this board."""
    if board_id not in _partnum_col_cache:
        board = monday.get_board(board_id)
        col_id: str | None = None
        if board:
            for col in board.get("columns", []):
                t = col["title"].lower()
                if "partnum" in t or "part number" in t or t == "part":
                    col_id = col["id"]
                    break
        _partnum_col_cache[board_id] = col_id
        log.info(f"Part-number column for board {board_id}: {col_id!r} "
                 f"({'found' if col_id else 'using item name as fallback'})")
    return _partnum_col_cache[board_id]


def _get_column_ids(monday: MondayClient, board_id: str) -> dict[str, str]:
    """Return (and cache) the PO-data column IDs for this board."""
    if board_id not in _column_ids_cache:
        _column_ids_cache[board_id] = get_column_ids(monday, board_id)
    return _column_ids_cache[board_id]


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Epicor → Monday.com Webhook")


@app.get("/health")
def health():
    """Health check — Render uses this to verify the service is up."""
    return {"status": "ok"}


@app.post("/webhook/{secret}")
async def monday_webhook(
    secret: str,
    request: Request,
    background_tasks: BackgroundTasks,
):
    """
    Receive Monday.com webhook events.

    The URL path secret is compared to MONDAY_WEBHOOK_SECRET so only
    Monday.com (which knows the full URL) can trigger enrichment.
    """
    # --- Verify URL secret ---
    expected = _get_webhook_secret()
    if expected and secret != expected:
        log.warning("Webhook called with wrong secret — ignoring")
        return JSONResponse({"error": "forbidden"}, status_code=403)

    body = await request.json()

    # --- Monday.com sends a challenge the first time a webhook is registered ---
    if "challenge" in body:
        log.info("Monday.com challenge received — echoing back")
        return JSONResponse({"challenge": body["challenge"]})

    event      = body.get("event", {})
    event_type = event.get("type", "")
    board_id   = str(event.get("boardId", ""))
    item_id    = str(event.get("itemId", ""))

    log.info(f"Event: type={event_type!r}  board={board_id}  item={item_id}")

    if not item_id or not board_id:
        return JSONResponse({"status": "ignored", "reason": "missing ids"})

    # Only act on the configured parts board
    if board_id != str(config.MONDAY_PARTS_BOARD_ID):
        log.debug(f"Ignoring event for board {board_id} (not the parts board)")
        return JSONResponse({"status": "ignored", "reason": "wrong board"})

    # Enrich on item creation or when the part-number column changes.
    # We deliberately do NOT subscribe to change_column_value broadly —
    # that would re-trigger on every column our own code writes, causing loops.
    if event_type in ("create_item", "change_specific_column_value"):
        background_tasks.add_task(_enrich_item, board_id, item_id)
        return JSONResponse({"status": "accepted"})

    return JSONResponse({"status": "ignored", "reason": f"unhandled event: {event_type}"})


# ---------------------------------------------------------------------------
# Background enrichment task
# ---------------------------------------------------------------------------

def _enrich_item(board_id: str, item_id: str) -> None:
    """
    Fetch part info from Epicor and write it back to the Monday.com item.

    Runs in a background thread so the webhook handler returns immediately.
    """
    try:
        monday     = get_monday_client()
        epicor     = get_epicor_client()
        column_ids = _get_column_ids(monday, board_id)

        if not column_ids:
            log.warning(
                f"Board {board_id} has no recognisable PO columns. "
                f"Run  python main.py --configure  to create them first."
            )
            return

        # Fetch the current item from Monday.com
        item_data = monday.get_item(item_id)
        if not item_data:
            log.warning(f"Item {item_id} not found on board {board_id}")
            return

        # Determine part number
        partnum_col = _get_partnum_col_id(monday, board_id)
        partnum: str | None = None

        if partnum_col:
            for cv in item_data.get("column_values", []):
                if cv["id"] == partnum_col and cv.get("text"):
                    partnum = cv["text"].strip().upper()
                    break

        if not partnum:
            # Fall back to the item name (most common pattern)
            partnum = item_data.get("name", "").strip().upper() or None

        if not partnum:
            log.info(f"Item {item_id} has no part number — skipping")
            return

        existing_values = {
            cv["id"]: cv.get("text", "") or ""
            for cv in item_data.get("column_values", [])
        }

        item = {
            "item_id":        item_id,
            "item_name":      item_data.get("name", ""),
            "partnum":        partnum,
            "existing_values": existing_values,
        }

        log.info(f"Enriching item {item_id} → part {partnum!r}")
        result = process_item(epicor, monday, board_id, column_ids, item)

        if result.get("error"):
            log.error(f"Epicor lookup failed for {partnum!r}: {result['error']}")
        elif result.get("updated"):
            log.info(f"Updated Monday.com item {item_id} ({partnum!r}) successfully")
        else:
            log.info(f"Item {item_id} ({partnum!r}) — no changes needed")

    except Exception:
        log.exception(f"Unhandled error while enriching item {item_id}")
