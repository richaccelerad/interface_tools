"""
setup_webhook.py — Manage Monday.com webhook subscriptions

Usage:
    python setup_webhook.py list
    python setup_webhook.py create <render-url> [--event create_item|change_specific_column_value]
    python setup_webhook.py delete <webhook-id>
    python setup_webhook.py test <render-url>

Examples:
    # See what webhooks are registered on the parts board
    python setup_webhook.py list

    # Register a webhook for new item creation
    python setup_webhook.py create https://epicor-monday-webhook.onrender.com

    # Register for part-number column changes instead of item creation
    python setup_webhook.py create https://epicor-monday-webhook.onrender.com --event change_specific_column_value

    # Remove a webhook
    python setup_webhook.py delete 12345678

    # Verify the server is reachable and the challenge works
    python setup_webhook.py test https://epicor-monday-webhook.onrender.com
"""

import json
import sys

import requests

import config
from monday_client import MondayClient


def get_client() -> MondayClient:
    return MondayClient(config.MONDAY_API_TOKEN)


def get_webhook_url(render_base: str) -> str:
    """Build the full webhook URL including the secret path segment."""
    secret = getattr(config, "MONDAY_WEBHOOK_SECRET", "")
    if not secret:
        print("WARNING: MONDAY_WEBHOOK_SECRET is not set in config.py.")
        print("         The webhook endpoint will accept requests from anyone.")
        print("         Set it to a random string to add URL-based security.")
        print()
        return render_base.rstrip("/") + "/webhook/no-secret"
    return render_base.rstrip("/") + "/webhook/" + secret


def cmd_list(client: MondayClient) -> None:
    """List all webhooks registered on the parts board."""
    board_id = str(config.MONDAY_PARTS_BOARD_ID)
    query = """
    query ($boardId: ID!) {
        webhooks(board_id: $boardId) {
            id
            event
            board_id
            config
        }
    }
    """
    data = client.execute_query(query, {"boardId": board_id})
    webhooks = data.get("webhooks", [])

    if not webhooks:
        print(f"No webhooks registered on board {board_id}.")
        return

    print(f"Webhooks on board {board_id}:")
    print("-" * 60)
    for wh in webhooks:
        cfg = wh.get("config") or "{}"
        try:
            cfg_parsed = json.loads(cfg)
        except Exception:
            cfg_parsed = cfg
        print(f"  ID:    {wh['id']}")
        print(f"  Event: {wh['event']}")
        if cfg_parsed and cfg_parsed != {}:
            print(f"  Config: {cfg_parsed}")
        print()


def cmd_create(client: MondayClient, render_base: str, event: str) -> None:
    """Register a new webhook on the parts board."""
    board_id    = str(config.MONDAY_PARTS_BOARD_ID)
    webhook_url = get_webhook_url(render_base)

    print(f"Registering webhook:")
    print(f"  Board:  {board_id}")
    print(f"  URL:    {webhook_url}")
    print(f"  Event:  {event}")

    # For change_specific_column_value we need a column ID.
    # Try to find the part-number column automatically.
    wh_config: str | None = None
    if event == "change_specific_column_value":
        col_id = _find_partnum_col(client, board_id)
        if not col_id:
            print("\nERROR: Could not find a part-number column on this board.")
            print("       Create a column named 'PartNum' or 'Part Number', then re-run.")
            sys.exit(1)
        wh_config = json.dumps({"columnId": col_id})
        print(f"  Column: {col_id}  (part-number column)")

    mutation = """
    mutation ($boardId: ID!, $url: String!, $event: WebhookEventType!, $config: JSON) {
        create_webhook(board_id: $boardId, url: $url, event: $event, config: $config) {
            id
            board_id
        }
    }
    """
    variables = {
        "boardId": board_id,
        "url":     webhook_url,
        "event":   event,
        "config":  wh_config,
    }
    data = client.execute_query(mutation, variables)
    wh   = data.get("create_webhook", {})
    print(f"\nWebhook created!  ID: {wh.get('id')}  board: {wh.get('board_id')}")
    print()
    print("Next steps:")
    print("  1. Make sure your Render web service is deployed and running.")
    print("  2. Add a new item to the Monday.com board — the webhook should fire")
    print("     and Epicor data will appear in the row within a few seconds.")
    print("  3. Check Render logs if nothing happens: render logs --service-name epicor-monday-webhook")


def cmd_delete(client: MondayClient, webhook_id: str) -> None:
    """Delete a webhook by ID."""
    mutation = """
    mutation ($webhookId: ID!) {
        delete_webhook(id: $webhookId) {
            id
            board_id
        }
    }
    """
    data = client.execute_query(mutation, {"webhookId": webhook_id})
    deleted = data.get("delete_webhook", {})
    print(f"Deleted webhook {deleted.get('id')} from board {deleted.get('board_id')}.")


def cmd_test(render_base: str) -> None:
    """
    Send a fake Monday.com challenge to the webhook endpoint and verify it echoes
    back correctly.  Also checks the /health endpoint.
    """
    import secrets as _secrets

    base = render_base.rstrip("/")
    secret = getattr(config, "MONDAY_WEBHOOK_SECRET", "")
    webhook_url = get_webhook_url(render_base)

    # Test /health
    print(f"Testing /health at {base}/health ...")
    try:
        r = requests.get(f"{base}/health", timeout=15)
        if r.ok and r.json().get("status") == "ok":
            print("  OK — server is reachable")
        else:
            print(f"  WARN — unexpected response: {r.status_code} {r.text[:200]}")
    except Exception as exc:
        print(f"  FAIL — {exc}")
        print("  Is the Render web service deployed and running?")
        return

    # Test challenge
    challenge_token = _secrets.token_hex(16)
    print(f"\nTesting Monday.com challenge at {webhook_url} ...")
    try:
        r = requests.post(
            webhook_url,
            json={"challenge": challenge_token},
            timeout=15,
        )
        if r.ok and r.json().get("challenge") == challenge_token:
            print("  OK — challenge echoed correctly")
        else:
            print(f"  FAIL — response: {r.status_code} {r.text[:200]}")
    except Exception as exc:
        print(f"  FAIL — {exc}")
        return

    print("\nAll tests passed. The webhook server is ready.")


def _find_partnum_col(client: MondayClient, board_id: str) -> str | None:
    """Return the column ID of the part-number column, or None."""
    board = client.get_board(board_id)
    if not board:
        return None
    for col in board.get("columns", []):
        t = col["title"].lower()
        if "partnum" in t or "part number" in t or t == "part":
            return col["id"]
    return None


def main() -> None:
    args = sys.argv[1:]

    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    cmd = args[0].lower()

    if cmd == "list":
        cmd_list(get_client())

    elif cmd == "create":
        if len(args) < 2:
            print("Usage: python setup_webhook.py create <render-url> [--event EVENT]")
            sys.exit(1)
        render_base = args[1]
        event = "create_item"
        for i, a in enumerate(args[2:], 2):
            if a == "--event" and i + 1 < len(args):
                event = args[i + 1]
        valid_events = ("create_item", "change_specific_column_value")
        if event not in valid_events:
            print(f"Unknown event {event!r}. Choose from: {valid_events}")
            sys.exit(1)
        cmd_create(get_client(), render_base, event)

    elif cmd == "delete":
        if len(args) < 2:
            print("Usage: python setup_webhook.py delete <webhook-id>")
            sys.exit(1)
        cmd_delete(get_client(), args[1])

    elif cmd == "test":
        if len(args) < 2:
            print("Usage: python setup_webhook.py test <render-url>")
            sys.exit(1)
        cmd_test(args[1])

    else:
        print(f"Unknown command: {cmd!r}")
        print("Run  python setup_webhook.py --help  for usage.")
        sys.exit(1)


if __name__ == "__main__":
    main()
