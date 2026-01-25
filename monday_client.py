"""
Monday.com API Client

A reusable client for interacting with the Monday.com GraphQL API.
Includes rate limiting with exponential backoff.
"""

import requests
import json
import time
import random
import sys
from typing import Optional


class MondayClient:
    """Client for interacting with Monday.com API with rate limiting support."""

    API_URL = "https://api.monday.com/v2"

    def __init__(self, api_token: str, max_retries: int = 5, base_delay: float = 1.0,
                 max_delay: float = 60.0, verbose: bool = True):
        """
        Initialize the Monday.com client.

        Args:
            api_token: Your Monday.com API token
            max_retries: Maximum number of retries on rate limit (default: 5)
            base_delay: Base delay in seconds for exponential backoff (default: 1.0)
            max_delay: Maximum delay in seconds between retries (default: 60.0)
            verbose: Print rate limit warnings to stderr (default: True)
        """
        self.api_token = api_token
        self.headers = {
            "Authorization": api_token,
            "Content-Type": "application/json"
        }
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.verbose = verbose

        # Rate limiting stats
        self._rate_limit_hits = 0
        self._total_requests = 0
        self._total_retries = 0

    @property
    def rate_limit_stats(self) -> dict:
        """Return rate limiting statistics."""
        return {
            "total_requests": self._total_requests,
            "rate_limit_hits": self._rate_limit_hits,
            "total_retries": self._total_retries,
        }

    def execute_query(self, query: str, variables: Optional[dict] = None) -> dict:
        """
        Execute a GraphQL query against Monday.com API with rate limiting support.

        Uses exponential backoff with jitter when rate limited (429 errors).

        Args:
            query: GraphQL query string
            variables: Optional variables for the query

        Returns:
            The 'data' portion of the API response

        Raises:
            Exception: If the API returns errors after all retries exhausted
        """
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        self._total_requests += 1
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                response = requests.post(
                    self.API_URL,
                    headers=self.headers,
                    json=payload
                )

                # Handle rate limiting (429)
                if response.status_code == 429:
                    self._rate_limit_hits += 1

                    if attempt < self.max_retries:
                        # Calculate delay with exponential backoff and jitter
                        delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                        jitter = random.uniform(0, delay * 0.5)
                        total_delay = delay + jitter

                        # Check for Retry-After header
                        retry_after = response.headers.get("Retry-After")
                        if retry_after:
                            try:
                                total_delay = max(total_delay, float(retry_after))
                            except ValueError:
                                pass

                        if self.verbose:
                            print(f"[Monday.com] Rate limited (429). "
                                  f"Retry {attempt + 1}/{self.max_retries} in {total_delay:.1f}s...",
                                  file=sys.stderr)

                        self._total_retries += 1
                        time.sleep(total_delay)
                        continue
                    else:
                        # Max retries exhausted
                        response.raise_for_status()

                # Handle other HTTP errors
                response.raise_for_status()

                result = response.json()

                if "errors" in result:
                    # Check if it's a rate limit error in the response body
                    error_str = str(result['errors'])
                    if 'rate' in error_str.lower() or 'limit' in error_str.lower():
                        self._rate_limit_hits += 1
                        if attempt < self.max_retries:
                            delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                            jitter = random.uniform(0, delay * 0.5)
                            total_delay = delay + jitter

                            if self.verbose:
                                print(f"[Monday.com] Rate limit error in response. "
                                      f"Retry {attempt + 1}/{self.max_retries} in {total_delay:.1f}s...",
                                      file=sys.stderr)

                            self._total_retries += 1
                            time.sleep(total_delay)
                            continue

                    raise Exception(f"Monday.com API error: {result['errors']}")

                return result.get("data", {})

            except requests.exceptions.HTTPError as e:
                last_exception = e
                if e.response is not None and e.response.status_code == 429:
                    # Already handled above, but just in case
                    if attempt >= self.max_retries:
                        raise
                else:
                    raise

        # Should not reach here, but just in case
        if last_exception:
            raise last_exception
        raise Exception("Monday.com API request failed after all retries")

    # -------------------------------------------------------------------------
    # Board Operations
    # -------------------------------------------------------------------------

    def get_boards(self) -> list[dict]:
        """Get all boards accessible to the user."""
        query = """
        query {
            boards {
                id
                name
            }
        }
        """
        result = self.execute_query(query)
        return result.get("boards", [])

    def get_board(self, board_id: str) -> Optional[dict]:
        """Get a specific board by ID."""
        query = """
        query ($boardId: [ID!]!) {
            boards(ids: $boardId) {
                id
                name
                columns {
                    id
                    title
                    type
                }
            }
        }
        """
        result = self.execute_query(query, {"boardId": [board_id]})
        boards = result.get("boards", [])
        return boards[0] if boards else None

    def create_board(self, name: str, board_kind: str = "public") -> str:
        """
        Create a new board.

        Args:
            name: Board name
            board_kind: 'public' or 'private'

        Returns:
            The new board's ID
        """
        query = """
        mutation ($name: String!, $kind: BoardKind!) {
            create_board(board_name: $name, board_kind: $kind) {
                id
            }
        }
        """
        variables = {"name": name, "kind": board_kind}
        result = self.execute_query(query, variables)
        return result["create_board"]["id"]

    # -------------------------------------------------------------------------
    # Column Operations
    # -------------------------------------------------------------------------

    def get_board_columns(self, board_id: str) -> list[dict]:
        """Get columns for a specific board."""
        query = """
        query ($boardId: [ID!]!) {
            boards(ids: $boardId) {
                columns {
                    id
                    title
                    type
                }
            }
        }
        """
        result = self.execute_query(query, {"boardId": [board_id]})
        boards = result.get("boards", [])
        return boards[0].get("columns", []) if boards else []

    def create_column(self, board_id: str, title: str, column_type: str) -> str:
        """
        Create a new column on a board.

        Args:
            board_id: The board ID
            title: Column title
            column_type: Column type (text, long_text, date, numbers, status, etc.)

        Returns:
            The new column's ID
        """
        query = """
        mutation ($boardId: ID!, $title: String!, $columnType: ColumnType!) {
            create_column(board_id: $boardId, title: $title, column_type: $columnType) {
                id
            }
        }
        """
        variables = {
            "boardId": board_id,
            "title": title,
            "columnType": column_type
        }
        result = self.execute_query(query, variables)
        return result["create_column"]["id"]

    # -------------------------------------------------------------------------
    # Item Operations
    # -------------------------------------------------------------------------

    def get_items(self, board_id: str, limit: int = 500) -> list[dict]:
        """Get all items from a board."""
        query = """
        query ($boardId: [ID!]!, $limit: Int!) {
            boards(ids: $boardId) {
                items_page(limit: $limit) {
                    items {
                        id
                        name
                        column_values {
                            id
                            text
                            value
                        }
                    }
                }
            }
        }
        """
        result = self.execute_query(query, {"boardId": [board_id], "limit": limit})
        boards = result.get("boards", [])
        if not boards:
            return []
        return boards[0].get("items_page", {}).get("items", [])

    def get_item(self, item_id: str) -> Optional[dict]:
        """Get a specific item by ID."""
        query = """
        query ($itemId: [ID!]!) {
            items(ids: $itemId) {
                id
                name
                column_values {
                    id
                    text
                    value
                }
            }
        }
        """
        result = self.execute_query(query, {"itemId": [item_id]})
        items = result.get("items", [])
        return items[0] if items else None

    def create_item(self, board_id: str, item_name: str, column_values: Optional[dict] = None) -> str:
        """
        Create a new item on a board.

        Args:
            board_id: The board ID
            item_name: Name for the new item
            column_values: Optional dict of column_id -> value

        Returns:
            The new item's ID
        """
        query = """
        mutation ($boardId: ID!, $itemName: String!, $columnValues: JSON!) {
            create_item(board_id: $boardId, item_name: $itemName, column_values: $columnValues) {
                id
            }
        }
        """
        variables = {
            "boardId": board_id,
            "itemName": item_name,
            "columnValues": json.dumps(column_values or {})
        }
        result = self.execute_query(query, variables)
        return result["create_item"]["id"]

    def update_item(self, board_id: str, item_id: str, column_values: dict) -> str:
        """
        Update an existing item's column values.

        Args:
            board_id: The board ID
            item_id: The item ID to update
            column_values: Dict of column_id -> value

        Returns:
            The updated item's ID
        """
        query = """
        mutation ($boardId: ID!, $itemId: ID!, $columnValues: JSON!) {
            change_multiple_column_values(board_id: $boardId, item_id: $itemId, column_values: $columnValues) {
                id
            }
        }
        """
        variables = {
            "boardId": board_id,
            "itemId": item_id,
            "columnValues": json.dumps(column_values)
        }
        result = self.execute_query(query, variables)
        return result["change_multiple_column_values"]["id"]

    def delete_item(self, item_id: str) -> str:
        """
        Delete an item.

        Args:
            item_id: The item ID to delete

        Returns:
            The deleted item's ID
        """
        query = """
        mutation ($itemId: ID!) {
            delete_item(item_id: $itemId) {
                id
            }
        }
        """
        result = self.execute_query(query, {"itemId": item_id})
        return result["delete_item"]["id"]

    # -------------------------------------------------------------------------
    # Group Operations
    # -------------------------------------------------------------------------

    def get_groups(self, board_id: str) -> list[dict]:
        """Get all groups from a board."""
        query = """
        query ($boardId: [ID!]!) {
            boards(ids: $boardId) {
                groups {
                    id
                    title
                }
            }
        }
        """
        result = self.execute_query(query, {"boardId": [board_id]})
        boards = result.get("boards", [])
        return boards[0].get("groups", []) if boards else []

    def create_group(self, board_id: str, group_name: str) -> str:
        """
        Create a new group on a board.

        Args:
            board_id: The board ID
            group_name: Name for the new group

        Returns:
            The new group's ID
        """
        query = """
        mutation ($boardId: ID!, $groupName: String!) {
            create_group(board_id: $boardId, group_name: $groupName) {
                id
            }
        }
        """
        result = self.execute_query(query, {"boardId": board_id, "groupName": group_name})
        return result["create_group"]["id"]


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------

def run_tests(api_token: str, board_id: str):
    """
    Run tests to verify MondayClient functionality.

    Args:
        api_token: Monday.com API token
        board_id: Board ID to use for testing
    """
    print("Monday.com Client Tests")
    print("=" * 50)

    client = MondayClient(api_token)
    errors = []

    # Test 1: Get boards
    print("\n[Test 1] Get boards...")
    try:
        boards = client.get_boards()
        assert isinstance(boards, list), "Expected list of boards"
        assert len(boards) > 0, "Expected at least one board"
        print(f"  PASS - Found {len(boards)} boards")
    except Exception as e:
        errors.append(f"Get boards: {e}")
        print(f"  FAIL - {e}")

    # Test 2: Get board columns
    print("\n[Test 2] Get board columns...")
    try:
        columns = client.get_board_columns(board_id)
        assert isinstance(columns, list), "Expected list of columns"
        print(f"  PASS - Found {len(columns)} columns")
    except Exception as e:
        errors.append(f"Get board columns: {e}")
        print(f"  FAIL - {e}")

    # Test 3: Create, read, update, delete item
    print("\n[Test 3] CRUD operations on item...")
    item_id = None
    try:
        # Create
        item_id = client.create_item(board_id, "Test Item - MondayClient")
        assert item_id is not None, "Expected item ID"
        print(f"  CREATE - Item ID: {item_id}")

        # Read
        item = client.get_item(item_id)
        assert item is not None, "Expected item data"
        assert item["name"] == "Test Item - MondayClient", "Item name mismatch"
        print(f"  READ - Name: {item['name']}")

        # Update (find a text column to update)
        columns = client.get_board_columns(board_id)
        text_col = next((c for c in columns if c["type"] == "text"), None)
        if text_col:
            client.update_item(board_id, item_id, {text_col["id"]: "Updated Value"})
            updated_item = client.get_item(item_id)
            updated_value = next(
                (cv["text"] for cv in updated_item["column_values"] if cv["id"] == text_col["id"]),
                None
            )
            assert updated_value == "Updated Value", f"Update failed, got: {updated_value}"
            print(f"  UPDATE - Set {text_col['title']} to 'Updated Value'")
        else:
            print("  UPDATE - Skipped (no text column found)")

        # Delete
        deleted_id = client.delete_item(item_id)
        assert deleted_id == item_id, "Delete should return the item ID"
        print(f"  DELETE - Item {item_id} deleted")
        item_id = None

        print("  PASS - All CRUD operations successful")
    except Exception as e:
        errors.append(f"CRUD operations: {e}")
        print(f"  FAIL - {e}")
        # Cleanup if item was created but test failed
        if item_id:
            try:
                client.delete_item(item_id)
                print("  (Cleaned up test item)")
            except:
                pass

    # Test 4: Create column
    print("\n[Test 4] Create column...")
    try:
        import time
        col_title = f"Test Column {int(time.time())}"
        col_id = client.create_column(board_id, col_title, "text")
        assert col_id is not None, "Expected column ID"
        print(f"  PASS - Created column '{col_title}' (ID: {col_id})")
    except Exception as e:
        errors.append(f"Create column: {e}")
        print(f"  FAIL - {e}")

    # Test 5: Get items from board
    print("\n[Test 5] Get items from board...")
    try:
        items = client.get_items(board_id, limit=10)
        assert isinstance(items, list), "Expected list of items"
        print(f"  PASS - Found {len(items)} items (limit 10)")
    except Exception as e:
        errors.append(f"Get items: {e}")
        print(f"  FAIL - {e}")

    # Test 6: Custom query
    print("\n[Test 6] Custom query (get current user)...")
    try:
        result = client.execute_query("""
            query {
                me {
                    name
                    email
                }
            }
        """)
        assert "me" in result, "Expected 'me' in result"
        print(f"  PASS - User: {result['me']['name']}")
    except Exception as e:
        errors.append(f"Custom query: {e}")
        print(f"  FAIL - {e}")

    # Summary
    print("\n" + "=" * 50)
    if errors:
        print(f"TESTS COMPLETED WITH {len(errors)} ERROR(S):")
        for err in errors:
            print(f"  - {err}")
        return False
    else:
        print("ALL TESTS PASSED!")
        return True


if __name__ == "__main__":
    import sys
    import config

    if len(sys.argv) >= 3:
        api_token = sys.argv[1]
        board_id = sys.argv[2]
    else:
        api_token = config.MONDAY_API_TOKEN
        board_id = config.MONDAY_BOARD_ID

    success = run_tests(api_token, board_id)
    sys.exit(0 if success else 1)
