# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python tool whose purpose is to integrate information from epicor into monday.com.
Specifically, it looks at a specified table, gathers all the partNum's, and finds the POs associated with those parts, and adds the data
to the monday.com table
As such, it serves as an integration toolkit for enterprise systems, providing API clients for:
- **Epicor ERP**: REST client for querying Purchase Order data via POSvc.GetRows
- **Monday.com**: GraphQL client for board/item/group management

This code is intended to be deployed on Render.com, to run regularly

## Running the Code

```bash
# Run main program
python main.py

# Run Epicor PO lookup example
python epicor_po_test_x7.py

# Run Monday.com client tests
python monday_client.py
python monday_client.py <api_token> <board_id>
```

No formal test framework is used. Tests are inline functions that print pass/fail results.

## Dependencies

- `requests` (external)
- Standard library: `json`, `dataclasses`, `typing`, `re`

Install dependencies: `pip install -r requirements.txt`

## Architecture

### Epicor Client (`epicor_po_x2.py`)

**Key classes:**
- `EpicorClient`: Stateful REST client with session-based auth
- `POLineMatch`: Frozen dataclass representing normalized PO line data
- `EpicorError`: Custom exception for API errors

**Parameter auto-learning**: The client can automatically discover and add missing `whereClause*` parameters when Epicor's API demands them. The regex `_MISSING_PARAM_RE` extracts parameter names from error messages. Known required parameters are in `DEFAULT_GETROWS_EXTRAS`.

**Status determination logic** (in `get_po_lines_by_partnum`):
1. `void_line=True` → "void"
2. Any of `po_open`, `line_open`, or `any_open_release` is True → "open"
3. All indicators are False → "closed"
4. No indicators available → "unknown"

**Data aggregation**: Queries POHeader, PODetail, and PORel tables, then joins them in memory by PONum/POLine to compute complete status and received quantities.

**IMPORTANT - Read-only access**: This program must NEVER update, create, or delete data in Epicor. It should only pull/query data. POST requests with JSON payloads are acceptable for querying (e.g., GetRows), but never use endpoints that modify Epicor data.

### Monday.com Client (`monday_client.py`)

**Key class:**
- `MondayClient`: GraphQL wrapper with methods organized by domain:
  - Board operations: `get_boards()`, `get_board()`, `create_board()`
  - Column operations: `get_board_columns()`, `create_column()`
  - Item operations: `get_items()`, `get_item()`, `create_item()`, `update_item()`, `delete_item()`
  - Group operations: `get_groups()`, `create_group()`

**Core method**: `execute_query(query, variables)` handles all GraphQL requests and error extraction.

## File Organization

- Root files (`*_x2.py`, `monday_client.py`): Current production versions
- `basic_backup/`: Backup copies of current versions
- `old/`: Historical versions showing development progression (x1 through x6)
- Version suffix pattern: `_x<N>` indicates iteration number

## Credential Handling

Credentials are stored in `config.py` (excluded from git via `.gitignore`). Copy `config.example.py` to `config.py` and fill in your values:

- `EPICOR_BASE_URL`, `EPICOR_COMPANY`, `EPICOR_PLANT`, `EPICOR_API_KEY`, `EPICOR_USERNAME`, `EPICOR_PASSWORD`
- `MONDAY_API_TOKEN`, `MONDAY_BOARD_ID`
- `MONDAY_PARTS_BOARD_ID` - The board containing PartNum values to look up in Epicor
