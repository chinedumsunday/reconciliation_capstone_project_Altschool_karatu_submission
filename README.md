# QuickCart Data Reconciliation — AltSchool Capstone

## Overview

This project resolves a P0 data integrity incident at QuickCart, a fast-growing e-commerce startup. The Marketing dashboard's "Total Sales" figure did not match the bank settlement statement, blocking Finance from closing the month. The goal is to establish a single, bank-reconcilable source of truth.

---

## Project Structure

```
quickcart/
├── quickcart_data/
│   ├── raw_data.jsonl              # Raw, messy transaction logs
│   ├── seed_orders.sql             # Orders seed data
│   ├── seed_payments.sql           # Payments seed data
│   ├── seed_bank_settlements.sql   # Bank settlement seed data
│   └── cleaned/
│       └── cleaned_transactions.csv  # Output of clean_transactions.py
├── logs/
│   └── clean_transactions.log      # Pipeline execution logs
├── schema.sql                      # Database schema definitions
├── clean_transactions.py           # Part A: Python data cleaning script
├── reconciliation.sql              # Part B: SQL reconciliation queries
└── README.md
```

---

## Deliverables

### Part A — `clean_transactions.py`

A standalone Python script that:

- Reads `raw_data.jsonl` and parses nested JSON fields
- Extracts relevant fields: `order_id`, `payment_id`, `customer_email`, `amount`, `currency`, `flags`, `payment_status`, etc.
- Normalises all currency formats into a single `amount_usd` float column:
  - `"$10.00"` → `10.00`
  - `"10.00"` → `10.00`
  - `1000` (cents integer) → `10.00`
- Filters out:
  - `heartbeat` events (non-transactional)
  - Records missing `order_id` or `payment_id`
  - Transactions flagged as `test` or `sandbox`
  - Records with null, zero, or negative amounts
  - `FAILED` or `PENDING` payment statuses
  - Duplicate rows
- Archives the raw JSON logs to **MongoDB** for auditing
- Exports the cleaned dataset to `quickcart_data/cleaned/cleaned_transactions.csv`

### Part B — `reconciliation.sql`

A SQL script that produces a finance-grade reconciliation report using CTEs:

| CTE | Purpose |
|---|---|
| `successful_payments` | Joins orders and payments; filters for `SUCCESS` status, non-test orders |
| `deduplicated_payments` | Uses `ROW_NUMBER()` to keep the latest successful attempt per order |
| `internal_clean` | Converts `amount_cents` → `amount_usd`; the internal source of truth |
| `bank_clean` / `bank_final` | Cleans and deduplicates the bank settlement records |
| `reconciliation` | Matches internal records to bank records via `COALESCE(payment_id, provider_ref)` |
| `orphan_bank_payments` | Bank settlements with no matching internal record |

**Final output columns:**

| Column | Description |
|---|---|
| `total_orders` | Count of deduplicated successful internal orders |
| `total_internal_sales_usd` | Sum of internal revenue (USD) |
| `total_bank_settled_usd` | Sum of bank-settled amounts (USD) |
| `orphan_count` | Settlements with no matching internal payment |
| `orphan_total_usd` | Value of orphan settlements |
| `discrepancy_gap_usd` | Internal sales − Bank settled (the reconciliation gap) |

---

## Setup & Usage

### 1. Generate Data

```bash
python generate_quickcart_data.py --outdir quickcart_data
```

### 2. Load into PostgreSQL

```bash
psql "$DATABASE_URL" -f schema.sql
psql "$DATABASE_URL" -f quickcart_data/seed_orders.sql
psql "$DATABASE_URL" -f quickcart_data/seed_payments.sql
psql "$DATABASE_URL" -f quickcart_data/seed_bank_settlements.sql
```

### 3. Run Python Cleaning Pipeline

```bash
pip install pandas pymongo python-dotenv
python clean_transactions.py
```

Set your MongoDB URI in a `.env` file:

```
mongo_uri=mongodb+srv://<user>:<password>@<cluster>.mongodb.net/
```

### 4. Run SQL Reconciliation

```bash
psql "$DATABASE_URL" -f reconciliation.sql
```

---

## Key Design Decisions

**Currency normalisation:** The `is_cent()` helper detects integer-only values (no `$`, `.`, or `USD`) and divides them by 100. All other formats are stripped of non-numeric characters and cast to float.

**Deduplication strategy:** `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY attempt_no DESC, attempted_at DESC)` ensures the most recent successful payment attempt is used per order — not the first.

**Matching logic:** `COALESCE(payment_id, provider_ref)` is used to join internal and bank records, handling cases where either field may be null.

**Orphan detection:** `NOT EXISTS` is used rather than a `LEFT JOIN IS NULL` pattern for clarity and auditability in a finance context.

---

## Dependencies

- Python 3.8+: `pandas`, `pymongo`, `python-dotenv`
- PostgreSQL 13+
- MongoDB Atlas (or any MongoDB-compatible instance)