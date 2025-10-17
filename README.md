# Warehouse Price Platform — Initial Loader (v1)

This loader converts your existing workbook into database-ready CSVs for PostgreSQL.

## What it does
- Reads your **price workbook**, **column mapping**, and **supplier alias** files.
- Excludes **Reference/Derived** columns (based on Bucket and Notes).
- Parses supplier **Channel** (Direct, Proposition, T&R, Short-dated, Spot, Promo, Tender).
- Treats header month (e.g., "AUG 25") as **ValidFrom = 2025-08-01**.
- Includes only numeric prices **> 0** as `price_quotes` rows.
- Tags each run with **QuotedOn** and a **BatchId**.
- Emits **CSV** files suitable for **COPY** into Postgres.

## Files it produces (in `out/`)
- `products.csv` — unique products by PIP (name, pack size where available)
- `suppliers.csv` — canonical supplier names from your alias file / header parsing
- `supplier_items.csv` — supplier × product pairs seen in the data (scaffold)
- `price_quotes.csv` — long format price rows with Supplier, Channel, ValidFrom, QuotedOn, BatchId
- `reference_columns.csv` — all excluded Reference/Derived columns with notes
- `duplicates.csv` — columns with identical data signatures
- `manifest.json` — counts and metadata

## Prereqs
- Python 3.10+
- `pip install pandas pyyaml openpyxl`

## How to run
Place these files in the same folder:
```
Medicare Listings Price Comparison.xlsx
Column_classification_overview.csv
supplier_alias_proposal.csv
config.yaml
loader.py
```

Run:
```
python loader.py
```

Outputs appear in `./out`.

## Load into PostgreSQL (example)
```sql
-- Create tables first (use schema_postgres_v1.sql)

-- Adjust paths to your environment; on Windows, use COPY FROM PROGRAM with psql \copy if needed
\COPY supplier(name) FROM 'out/suppliers.csv' CSV HEADER;
\COPY product(medicare_pip,name,pack_size) FROM 'out/products.csv' CSV HEADER;

-- supplier_items.csv maps supplier names to product PIPs; you'll resolve IDs after loading supplier/product
-- For price_quotes.csv you may initially load supplier_id/product_id by joining names/PIPs in a staging table,
-- then populate supplier_item_id when ready.

-- Example staging approach:
CREATE TEMP TABLE pq_stage (
  medicarepipcode TEXT,
  productname TEXT,
  packsize TEXT,
  supplier TEXT,
  channel TEXT,
  sourcecolumn TEXT,
  validfrom DATE,
  quotedon DATE,
  batchid TEXT,
  quotedprice NUMERIC
);

\COPY pq_stage FROM 'out/price_quotes.csv' CSV HEADER;

-- Then upsert suppliers/products if needed, resolve IDs, and insert into price_quote.
```

## Notes
- Zero values and non-numeric entries are excluded by design.
- If a supplier/channel needs a custom mapping, add it in `supplier_alias_proposal.csv` and rerun.
- The loader prefers your **alias file**; if a column isn’t present there, it will safely parse the header.
