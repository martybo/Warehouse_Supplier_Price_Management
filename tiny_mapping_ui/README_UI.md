# Tiny dm+d Mapping Reviewer (Flask)

## Setup
1) Extract this folder somewhere (e.g., `C:\Warehouse_Supplier_Price_Management-main\tiny_mapping_ui`).
2) Create and activate a virtual environment (optional).
3) Install deps:
```
pip install -r requirements.txt
```
4) Copy `.env.example` to `.env` and edit values (DB password, API token, approver name).

## Run
```
python app.py
```
Open http://127.0.0.1:5000 and use keys: **A** Approve · **S** Skip · **N** Next.

The UI calls APIs:
- GET `/api/next?packmatch=1` — fetch next candidate (exact pack matches first if checked)
- POST `/api/approve` with `{stage_id}` — writes to `product_mapping` and marks `mapping_stage` approved
- POST `/api/skip` with `{stage_id, note}` — marks reviewed with note
- GET `/api/stats` — summary numbers

