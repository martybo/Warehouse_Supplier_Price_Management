# Tiny dm+d Mapping Reviewer (Flask)

## Setup
1) Extract this folder somewhere (e.g., `C:\Warehouse_Supplier_Price_Management-main\tiny_mapping_ui`).
2) Create and activate a virtual environment (optional).
3) Install deps:
```
pip install -r requirements.txt
```
   If you plan to run the dm+d name loader, install the additional data tooling:
``` 
pip install pandas openpyxl
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

## dm+d name loader

The helper script `dmd_name_loader.py` can be invoked with custom input/output locations:

```
python dmd_name_loader.py --vmpp /path/to/f_vmpp.xlsx --ampp /path/to/f_ampp.xlsx --out-dir ./out_dmd_v2
```

Warnings are emitted if the expected columns are missing, and the output directory will contain both the CSV lookup and a manifest JSON file describing the run.

