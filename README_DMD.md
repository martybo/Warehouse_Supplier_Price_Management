# dm+d Importer (v1)
- Configure `config_dmd.yaml` to match your file headers.
- Run: `pip install pandas pyyaml openpyxl && python dmd_loader.py`
- Outputs: `out_dmd/dmd_items.csv`, `out_dmd/mapping_suggestions.csv` (optional), `out_dmd/manifest.json`.
- Load into Postgres:
  `\COPY dmd_item(vmpp_id, ampp_id, vtm_id, dt_cat, dt_price, dt_pack_size, effective_date) FROM 'out_dmd/dmd_items.csv' CSV HEADER;`
