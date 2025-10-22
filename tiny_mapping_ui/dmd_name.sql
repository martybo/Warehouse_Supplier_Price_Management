-- Create dm+d name lookup table and load from CSV
CREATE TABLE IF NOT EXISTS dmd_name (
  vmpp_id TEXT,
  ampp_id TEXT,
  dm_name TEXT
);

-- Example COPY command (adjust path separators as needed on Windows):
-- \COPY dmd_name(vmpp_id, ampp_id, dm_name) FROM 'C:/Warehouse_Supplier_Price_Management-main/out_dmd_v2/dmd_names.csv' CSV HEADER;
