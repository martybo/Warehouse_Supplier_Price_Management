#!/usr/bin/env python3
"""
dm+d Name Loader
Reads:
  - f_vmpp.xlsx (sheet: VMPP) → columns: VPPID + NAME/DESC/NM variants
  - f_ampp.xlsx (first sheet)  → columns: APPID + NAME/DESC/NM variants
Outputs:
  - out_dmd_v2/dmd_names.csv with columns: vmpp_id, ampp_id, dm_name
Run:
  pip install pandas openpyxl
  python dmd_name_loader.py
"""
import os, pandas as pd, json
from datetime import datetime

def pick_col(df, options):
    for c in options:
        if c in df.columns:
            return c
    return None

def load_vmpp_names(path):
    df = pd.read_excel(path, sheet_name="VMPP")
    df.columns = [c.strip() for c in df.columns]
    id_col = pick_col(df, ["VPPID"])
    name_col = pick_col(df, ["NM","DESC","NAME","DESCR","VMPP_NAME","VPPSNM"])
    if not id_col or not name_col:
        return pd.DataFrame(columns=["vmpp_id","dm_name"])
    out = df[[id_col, name_col]].rename(columns={id_col:"vmpp_id", name_col:"dm_name"})
    out["vmpp_id"] = out["vmpp_id"].astype(str).str.strip()
    out["dm_name"] = out["dm_name"].astype(str).str.strip()
    out = out.dropna(subset=["vmpp_id","dm_name"]).drop_duplicates()
    return out

def load_ampp_names(path):
    xl = pd.ExcelFile(path)
    sheet = xl.sheet_names[0]
    df = xl.parse(sheet_name=sheet)
    df.columns = [c.strip() for c in df.columns]
    id_col = pick_col(df, ["APPID"])
    name_col = pick_col(df, ["NM","DESC","NAME","DESCR","AMPP_NAME","AMPSNM"])
    if not id_col or not name_col:
        return pd.DataFrame(columns=["ampp_id","dm_name"])
    out = df[[id_col, name_col]].rename(columns={id_col:"ampp_id", name_col:"dm_name"})
    out["ampp_id"] = out["ampp_id"].astype(str).str.strip()
    out["dm_name"] = out["dm_name"].astype(str).str.strip()
    out = out.dropna(subset=["ampp_id","dm_name"]).drop_duplicates()
    return out

def main():
    f_vmpp = "f_vmpp.xlsx"
    f_ampp = "f_ampp.xlsx"
    vmpp = load_vmpp_names(f_vmpp)
    ampp = load_ampp_names(f_ampp)

    # unify to single file
    vmpp["ampp_id"] = ""
    ampp["vmpp_id"] = ""
    names = pd.concat([vmpp[["vmpp_id","ampp_id","dm_name"]], ampp[["vmpp_id","ampp_id","dm_name"]]], ignore_index=True)
    os.makedirs("out_dmd_v2", exist_ok=True)
    names.to_csv("out_dmd_v2/dmd_names.csv", index=False)

    manifest = {
        "rows": len(names),
        "vmpp_rows": len(vmpp),
        "ampp_rows": len(ampp),
        "created_at_utc": datetime.utcnow().isoformat()+"Z"
    }
    with open("out_dmd_v2/dmd_names_manifest.json","w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote out_dmd_v2/dmd_names.csv ({len(names)} rows)")

if __name__ == "__main__":
    main()
