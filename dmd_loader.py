#!/usr/bin/env python3
# (see file header in previous cell; shortened here for brevity in generation)

import pandas as pd
import yaml
from datetime import datetime
import re
import os

def log(msg): print(f"[{datetime.utcnow().isoformat()}Z] {msg}", flush=True)

def load_any(path, fmt):
    if fmt.lower() == "csv":
        return pd.read_csv(path)
    elif fmt.lower() == "xlsx":
        return pd.read_excel(path)
    else:
        raise ValueError("inputs.format must be csv or xlsx")

def coerce_date(series):
    return pd.to_datetime(series, errors="coerce").dt.date

def normalise_cols(df, colmap):
    out = pd.DataFrame()
    out["effective_date"] = coerce_date(df[colmap["effective_date"]])
    out["dt_price"] = pd.to_numeric(df[colmap["dt_price"]], errors="coerce")
    out["dt_cat"] = df[colmap["dt_cat"]].astype(str)
    def opt(key, default=None):
        col = colmap.get(key)
        return df[col] if col and col in df.columns else default
    out["vmpp_id"] = opt("vmpp_id")
    out["ampp_id"] = opt("ampp_id")
    out["vtm_id"] = opt("vtm_id")
    out["dt_pack_size"] = opt("pack_size")
    out["pip_code"] = opt("pip_code")
    out["name"] = opt("nm")
    out = out.dropna(subset=["effective_date"]).copy()
    return out

def normalise_name(s):
    if pd.isna(s): return ""
    s = str(s).lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def suggest_mappings(dmd_df, products_df):
    sug = []
    dmd_by_pip = {}
    if "pip_code" in dmd_df.columns:
        for _,r in dmd_df.dropna(subset=["pip_code"]).iterrows():
            dmd_by_pip.setdefault(str(r["pip_code"]).strip(), []).append(r)
    dmd_df["nm_norm"] = dmd_df.get("name","").apply(normalise_name)
    for _, p in products_df.iterrows():
        pip = str(p["medicare_pip"]).strip() if pd.notna(p["medicare_pip"]) else ""
        pname = normalise_name(p["name"])
        psize = str(p.get("pack_size","")).strip().lower()
        candidates = []
        if pip and pip in dmd_by_pip:
            for r in dmd_by_pip[pip]:
                candidates.append(("pip", r))
        if not candidates and pname:
            subset = dmd_df[dmd_df["nm_norm"].str.contains(pname[:30], na=False)]
            if psize:
                subset = subset[subset.get("dt_pack_size","").astype(str).str.lower().str.contains(psize, na=False)]
            for _,r in subset.head(5).iterrows():
                candidates.append(("name", r))
        if candidates:
            tag, r = candidates[0]
            sug.append({
                "product_medicare_pip": pip,
                "product_name": p["name"],
                "dmd_match_type": tag,
                "vmpp_id": r.get("vmpp_id",""),
                "ampp_id": r.get("ampp_id",""),
                "vtm_id": r.get("vtm_id",""),
                "dt_cat": r.get("dt_cat",""),
                "dt_price": r.get("dt_price",""),
                "dt_pack_size": r.get("dt_pack_size",""),
                "effective_date": r.get("effective_date",""),
                "dmd_name": r.get("name","")
            })
    return pd.DataFrame(sug)

def main():
    with open("config_dmd.yaml","r") as f:
        cfg = yaml.safe_load(f)
    df = load_any(cfg["inputs"]["file"], cfg["inputs"]["format"])
    colmap = cfg["columns"]
    dmd_norm = normalise_cols(df, colmap)
    outdir = cfg.get("outputs",{}).get("dir","out_dmd")
    os.makedirs(outdir, exist_ok=True)
    dmd_items = dmd_norm[["vmpp_id","ampp_id","vtm_id","dt_cat","dt_price","dt_pack_size","effective_date"]].copy()
    dmd_items.to_csv(os.path.join(outdir,"dmd_items.csv"), index=False)
    products_path = cfg.get("product_file","out/products.csv")
    if os.path.exists(products_path):
        products = pd.read_csv(products_path)
        suggestions = suggest_mappings(dmd_norm, products)
        suggestions.to_csv(os.path.join(outdir,"mapping_suggestions.csv"), index=False)
    manifest = {"rows":{"dmd_items": len(dmd_items)}, "inputs": cfg["inputs"], "created_at_utc": datetime.utcnow().isoformat()+"Z"}
    import json; json.dump(manifest, open(os.path.join(outdir,"manifest.json"),"w"), indent=2)
    log("Done.")

if __name__ == "__main__":
    main()
