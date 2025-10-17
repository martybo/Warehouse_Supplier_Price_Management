#!/usr/bin/env python3
"""
Warehouse Price Platform — Initial Loader (v1)
Reads:
  - Medicare Listings Price Comparison.xlsx (sheet: "Price Comparison")
  - Column_classification_overview.csv (with Buckets + Notes)
  - supplier_alias_proposal.csv (edited canonical names + channels)

Outputs CSVs suitable for PostgreSQL COPY:
  - out/products.csv
  - out/suppliers.csv
  - out/supplier_items.csv  (optional scaffold, unique by supplier+product)
  - out/price_quotes.csv
  - out/reference_columns.csv
  - out/manifest.json (row counts, batch id, timings)

Rules (per spec):
  - Only numeric prices > 0 are staged to price_quotes.
  - Header month = valid_from (YYYY-MM-01).
  - quoted_on = run date; batch_id = initial_migration_<UTC>.
  - Supplier & Channel derived from supplier_alias_proposal.csv; if missing, header is parsed with safe defaults.
  - Columns with Bucket=Reference/Derived, or Notes containing any of:
    "ref only", "reference", "derived", "duplicate", "do not stage", "not part of staging", "exclude"
    are excluded from staging but listed in reference_columns.csv.

Usage:
  Edit config.yaml then:  python loader.py
"""

import os, sys, re, json, hashlib
from datetime import datetime
import pandas as pd
import yaml

def log(msg):
    print(f"[{datetime.utcnow().isoformat()}Z] {msg}", flush=True)

def normalize_bucket_with_notes(bucket, notes):
    t_bucket = str(bucket).strip().lower() if pd.notna(bucket) else ""
    t_notes  = str(notes).strip().lower() if pd.notna(notes) else ""
    if any(k in t_notes for k in ["ref only", "reference", "derived", "duplicate", "do not stage", "not part of staging", "exclude"]):
        return "Reference/Derived"
    if "master" in t_bucket or "dm" in t_bucket:
        return "Master/DM+D"
    if "order" in t_bucket and "qty" in t_bucket:
        return "Order Qty"
    if "supplier" in t_bucket or "price" in t_bucket:
        return "Supplier/Price"
    if "reference" in t_bucket or "derived" in t_bucket:
        return "Reference/Derived"
    if "other" in t_bucket or "meta" in t_bucket:
        return "Other/Meta"
    return "Other/Meta"

MONTH_MAP = {'jan':1,'january':1,'feb':2,'february':2,'mar':3,'march':3,'apr':4,'april':4,'may':5,
             'jun':6,'june':6,'jul':7,'july':7,'aug':8,'august':8,'sep':9,'sept':9,'september':9,
             'oct':10,'october':10,'nov':11,'november':11,'dec':12,'december':12}

# Safe channel word-boundary patterns
CHANNEL_PATTERNS = [
    ("Direct",       r"\bdirect\b"),
    ("Proposition",  r"\b(prop|proposition)\b"),
    ("T&R",          r"\b(t\s*&\s*r|t\s*and\s*r|tand r|t&r)\b"),
    ("Short-dated",  r"\b(short[-\s]?dated|shortdated|s/d)\b"),
    ("Spot",         r"\b(spot\s*buy|spot-buy|spotbuy|spot)\b"),
    ("Promo",        r"\b(promo|promotion|promotional)\b"),
    ("Tender",       r"\b(tender|tendered)\b"),
]

def parse_valid_from(raw_header):
    lc = str(raw_header).lower()
    m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[^\d]{0,3}(\d{2,4})', lc)
    if not m:
        return ""
    mon, y = m.group(1), m.group(2)
    month = MONTH_MAP.get(mon, None)
    if month is None: return ""
    y = int(y)
    year = 2000 + y if y < 100 else y
    return f"{year:04d}-{month:02d}-01"

def parse_supplier_and_channel_from_header(header_text):
    # Fallback parsing when alias table doesn't provide a mapping for this column
    text = str(header_text).strip()
    lc = text.lower()
    base = re.sub(r'[\-\–\—_/]*\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[^\d]{0,3}(\d{2,4})?', '', lc, flags=re.IGNORECASE)
    base = re.sub(r'\s{2,}', ' ', base).strip()
    for n in ["price", "concessions", "orderlist", "last purchased"]:
        base = re.sub(rf'\b{n}\b', '', base, flags=re.IGNORECASE).strip()
    detected_channel = ""
    for label, pat in CHANNEL_PATTERNS:
        if re.search(pat, base, flags=re.IGNORECASE):
            detected_channel = label
            base = re.sub(pat, '', base, flags=re.IGNORECASE)
    supplier_name = re.sub(r'\s{2,}', ' ', base).strip().title()
    if not supplier_name:
        supplier_name = header_text
    return supplier_name, detected_channel

def col_signature(series):
    s = series.astype(str).replace({"nan":"<NA>"}).str.strip()
    joined = "|".join(s.tolist())
    return hashlib.md5(joined.encode("utf-8")).hexdigest()

def main():
    with open("config.yaml","r") as f:
        cfg = yaml.safe_load(f)

    src_excel = cfg["inputs"]["price_workbook"]
    src_sheet = cfg["inputs"]["sheet_name"]
    src_mapping = cfg["inputs"]["column_mapping_csv"]
    src_alias = cfg["inputs"]["supplier_alias_csv"]
    out_dir = cfg["outputs"]["dir"]
    os.makedirs(out_dir, exist_ok=True)

    log("Loading inputs…")
    df = pd.read_excel(src_excel, sheet_name=src_sheet)
    mapping = pd.read_csv(src_mapping)
    if "Notes" not in mapping.columns: mapping["Notes"] = ""
    alias = pd.read_csv(src_alias)

    # Normalize mapping
    mapping["FinalBucket"] = [normalize_bucket_with_notes(b, n) for b, n in zip(mapping.get("Bucket",""), mapping.get("Notes",""))]
    mapping_present = mapping[mapping["Column"].isin(df.columns)].copy()

    # Build reference column table
    ref_cols = mapping_present.loc[mapping_present["FinalBucket"]=="Reference/Derived", ["Column","Notes"]].copy()
    ref_cols.rename(columns={"Column":"column_name","Notes":"notes"}, inplace=True)
    ref_cols["last_seen_on"] = datetime.utcnow().date().isoformat()
    ref_out = os.path.join(out_dir, "reference_columns.csv")
    ref_cols.to_csv(ref_out, index=False)
    log(f"Reference/Derived columns: {len(ref_cols)}")

    # Identify supplier/price columns for staging
    supplier_cols = mapping_present.loc[mapping_present["FinalBucket"]=="Supplier/Price","Column"].tolist()

    # Build alias dicts
    alias_cols = {row["SourceColumn"]: (row.get("ProposedSupplier",""), row.get("ProposedChannel","")) for _,row in alias.iterrows()}

    # Create supplier + channel lookup per column
    col_to_sup_chan = {}
    for col in supplier_cols:
        if col in alias_cols and (str(alias_cols[col][0]).strip() or str(alias_cols[col][1]).strip()):
            sup, chan = alias_cols[col]
        else:
            sup, chan = parse_supplier_and_channel_from_header(col)
        col_to_sup_chan[col] = (str(sup).strip().title(), str(chan).strip())

    # Suppliers list
    suppliers = sorted({v[0] for v in col_to_sup_chan.values() if v[0]})
    suppliers_df = pd.DataFrame({"name": suppliers})
    suppliers_out = os.path.join(out_dir, "suppliers.csv")
    suppliers_df.to_csv(suppliers_out, index=False)

    # Products (from sheet)
    pip_col = "MediCare PIPCode" if "MediCare PIPCode" in df.columns else None
    name_col = "Product Name" if "Product Name" in df.columns else None
    size_col = "Pack Size" if "Pack Size" in df.columns else None
    products = pd.DataFrame()
    if pip_col and name_col:
        products = df[[pip_col, name_col] + ([size_col] if size_col else [])].dropna(subset=[pip_col]).drop_duplicates()
        products.rename(columns={pip_col:"medicare_pip", name_col:"name", size_col:"pack_size" if size_col else "":""}, inplace=True)
        products_out = os.path.join(out_dir, "products.csv")
        products.to_csv(products_out, index=False)

    # Melt to staging
    base_cols = [c for c in ["MediCare PIPCode","Product Name","Pack Size"] if c in df.columns]
    melted = pd.DataFrame()
    if base_cols and supplier_cols:
        melted = df[base_cols + supplier_cols].melt(id_vars=base_cols, var_name="SourceColumn", value_name="QuotedPrice")
        # Numeric > 0 only
        melted["QuotedPrice"] = pd.to_numeric(melted["QuotedPrice"].astype(str).str.replace(",","").str.strip(), errors="coerce")
        melted = melted[melted["QuotedPrice"].notna() & (melted["QuotedPrice"] > 0)]

        # Enrich with supplier/channel and dates
        melted["Supplier"] = melted["SourceColumn"].map(lambda c: col_to_sup_chan.get(c, ("", ""))[0])
        melted["Channel"]  = melted["SourceColumn"].map(lambda c: col_to_sup_chan.get(c, ("", ""))[1])
        melted["ValidFrom"] = melted["SourceColumn"].map(parse_valid_from)
        melted["QuotedOn"]  = datetime.utcnow().date().isoformat()
        melted["BatchId"]   = "initial_migration_" + datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

        # Reorder & rename
        rename_map = {"MediCare PIPCode":"MediCarePIPCode", "Product Name":"ProductName", "Pack Size":"PackSize"}
        keep = [c for c in ["MediCare PIPCode","Product Name","Pack Size"] if c in melted.columns]
        melted = melted[keep + ["Supplier","Channel","SourceColumn","ValidFrom","QuotedOn","BatchId","QuotedPrice"]].rename(columns=rename_map)

    # Export price_quotes.csv (staging)
    pq_out = os.path.join(out_dir, "price_quotes.csv")
    melted.to_csv(pq_out, index=False)

    # Optional supplier_items scaffold (supplier x product combos seen)
    supplier_items = pd.DataFrame()
    if not melted.empty:
        supplier_items = melted[["Supplier","MediCarePIPCode"]].drop_duplicates().copy()
        supplier_items.rename(columns={"MediCarePIPCode":"medicare_pip"}, inplace=True)
        supplier_items_out = os.path.join(out_dir, "supplier_items.csv")
        supplier_items.to_csv(supplier_items_out, index=False)

    # Duplicate report (by identical data signatures per supplier column)
    dupe_groups = {}
    for col in supplier_cols:
        sig = hashlib.md5("|".join(df[col].astype(str).replace({"nan":"<NA>"}).str.strip().tolist()).encode("utf-8")).hexdigest()
        dupe_groups.setdefault(sig, []).append(col)
    dupes = [{"signature":sig, "columns":"; ".join(cols), "count":len(cols)} for sig, cols in dupe_groups.items() if len(cols) > 1]
    dupes_df = pd.DataFrame(dupes).sort_values("count", ascending=False) if dupes else pd.DataFrame(columns=["signature","columns","count"])
    dupes_out = os.path.join(out_dir, "duplicates.csv")
    dupes_df.to_csv(dupes_out, index=False)

    # Manifest
    manifest = {
        "batch_id": melted["BatchId"].iloc[0] if not melted.empty else None,
        "rows": {
            "products": len(products) if not products.empty else 0,
            "suppliers": len(suppliers_df),
            "supplier_items": len(supplier_items) if not supplier_items.empty else 0,
            "price_quotes": len(melted),
            "reference_columns": len(ref_cols),
            "duplicates": len(dupes_df)
        },
        "inputs": {
            "excel": src_excel,
            "sheet": src_sheet,
            "mapping": src_mapping,
            "alias": src_alias
        },
        "created_at_utc": datetime.utcnow().isoformat()+"Z"
    }
    with open(os.path.join(out_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

    log("Done.")
    log(json.dumps(manifest, indent=2))

if __name__ == "__main__":
    main()
