#!/usr/bin/env python3
"""
Warehouse Price Platform — Initial Loader (v1)

This loader ingests the workbook and supporting CSV metadata to emit the
PostgreSQL ready extracts described in the README.
"""

import json
import hashlib
import os
import re
from datetime import datetime
from typing import Dict, Iterable, Tuple

import pandas as pd
import yaml


def log(msg: str) -> None:
    print(f"[{datetime.utcnow().isoformat()}Z] {msg}", flush=True)


REFERENCE_NOTE_KEYWORDS = {
    "ref only",
    "reference",
    "derived",
    "duplicate",
    "do not stage",
    "not part of staging",
    "exclude",
}


def normalize_bucket_with_notes(bucket, notes) -> str:
    """Map the supplied bucket + notes to a canonical FinalBucket value."""

    t_bucket = str(bucket).strip().lower() if pd.notna(bucket) else ""
    t_notes = str(notes).strip().lower() if pd.notna(notes) else ""

    if any(keyword in t_notes for keyword in REFERENCE_NOTE_KEYWORDS):
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


MONTH_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

CHANNEL_PATTERNS = [
    ("Direct", r"\bdirect\b"),
    ("Proposition", r"\b(prop|proposition)\b"),
    ("T&R", r"\b(t\s*&\s*r|t\s*and\s*r|tand r|t&r)\b"),
    ("Short-dated", r"\b(short[-\s]?dated|shortdated|s/d)\b"),
    ("Spot", r"\b(spot\s*buy|spot-buy|spotbuy|spot)\b"),
    ("Promo", r"\b(promo|promotion|promotional)\b"),
    ("Tender", r"\b(tender|tendered)\b"),
]


def parse_valid_from(raw_header) -> str:
    lc = str(raw_header).lower()
    match = re.search(
        r"(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[^\d]{0,3}(\d{2,4})",
        lc,
    )
    if not match:
        return ""
    mon, year_suffix = match.group(1), match.group(2)
    month = MONTH_MAP.get(mon)
    if month is None:
        return ""
    year = int(year_suffix)
    year = 2000 + year if year < 100 else year
    return f"{year:04d}-{month:02d}-01"


def parse_supplier_and_channel_from_header(header_text) -> Tuple[str, str]:
    """Fallback parsing when alias table lacks the column."""

    text = str(header_text).strip()
    base = re.sub(
        r"[\-\–\—_/]*\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[^\d]{0,3}(\d{2,4})?",
        "",
        text,
        flags=re.IGNORECASE,
    )
    base = re.sub(r"\s{2,}", " ", base).strip()
    for marker in ["price", "concessions", "orderlist", "last purchased"]:
        base = re.sub(rf"\b{marker}\b", "", base, flags=re.IGNORECASE).strip()

    detected_channel = ""
    for label, pattern in CHANNEL_PATTERNS:
        if re.search(pattern, base, flags=re.IGNORECASE):
            detected_channel = label
            base = re.sub(pattern, "", base, flags=re.IGNORECASE)

    supplier_name = re.sub(r"\s{2,}", " ", base).strip().title()
    if not supplier_name:
        supplier_name = text
    return supplier_name, detected_channel


def col_signature(series: pd.Series) -> str:
    cleaned = series.astype(str).replace({"nan": "<NA>"}).str.strip()
    joined = "|".join(cleaned.tolist())
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as config_handle:
        cfg = yaml.safe_load(config_handle)

    required_paths = [
        ("inputs", "price_workbook"),
        ("inputs", "sheet_name"),
        ("inputs", "column_mapping_csv"),
        ("inputs", "supplier_alias_csv"),
        ("outputs", "dir"),
    ]
    for section, key in required_paths:
        if section not in cfg or key not in cfg[section]:
            raise KeyError(f"Missing `{section}.{key}` in config file {path}.")

    for file_key in ["price_workbook", "column_mapping_csv", "supplier_alias_csv"]:
        file_path = cfg["inputs"][file_key]
        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"Required input `{file_key}` not found at {file_path}."
            )

    return cfg


def load_price_workbook(path: str, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet_name)
    if df.empty:
        log("Warning: price workbook is empty.")
    return df


def load_column_mapping(path: str) -> pd.DataFrame:
    mapping = pd.read_csv(path)
    if "Column" not in mapping.columns:
        raise ValueError("Column mapping must include a `Column` header.")
    if "Bucket" not in mapping.columns:
        mapping["Bucket"] = ""
    if "Notes" not in mapping.columns:
        mapping["Notes"] = ""
    mapping["FinalBucket"] = [
        normalize_bucket_with_notes(bucket, note)
        for bucket, note in zip(mapping["Bucket"], mapping["Notes"])
    ]
    return mapping


def load_alias_table(path: str) -> pd.DataFrame:
    alias = pd.read_csv(path)
    expected = {"SourceColumn", "ProposedSupplier", "ProposedChannel"}
    missing = expected.difference(alias.columns)
    if missing:
        raise ValueError(
            "Supplier alias file must include columns: "
            f"{', '.join(sorted(expected))}. Missing: {', '.join(sorted(missing))}"
        )
    return alias.fillna("")


def prepare_mapping(mapping: pd.DataFrame, present_columns: Iterable[str]) -> pd.DataFrame:
    mapping_present = mapping[mapping["Column"].isin(present_columns)].copy()
    if mapping_present.empty:
        log("Warning: no mapping rows matched the workbook headers.")
    return mapping_present


def build_reference_columns(mapping_present: pd.DataFrame, out_dir: str) -> pd.DataFrame:
    ref_cols = mapping_present.loc[
        mapping_present["FinalBucket"] == "Reference/Derived", ["Column", "Notes"]
    ].copy()
    ref_cols.rename(columns={"Column": "column_name", "Notes": "notes"}, inplace=True)
    ref_cols["last_seen_on"] = datetime.utcnow().date().isoformat()
    ref_cols.to_csv(os.path.join(out_dir, "reference_columns.csv"), index=False)
    log(f"Reference/Derived columns: {len(ref_cols)}")
    return ref_cols


def build_supplier_channel_lookup(
    supplier_cols: Iterable[str], alias: pd.DataFrame
) -> Dict[str, Tuple[str, str]]:
    alias_lookup = {
        row["SourceColumn"]: (
            row.get("ProposedSupplier", ""),
            row.get("ProposedChannel", ""),
        )
        for _, row in alias.iterrows()
    }

    col_to_sup_chan: Dict[str, Tuple[str, str]] = {}
    for col in supplier_cols:
        aliased = alias_lookup.get(col, ("", ""))
        if any(str(value).strip() for value in aliased):
            supplier, channel = aliased
        else:
            supplier, channel = parse_supplier_and_channel_from_header(col)
        col_to_sup_chan[col] = (str(supplier).strip().title(), str(channel).strip())

    return col_to_sup_chan


def export_suppliers(col_to_sup_chan: Dict[str, Tuple[str, str]], out_dir: str) -> pd.DataFrame:
    suppliers = sorted({name for name, _ in col_to_sup_chan.values() if name})
    suppliers_df = pd.DataFrame({"name": suppliers})
    suppliers_df.to_csv(os.path.join(out_dir, "suppliers.csv"), index=False)
    return suppliers_df


def export_products(df: pd.DataFrame, out_dir: str) -> pd.DataFrame:
    pip_col = "MediCare PIPCode" if "MediCare PIPCode" in df.columns else None
    name_col = "Product Name" if "Product Name" in df.columns else None
    size_col = "Pack Size" if "Pack Size" in df.columns else None

    products = pd.DataFrame()
    if pip_col and name_col:
        cols = [pip_col, name_col] + ([size_col] if size_col else [])
        products = df[cols].dropna(subset=[pip_col]).drop_duplicates()
        rename_map = {pip_col: "medicare_pip", name_col: "name"}
        if size_col:
            rename_map[size_col] = "pack_size"
        products = products.rename(columns=rename_map)
        products.to_csv(os.path.join(out_dir, "products.csv"), index=False)
    else:
        log("Warning: unable to emit products.csv (missing PIP or Product Name columns).")

    return products


def build_price_quotes(
    df: pd.DataFrame,
    supplier_cols: Iterable[str],
    col_to_sup_chan: Dict[str, Tuple[str, str]],
) -> pd.DataFrame:
    base_cols = [
        column
        for column in ["MediCare PIPCode", "Product Name", "Pack Size"]
        if column in df.columns
    ]
    melted = pd.DataFrame()
    if base_cols and supplier_cols:
        melted = df[base_cols + list(supplier_cols)].melt(
            id_vars=base_cols, var_name="SourceColumn", value_name="QuotedPrice"
        )
        melted["QuotedPrice"] = pd.to_numeric(
            melted["QuotedPrice"].astype(str).str.replace(",", "").str.strip(),
            errors="coerce",
        )
        melted = melted[melted["QuotedPrice"].notna() & (melted["QuotedPrice"] > 0)]

        melted["Supplier"] = melted["SourceColumn"].map(
            lambda column: col_to_sup_chan.get(column, ("", ""))[0]
        )
        melted["Channel"] = melted["SourceColumn"].map(
            lambda column: col_to_sup_chan.get(column, ("", ""))[1]
        )
        melted["ValidFrom"] = melted["SourceColumn"].map(parse_valid_from)
        run_date = datetime.utcnow()
        melted["QuotedOn"] = run_date.date().isoformat()
        melted["BatchId"] = "initial_migration_" + run_date.strftime("%Y%m%dT%H%M%SZ")

        rename_map = {
            "MediCare PIPCode": "MediCarePIPCode",
            "Product Name": "ProductName",
            "Pack Size": "PackSize",
        }
        keep = [
            column
            for column in ["MediCare PIPCode", "Product Name", "Pack Size"]
            if column in melted.columns
        ]
        melted = melted[
            keep
            + [
                "Supplier",
                "Channel",
                "SourceColumn",
                "ValidFrom",
                "QuotedOn",
                "BatchId",
                "QuotedPrice",
            ]
        ].rename(columns=rename_map)
    else:
        log(
            "Warning: unable to build price_quotes.csv (missing ID columns or supplier columns)."
        )

    return melted


def export_price_quotes(melted: pd.DataFrame, out_dir: str) -> None:
    melted.to_csv(os.path.join(out_dir, "price_quotes.csv"), index=False)


def export_supplier_items(melted: pd.DataFrame, out_dir: str) -> pd.DataFrame:
    supplier_items = pd.DataFrame()
    if not melted.empty and "MediCarePIPCode" in melted.columns:
        supplier_items = melted[["Supplier", "MediCarePIPCode"]].drop_duplicates().copy()
        supplier_items.rename(columns={"MediCarePIPCode": "medicare_pip"}, inplace=True)
        supplier_items.to_csv(os.path.join(out_dir, "supplier_items.csv"), index=False)
    return supplier_items


def export_duplicates_report(
    df: pd.DataFrame, supplier_cols: Iterable[str], out_dir: str
) -> pd.DataFrame:
    dupe_groups: Dict[str, list] = {}
    for column in supplier_cols:
        if column not in df.columns:
            continue
        signature = col_signature(df[column])
        dupe_groups.setdefault(signature, []).append(column)
    dupes = [
        {
            "signature": signature,
            "columns": "; ".join(columns),
            "count": len(columns),
        }
        for signature, columns in dupe_groups.items()
        if len(columns) > 1
    ]
    dupes_df = (
        pd.DataFrame(dupes).sort_values("count", ascending=False)
        if dupes
        else pd.DataFrame(columns=["signature", "columns", "count"])
    )
    dupes_df.to_csv(os.path.join(out_dir, "duplicates.csv"), index=False)
    return dupes_df


def build_manifest(
    *,
    melted: pd.DataFrame,
    products: pd.DataFrame,
    suppliers: pd.DataFrame,
    supplier_items: pd.DataFrame,
    ref_cols: pd.DataFrame,
    dupes: pd.DataFrame,
    inputs: Dict[str, str],
) -> Dict:
    batch_id = melted["BatchId"].iloc[0] if not melted.empty else None
    return {
        "batch_id": batch_id,
        "rows": {
            "products": int(len(products)) if not products.empty else 0,
            "suppliers": int(len(suppliers)),
            "supplier_items": int(len(supplier_items)) if not supplier_items.empty else 0,
            "price_quotes": int(len(melted)),
            "reference_columns": int(len(ref_cols)),
            "duplicates": int(len(dupes)),
        },
        "inputs": inputs,
        "created_at_utc": datetime.utcnow().isoformat() + "Z",
    }


def main() -> None:
    cfg = load_config("config.yaml")

    src_excel = cfg["inputs"]["price_workbook"]
    src_sheet = cfg["inputs"]["sheet_name"]
    src_mapping = cfg["inputs"]["column_mapping_csv"]
    src_alias = cfg["inputs"]["supplier_alias_csv"]
    out_dir = cfg["outputs"]["dir"]
    os.makedirs(out_dir, exist_ok=True)

    log("Loading inputs…")
    df = load_price_workbook(src_excel, src_sheet)
    mapping = load_column_mapping(src_mapping)
    alias = load_alias_table(src_alias)

    mapping_present = prepare_mapping(mapping, df.columns)

    ref_cols = build_reference_columns(mapping_present, out_dir)

    supplier_cols = mapping_present.loc[
        mapping_present["FinalBucket"] == "Supplier/Price", "Column"
    ].tolist()

    col_to_sup_chan = build_supplier_channel_lookup(supplier_cols, alias)

    suppliers_df = export_suppliers(col_to_sup_chan, out_dir)

    products = export_products(df, out_dir)

    melted = build_price_quotes(df, supplier_cols, col_to_sup_chan)
    export_price_quotes(melted, out_dir)

    supplier_items = export_supplier_items(melted, out_dir)

    dupes_df = export_duplicates_report(df, supplier_cols, out_dir)

    manifest = build_manifest(
        melted=melted,
        products=products,
        suppliers=suppliers_df,
        supplier_items=supplier_items,
        ref_cols=ref_cols,
        dupes=dupes_df,
        inputs={
            "excel": src_excel,
            "sheet": src_sheet,
            "mapping": src_mapping,
            "alias": src_alias,
        },
    )
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    log("Done.")
    log(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
