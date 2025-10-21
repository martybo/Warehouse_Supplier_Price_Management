#!/usr/bin/env python3
# dm+d Importer (v2.3) — resilient to AMPP/VMPP/lookup column variants
# Inputs (place alongside this script or pass --data-dir):
#   f_vmpp.xlsx   (sheets: DtInfo + VMPP)
#   f_ampp.xlsx   (any sheet; APPID required; PRICE/PRICEDT optional; REIMBSTATDT accepted)
#   f_lookup.xlsx (sheet: DtPayCat; columns can be CD/DESC or PAY_CATCD/PAY_CATNM)
#
# Outputs (out_dmd_v2/):
#   dmd_items.csv        -> vmpp/ampp rows for dmd_item
#   dmd_attributes.csv   -> attributes (ZD + pay category) keyed by APPID/VPPID
#   manifest.json

from __future__ import annotations
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd
import json
import os


def log(msg: str) -> None:
    print(f"[{datetime.utcnow().isoformat()}Z] {msg}", flush=True)


# ---------- Readers ----------
def read_vmpp(path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Read f_vmpp.xlsx (DtInfo + VMPP) and strip column whitespace."""
    df_info = pd.read_excel(path, sheet_name="DtInfo")
    df_vmpp = pd.read_excel(path, sheet_name="VMPP")
    df_info.columns = [c.strip() for c in df_info.columns]
    df_vmpp.columns = [c.strip() for c in df_vmpp.columns]
    return df_info, df_vmpp


def read_any_ampp_sheet(path: Path) -> pd.DataFrame:
    """Read f_ampp.xlsx from a sensible sheet (AMPP/Amp/Sheet1 or first)."""
    xl = pd.ExcelFile(path)
    preferred = ["AMPP", "Amp", "Sheet1"]
    target = next((n for n in preferred if n in xl.sheet_names), xl.sheet_names[0])
    df = xl.parse(target)
    df.columns = [c.strip() for c in df.columns]
    return df


def read_lookup(path: Path) -> pd.DataFrame:
    """Read f_lookup.xlsx (DtPayCat) and strip column whitespace."""
    df = pd.read_excel(path, sheet_name="DtPayCat")
    df.columns = [c.strip() for c in df.columns]
    return df


# ---------- Column helpers ----------
def find_col(df: pd.DataFrame, *candidates: str, required: bool = True) -> Optional[str]:
    for n in candidates:
        if n in df.columns:
            return n
    if required:
        raise KeyError(f"None of {candidates} found in columns: {df.columns.tolist()}")
    return None


def ensure_exists(p: Path) -> Path:
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {p}")
    return p


# ---------- Main ----------
def main(data_dir: Optional[Path] = None) -> None:
    base_path = data_dir.resolve() if data_dir else Path(__file__).resolve().parent
    f_vmpp = ensure_exists(base_path / "f_vmpp.xlsx")
    f_ampp = ensure_exists(base_path / "f_ampp.xlsx")
    f_lookup = ensure_exists(base_path / "f_lookup.xlsx")

    log(f"Reading: {f_vmpp.name}, {f_ampp.name}, {f_lookup.name}")

    info, vmpp_tab = read_vmpp(f_vmpp)
    ampp_raw = read_any_ampp_sheet(f_ampp)
    lookup = read_lookup(f_lookup)

    # ---- VMPP (DtInfo + pack size from VMPP sheet) ----
    VPPID_info = find_col(info, "VPPID")
    PRICE_info = find_col(info, "PRICE")
    DT_info = find_col(info, "DT", "PRICE_DT", "EFFECTIVE_DT")
    PAYCD_info = find_col(info, "PAY_CATCD", "PAYCATCD", "PAY_CAT_CD")

    VPPID_vmpp = find_col(vmpp_tab, "VPPID", "VMPP", "VMPP_ID", "VPP_ID", "VMPPID")
    PACK_vmpp = find_col(
        vmpp_tab, "QTVAL", "QTYVAL", "QTY", "PACK_SIZE", "QTY_VAL", "QTY VALUE", required=False
    )

    vmpp = pd.DataFrame(
        {
            "VPPID": info[VPPID_info],
            "dt_price": pd.to_numeric(info[PRICE_info], errors="coerce"),
            "effective_date": pd.to_datetime(info[DT_info], errors="coerce").dt.date,
            "pay_catcd": info[PAYCD_info].astype(str).str.strip(),
        }
    )

    if PACK_vmpp is not None:
        vmpp = vmpp.merge(
            vmpp_tab[[VPPID_vmpp, PACK_vmpp]].rename(
                columns={VPPID_vmpp: "VPPID", PACK_vmpp: "dt_pack_size"}
            ),
            on="VPPID",
            how="left",
        )
    else:
        vmpp["dt_pack_size"] = None

    vmpp = vmpp.rename(columns={"VPPID": "vmpp_id"}).dropna(
        subset=["vmpp_id", "effective_date"]
    )

    # Attach category name from lookup (accept CD/DESC or PAY_CAT*)
    L_CODE = find_col(lookup, "PAY_CATCD", "PAYCATCD", "PAY_CAT_CD", "Code", "CD")
    L_NAME = find_col(lookup, "PAY_CATNM", "PAY_CAT", "Name", "DESC", required=False)

    if L_NAME is not None:
        lk = lookup[[L_CODE, L_NAME]].copy()
        lk[L_CODE] = lk[L_CODE].astype(str).str.strip()
        vmpp = (
            vmpp.merge(lk, left_on="pay_catcd", right_on=L_CODE, how="left")
            .rename(columns={L_NAME: "pay_cat_name"})
            .drop(columns=[L_CODE])
        )
    else:
        vmpp["pay_cat_name"] = None

    # ---- AMPP (price + Zero Discount) — tolerate missing columns ----
    APPID = find_col(ampp_raw, "APPID")
    APR = find_col(ampp_raw, "PRICE", required=False)  # may be absent
    ADT = find_col(
        ampp_raw, "PRICEDT", "PRICE_DT", "EFFECTIVE_DT", "REIMBSTATDT", required=False
    )
    AZD = find_col(ampp_raw, "ZERO_DISCD", "ZERO_DISCOUNT", required=False)

    zero_discount = (
        ampp_raw[AZD].astype(str).str.strip().isin({"0001", "0002"})
        if AZD is not None
        else pd.Series(False, index=ampp_raw.index)
    )

    ampp_cols = {
        "ampp_id": ampp_raw[APPID],
        "dt_price": pd.to_numeric(ampp_raw[APR], errors="coerce") if APR is not None else None,
        "effective_date": pd.to_datetime(ampp_raw[ADT], errors="coerce").dt.date
        if ADT is not None
        else pd.NaT,
        "zero_discount": zero_discount,
    }
    ampp = pd.DataFrame(ampp_cols).dropna(subset=["ampp_id"]).copy()

    # If we have no usable date at all, drop AMPP rows (keep VMPP fully)
    if "effective_date" in ampp and ampp["effective_date"].notna().any():
        ampp = ampp.dropna(subset=["effective_date"]).copy()
    else:
        ampp = ampp.iloc[0:0]

    # ---- Compose outputs ----
    dmd_items_vmpp = vmpp.assign(vtm_id=None, ampp_id=None, dt_cat=vmpp["pay_catcd"])[
        ["vmpp_id", "ampp_id", "vtm_id", "dt_cat", "dt_price", "dt_pack_size", "effective_date"]
    ]
    dmd_items_ampp = ampp.assign(vmpp_id=None, vtm_id=None, dt_cat=None, dt_pack_size=None)[
        ["vmpp_id", "ampp_id", "vtm_id", "dt_cat", "dt_price", "dt_pack_size", "effective_date"]
    ]
    dmd_items = pd.concat([dmd_items_vmpp, dmd_items_ampp], ignore_index=True)

    dmd_attr_vmpp = vmpp[["vmpp_id", "pay_catcd", "pay_cat_name", "effective_date"]].copy()
    dmd_attr_vmpp.insert(1, "level", "VMPP")
    dmd_attr_vmpp.insert(2, "zero_discount", False)
    dmd_attr_vmpp = dmd_attr_vmpp.rename(columns={"vmpp_id": "dmd_key"})

    if len(ampp):
        dmd_attr_ampp = ampp[["ampp_id", "zero_discount", "effective_date"]].copy()
        dmd_attr_ampp.insert(1, "level", "AMPP")
        dmd_attr_ampp.insert(3, "pay_catcd", None)
        dmd_attr_ampp.insert(4, "pay_cat_name", None)
        dmd_attr_ampp = dmd_attr_ampp.rename(columns={"ampp_id": "dmd_key"})
    else:
        dmd_attr_ampp = pd.DataFrame(
            columns=["dmd_key", "level", "zero_discount", "pay_catcd", "pay_cat_name", "effective_date"]
        )

    dmd_attributes = pd.concat([dmd_attr_vmpp, dmd_attr_ampp], ignore_index=True)

    outdir = base_path / "out_dmd_v2"
    outdir.mkdir(exist_ok=True)

    dmd_items.to_csv(outdir / "dmd_items.csv", index=False)
    dmd_attributes.to_csv(outdir / "dmd_attributes.csv", index=False)
    with (outdir / "manifest.json").open("w") as f:
        json.dump(
            {
                "rows": {
                    "dmd_items": len(dmd_items),
                    "dmd_attributes": len(dmd_attributes),
                    "vmpp_only": len(dmd_items_vmpp),
                    "ampp_rows": len(dmd_items_ampp),
                },
                "notes": "AMPP PRICE/PRICEDT optional; REIMBSTATDT accepted; VMPP pack size merged from VMPP sheet; lookup CD/DESC handled",
                "created_at_utc": datetime.utcnow().isoformat() + "Z",
            },
            f,
            indent=2,
        )

    log("Done (dm+d v2.3).")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="dm+d loader v2.3")
    p.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Folder containing f_vmpp.xlsx, f_ampp.xlsx, f_lookup.xlsx (defaults to script folder)",
    )
    args = p.parse_args()
    main(args.data_dir)
