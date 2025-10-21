#!/usr/bin/env python3
# dm+d Importer (v2.2) — VMPP pack size pulled from VMPP sheet and merged by VPPID
import os, pandas as pd, json, re
from datetime import datetime

def log(msg): print(f"[{datetime.utcnow().isoformat()}Z] {msg}", flush=True)

# ---------- Helpers ----------
def read_vmpp(path):
    df_info = pd.read_excel(path, sheet_name="DtInfo")
    df_vmpp = pd.read_excel(path, sheet_name="VMPP")
    df_info.columns = [c.strip() for c in df_info.columns]
    df_vmpp.columns = [c.strip() for c in df_vmpp.columns]
    return df_info, df_vmpp

def read_ampp(path):
    df = pd.read_excel(path)  # first sheet
    df.columns = [c.strip() for c in df.columns]
    return df

def read_lookup(path):
    df = pd.read_excel(path, sheet_name="DtPayCat")
    df.columns = [c.strip() for c in df.columns]
    return df

def find_col(df, *cands, required=True):
    for n in cands:
        if n in df.columns:
            return n
    if required:
        raise KeyError(f"None of {cands} found in columns: {df.columns.tolist()}")
    return None

# ---------- Main ----------
def main():
    # File names (keep next to this script)
    f_vmpp = "f_vmpp.xlsx"
    f_ampp = "f_ampp.xlsx"
    f_lookup = "f_lookup.xlsx"

    info, vmpp_tab = read_vmpp(f_vmpp)
    ampp_raw      = read_ampp(f_ampp)
    lookup        = read_lookup(f_lookup)

    # Keys/fields in DtInfo (VMPP level)
    VPPID_info = find_col(info, "VPPID")
    PRICE_info = find_col(info, "PRICE")
    DT_info    = find_col(info, "DT", "PRICE_DT", "EFFECTIVE_DT")
    PAYCD_info = find_col(info, "PAY_CATCD", "PAYCATCD", "PAY_CAT_CD")

    # Pack size lives in VMPP tab → detect best column and merge by VPPID
    VPPID_vmpp = find_col(vmpp_tab, "VPPID", "VMPP", "VMPP_ID", "VPP_ID", "VMPPID")
    PACK_vmpp  = find_col(vmpp_tab, "QTVAL", "QTYVAL", "QTY", "PACK_SIZE", "QTY_VAL", "QTY VALUE", required=False)

    # Build base VMPP dataframe from DtInfo
    vmpp = pd.DataFrame({
        "VPPID":          info[VPPID_info],
        "dt_price":       pd.to_numeric(info[PRICE_info], errors="coerce"),
        "effective_date": pd.to_datetime(info[DT_info], errors="coerce").dt.date,
        "pay_catcd":      info[PAYCD_info].astype(str).str.strip()
    })
    # Attach pack size from VMPP tab (if found)
    if PACK_vmpp:
        vmpp = vmpp.merge(
            vmpp_tab[[VPPID_vmpp, PACK_vmpp]].rename(columns={VPPID_vmpp: "VPPID", PACK_vmpp: "dt_pack_size"}),
            on="VPPID", how="left"
        )
    else:
        vmpp["dt_pack_size"] = None

    vmpp = vmpp.rename(columns={"VPPID": "vmpp_id"})
    vmpp = vmpp.dropna(subset=["vmpp_id", "effective_date"]).copy()

    # Attach category name from lookup if present
    L_CODE = find_col(lookup, "PAY_CATCD", "PAYCATCD", "PAY_CAT_CD", "Code")
    L_NAME = find_col(lookup, "PAY_CATNM", "PAY_CAT", "Name", required=False)
    if L_NAME:
        lk = lookup[[L_CODE, L_NAME]].copy()
        lk[L_CODE] = lk[L_CODE].astype(str).str.strip()
        vmpp = vmpp.merge(lk, left_on="pay_catcd", right_on=L_CODE, how="left") \
                   .rename(columns={L_NAME: "pay_cat_name"}) \
                   .drop(columns=[L_CODE])
    else:
        vmpp["pay_cat_name"] = None

    # AMPP (price + ZD)
    APPID = find_col(ampp_raw, "APPID")
    APR   = find_col(ampp_raw, "PRICE")
    ADT   = find_col(ampp_raw, "PRICEDT", "PRICE_DT", "EFFECTIVE_DT")
    AZD   = find_col(ampp_raw, "ZERO_DISCD", "ZERO_DISCOUNT", required=False)

    ampp = pd.DataFrame({
        "ampp_id":       ampp_raw[APPID],
        "dt_price":      pd.to_numeric(ampp_raw[APR], errors="coerce"),
        "effective_date": pd.to_datetime(ampp_raw[ADT], errors="coerce").dt.date,
        "zero_discount": ampp_raw[AZD].astype(str).str.strip().isin(["0001", "0002"]) if AZD else False
    }).dropna(subset=["ampp_id", "effective_date"]).copy()

    # Emit dmd_items: VMPP rows (with dt_pack_size & PAY_CATCD) + AMPP rows (no pack/category)
    dmd_items_vmpp = vmpp.assign(vtm_id=None, ampp_id=None, dt_cat=vmpp["pay_catcd"]) \
                         [["vmpp_id", "ampp_id", "vtm_id", "dt_cat", "dt_price", "dt_pack_size", "effective_date"]]
    dmd_items_ampp = ampp.assign(vmpp_id=None, vtm_id=None, dt_cat=None, dt_pack_size=None) \
                         [["vmpp_id", "ampp_id", "vtm_id", "dt_cat", "dt_price", "dt_pack_size", "effective_date"]]
    dmd_items = pd.concat([dmd_items_vmpp, dmd_items_ampp], ignore_index=True)

    # Emit attributes: VMPP category names + AMPP ZD flags (kept separate for clarity)
    dmd_attr_vmpp = vmpp[["vmpp_id", "pay_catcd", "pay_cat_name", "effective_date"]].copy()
    dmd_attr_vmpp.insert(1, "level", "VMPP")
    dmd_attr_vmpp.insert(2, "zero_discount", False)
    dmd_attr_vmpp = dmd_attr_vmpp.rename(columns={"vmpp_id": "dmd_key"})

    dmd_attr_ampp = ampp[["ampp_id", "zero_discount", "effective_date"]].copy()
    dmd_attr_ampp.insert(1, "level", "AMPP")
    dmd_attr_ampp.insert(3, "pay_catcd", None)
    dmd_attr_ampp.insert(4, "pay_cat_name", None)
    dmd_attr_ampp = dmd_attr_ampp.rename(columns={"ampp_id": "dmd_key"})

    dmd_attributes = pd.concat([dmd_attr_vmpp, dmd_attr_ampp], ignore_index=True)

    # Save
    outdir = "out_dmd_v2"
    os.makedirs(outdir, exist_ok=True)
    dmd_items.to_csv(os.path.join(outdir, "dmd_items.csv"), index=False)
    dmd_attributes.to_csv(os.path.join(outdir, "dmd_attributes.csv"), index=False)
    with open(os.path.join(outdir, "manifest.json"), "w") as f:
        json.dump({
            "rows": {"dmd_items": len(dmd_items), "dmd_attributes": len(dmd_attributes)},
            "created_at_utc": datetime.utcnow().isoformat() + "Z",
            "notes": "Pack size merged from VMPP sheet via VPPID (columns tried: QTVAL/QTYVAL/QTY/PACK_SIZE/QTY_VAL/QTY VALUE)"
        }, f, indent=2)

    log("Done (dm+d v2.2).")

if __name__ == "__main__":
    main()
