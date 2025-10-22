#!/usr/bin/env python3
"""dm+d Name Loader utilities."""

import argparse
import json
import os
import sys
from datetime import datetime

import pandas as pd


def pick_col(columns, options):
    for column in options:
        if column in columns:
            return column
    return None


def load_vmpp_names(path):
    header = pd.read_excel(path, sheet_name="VMPP", nrows=0)
    header.columns = header.columns.str.strip()
    id_col = pick_col(header.columns, ["VPPID"])
    name_col = pick_col(header.columns, ["NM", "DESC", "NAME", "DESCR", "VMPP_NAME", "VPPSNM"])
    if not id_col or not name_col:
        print(f"[warning] Could not find VMPP columns in {path}", file=sys.stderr)
        return pd.DataFrame(columns=["vmpp_id", "dm_name"])
    df = pd.read_excel(
        path,
        sheet_name="VMPP",
        usecols=[id_col, name_col],
        dtype=str,
    )
    df.columns = df.columns.str.strip()
    out = df.rename(columns={id_col: "vmpp_id", name_col: "dm_name"})
    out["vmpp_id"] = out["vmpp_id"].astype(str).str.strip()
    out["dm_name"] = out["dm_name"].astype(str).str.strip()
    out = out.dropna(subset=["vmpp_id", "dm_name"]).drop_duplicates()
    return out


def load_ampp_names(path):
    with pd.ExcelFile(path) as xl:
        sheet = xl.sheet_names[0]
        header = xl.parse(sheet_name=sheet, nrows=0)
        header.columns = header.columns.str.strip()
        id_col = pick_col(header.columns, ["APPID"])
        name_col = pick_col(header.columns, ["NM", "DESC", "NAME", "DESCR", "AMPP_NAME", "AMPSNM"])
        if not id_col or not name_col:
            print(f"[warning] Could not find AMPP columns in {path}", file=sys.stderr)
            return pd.DataFrame(columns=["ampp_id", "dm_name"])
        df = xl.parse(
            sheet_name=sheet,
            usecols=[id_col, name_col],
            dtype=str,
        )
    df.columns = df.columns.str.strip()
    out = df.rename(columns={id_col: "ampp_id", name_col: "dm_name"})
    out["ampp_id"] = out["ampp_id"].astype(str).str.strip()
    out["dm_name"] = out["dm_name"].astype(str).str.strip()
    out = out.dropna(subset=["ampp_id", "dm_name"]).drop_duplicates()
    return out


def parse_args():
    parser = argparse.ArgumentParser(description="Extract dm+d names for VMPP/AMPP records")
    parser.add_argument("--vmpp", default="f_vmpp.xlsx", help="Path to the VMPP extract (default: %(default)s)")
    parser.add_argument("--ampp", default="f_ampp.xlsx", help="Path to the AMPP extract (default: %(default)s)")
    parser.add_argument("--out-dir", default="out_dmd_v2", help="Directory to write CSV + manifest (default: %(default)s)")
    return parser.parse_args()


def main():
    args = parse_args()
    vmpp = load_vmpp_names(args.vmpp)
    ampp = load_ampp_names(args.ampp)

    vmpp["ampp_id"] = ""
    ampp["vmpp_id"] = ""
    names = pd.concat(
        [
            vmpp[["vmpp_id", "ampp_id", "dm_name"]],
            ampp[["vmpp_id", "ampp_id", "dm_name"]],
        ],
        ignore_index=True,
    )
    os.makedirs(args.out_dir, exist_ok=True)
    names.to_csv(os.path.join(args.out_dir, "dmd_names.csv"), index=False)

    manifest = {
        "rows": len(names),
        "vmpp_rows": len(vmpp),
        "ampp_rows": len(ampp),
        "created_at_utc": datetime.utcnow().isoformat() + "Z",
        "vmpp_source": os.path.abspath(args.vmpp),
        "ampp_source": os.path.abspath(args.ampp),
    }
    manifest_path = os.path.join(args.out_dir, "dmd_names_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote {os.path.join(args.out_dir, 'dmd_names.csv')} ({len(names)} rows)")


if __name__ == "__main__":
    main()
