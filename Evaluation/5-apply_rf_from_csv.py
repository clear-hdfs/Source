#!/usr/bin/env python3
import csv
import argparse
import subprocess
import sys
import os

def run_cmd(cmd):
    print("[cmd]", " ".join(cmd))
    res = subprocess.run(cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True)
    if res.returncode != 0:
        # print the error but continue with other files
        print("[err]", res.stderr.strip(), file=sys.stderr)
    return res.returncode

def main():
    ap = argparse.ArgumentParser(
        description="Apply HDFS replication factors from a CSV file_id,rf."
    )
    ap.add_argument("--csv", required=True,
                    help="CSV file with columns file_id,rf")
    ap.add_argument("--base", required=True,
                    help="Base HDFS directory, for example /exp/day2/data")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print commands without executing them")
    args = ap.parse_args()

    if not os.path.exists(args.csv):
        print(f"[err] CSV {args.csv} not found", file=sys.stderr)
        sys.exit(1)

    with open(args.csv, "r", newline="") as f:
        rd = csv.DictReader(f)
        if "file_id" not in rd.fieldnames or "rf" not in rd.fieldnames:
            print("[err] CSV must contain file_id and rf columns", file=sys.stderr)
            sys.exit(1)

        for row in rd:
            fid = row["file_id"].strip()
            if not fid:
                continue
            rf_str = row["rf"].strip()
            if not rf_str:
                continue
            try:
                rf = int(rf_str)
            except ValueError:
                print(f"[warn] invalid rf value for {fid} ({rf_str}), skipping line", file=sys.stderr)
                continue

            hdfs_path = f"{args.base}/{fid}"
            cmd = ["hdfs", "dfs", "-setrep", "-w", str(rf), hdfs_path]

            if args.dry_run:
                print("[dry-run]", " ".join(cmd))
            else:
                rc = run_cmd(cmd)
                if rc != 0:
                    print(f"[warn] setrep failed for {fid}", file=sys.stderr)

if __name__ == "__main__":
    main()
