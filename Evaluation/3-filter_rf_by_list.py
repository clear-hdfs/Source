#!/usr/bin/env python3
import csv
import argparse

def main():
    ap = argparse.ArgumentParser(
        description="Filter an RF CSV using a list of file_id values."
    )
    ap.add_argument("--top", required=True,
                    help="Text file with one file_id per line (top10000_day2.txt)")
    ap.add_argument("--rf", required=True,
                    help="Input CSV containing at least the columns file_id and rf (or equivalents).")
    ap.add_argument("--col-id", required=True,
                    help="Name of the column that contains file_id in the source CSV.")
    ap.add_argument("--col-rf", required=True,
                    help="Name of the column that contains the RF in the source CSV.")
    ap.add_argument("--out", required=True,
                    help="Output CSV with columns file_id,rf.")
    args = ap.parse_args()

    # load the list
    with open(args.top, "r") as f:
        wanted = {line.strip() for line in f if line.strip()}

    print(f"[info] {len(wanted)} file_id entries in {args.top}")

    with open(args.rf, "r", newline="") as f_in:
        rd = csv.DictReader(f_in)
        if args.col_id not in rd.fieldnames:
            raise SystemExit(f"Column {args.col_id} is missing in {args.rf}")
        if args.col_rf not in rd.fieldnames:
            raise SystemExit(f"Column {args.col_rf} is missing in {args.rf}")

        rows = []
        for row in rd:
            fid = row[args.col_id].strip()
            if not fid or fid not in wanted:
                continue
            rf_str = row[args.col_rf].strip()
            if not rf_str:
                continue
            rows.append((fid, rf_str))

    rows.sort(key=lambda x: x[0])

    with open(args.out, "w", newline="") as f_out:
        wr = csv.writer(f_out)
        wr.writerow(["file_id", "rf"])
        for fid, rf_str in rows:
            wr.writerow([fid, rf_str])

    print(f"[info] Wrote {args.out} with {len(rows)} rows")

if __name__ == "__main__":
    main()
