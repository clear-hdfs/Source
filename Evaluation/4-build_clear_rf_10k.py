#!/usr/bin/env python3
import csv
import argparse

def main():
    ap = argparse.ArgumentParser(
        description="Build a file_id,rf CSV for CLEAR from per cluster RF values."
    )
    ap.add_argument("--top", required=True,
                    help="List of file_id values (top10000_day2.txt)")
    ap.add_argument("--labels", required=True,
                    help="day2_cluster_labels.csv (file -> cluster)")
    ap.add_argument("--clusters", required=True,
                    help="day2_cluster_rf.csv (cluster -> rf)")
    # column names (adapt to your files)
    ap.add_argument("--col-file", default="dataset",
                    help="Name of the file_id column in labels (default: dataset)")
    ap.add_argument("--col-cluster-label", default="cluster",
                    help="Name of the cluster column in labels (default: cluster)")
    ap.add_argument("--col-cluster-rf", default="cluster",
                    help="Name of the cluster column in clusters (default: cluster)")
    ap.add_argument("--col-rf", default="rf",
                    help="Name of the rf column in clusters (default: rf)")
    ap.add_argument("--out", required=True,
                    help="Output CSV file_id,rf (e.g. rf_10k/day2_clear_rf_10k.csv)")
    args = ap.parse_args()

    # 1) Load the list of top files
    with open(args.top, "r") as f:
        wanted = {line.strip() for line in f if line.strip()}
    print(f"[info] {len(wanted)} file_id entries in {args.top}")

    # 2) Load RF per cluster
    cluster_rf = {}
    with open(args.clusters, "r", newline="") as f:
        rd = csv.DictReader(f)
        if args.col_cluster_rf not in rd.fieldnames:
            raise SystemExit(f"Column {args.col_cluster_rf} is missing from {args.clusters}")
        if args.col_rf not in rd.fieldnames:
            raise SystemExit(f"Column {args.col_rf} is missing from {args.clusters}")
        for row in rd:
            cid = row[args.col_cluster_rf].strip()
            if not cid:
                continue
            rf_str = row[args.col_rf].strip()
            if not rf_str:
                continue
            cluster_rf[cid] = rf_str
    print(f"[info] Loaded RF for {len(cluster_rf)} clusters from {args.clusters}")

    # 3) Map each file to its cluster
    file_cluster = {}
    with open(args.labels, "r", newline="") as f:
        rd = csv.DictReader(f)
        if args.col_file not in rd.fieldnames:
            raise SystemExit(f"Column {args.col_file} is missing from {args.labels}")
        if args.col_cluster_label not in rd.fieldnames:
            raise SystemExit(f"Column {args.col_cluster_label} is missing from {args.labels}")
        for row in rd:
            fid = row[args.col_file].strip()
            if not fid or fid not in wanted:
                continue
            cid = row[args.col_cluster_label].strip()
            if not cid:
                continue
            file_cluster[fid] = cid
    print(f"[info] Top files found with a cluster: {len(file_cluster)}")

    # 4) Build file_id -> rf by applying the cluster RF
    rows_out = []
    missing_cluster = 0
    missing_rf = 0
    for fid in sorted(wanted):
        cid = file_cluster.get(fid)
        if cid is None:
            missing_cluster += 1
            continue
        rf_str = cluster_rf.get(cid)
        if rf_str is None:
            missing_rf += 1
            continue
        rows_out.append((fid, rf_str))

    print(f"[info] Files with final RF: {len(rows_out)}")
    if missing_cluster:
        print(f"[warn] {missing_cluster} files do not have a cluster in {args.labels}")
    if missing_rf:
        print(f"[warn] {missing_rf} clusters do not have an RF in {args.clusters}")

    # 5) Write the CSV file_id,rf
    with open(args.out, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["file_id", "rf"])
        for fid, rf_str in rows_out:
            wr.writerow([fid, rf_str])

    print(f"[info] Wrote {args.out} with {len(rows_out)} rows")

if __name__ == "__main__":
    main()
