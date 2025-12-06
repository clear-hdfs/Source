#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
drpmlc_strategy.py

DRPMLC-like replication strategy for HDFS.

This script can build per-file features in two ways:

  1) From a raw Alibaba batch_instance_dayX.csv file:
       --batch batch_instance_dayX.csv
     It approximates the DRPMLC preprocessing using start_time / end_time.

  2) From a precomputed features_dayX.csv file:
       --features features_dayX.csv
     It reuses aggregated metrics such as frequency and age to build
     DRPMLC style features:
       - access_count
       - adbad (approximate)
       - days_from_last
       - file_size_proxy

Then it runs KMeans to produce clusters that are mapped to classes:
  Cold, Warm, Hot

Finally it assigns replication factors for each class and writes:
  - <day>_drpmlc_rf.csv       (per file replication factor and features)
  - <day>_drpmlc_summary.csv  (per class statistics on RF)
"""

import csv
import argparse
import os
from collections import defaultdict
import math

try:
    from sklearn.cluster import KMeans
except ImportError:
    KMeans = None


###############################################################################
# Step 1: build DRPMLC-like features from batch_instance_dayX.csv
###############################################################################

def build_file_features_from_batch(batch_path, w_start=None, w_end=None):
    """
    Approximate DRPMLC preprocessing from batch_instance_dayX.csv.

    For each file (job_name), compute:
      - access_count: number of accesses in the window
      - adbad: average difference between consecutive access times
               (using start_time as access timestamps)
      - days_from_last: time since last access until window end
      - file_size_proxy: total runtime as a surrogate for file size
    """
    stats = {}

    with open(batch_path, "r", newline="") as f:
        rd = csv.DictReader(f)
        required = {"job_name", "start_time", "end_time"}
        missing = required - set(rd.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns in {batch_path}: {missing}")

        for row in rd:
            fid = row["job_name"].strip()
            if not fid:
                continue

            try:
                start = float(row["start_time"])
                end = float(row["end_time"])
            except ValueError:
                continue

            if not (end > start):
                continue

            # Window filtering
            if w_start is not None and end <= w_start:
                continue
            if w_end is not None and start >= w_end:
                continue

            # Clip to window
            if w_start is not None and start < w_start:
                start = w_start
            if w_end is not None and end > w_end:
                end = w_end
            if not (end > start):
                continue

            dur = end - start

            st = stats.get(fid)
            if st is None:
                st = {
                    "access_times": [],
                    "access_count": 0,
                    "total_dur": 0.0,
                    "first_access": start,
                    "last_access": start,
                }
                stats[fid] = st

            st["access_times"].append(start)
            st["access_count"] += 1
            st["total_dur"] += dur
            if start < st["first_access"]:
                st["first_access"] = start
            if start > st["last_access"]:
                st["last_access"] = start

    # Second pass: compute ADBAD and DaysFromLastAccess
    features = {}
    for fid, st in stats.items():
        ats = sorted(st["access_times"])
        n = len(ats)

        if n <= 1:
            # If only one access, approximate ADBAD with time since first access
            if w_end is not None:
                adbad = max(0.0, w_end - ats[0])
            else:
                adbad = 0.0
        else:
            diffs = [ats[i] - ats[i - 1] for i in range(1, n)]
            diffs = [d for d in diffs if d >= 0.0]
            if diffs:
                adbad = sum(diffs) / float(len(diffs))
            else:
                adbad = 0.0

        if w_end is not None:
            days_from_last = max(0.0, w_end - st["last_access"])
        elif w_start is not None:
            days_from_last = max(0.0, st["last_access"] - w_start)
        else:
            days_from_last = 0.0

        features[fid] = {
            "access_count": st["access_count"],
            "adbad": adbad,
            "days_from_last": days_from_last,
            "file_size_proxy": st["total_dur"],
        }

    return features


###############################################################################
# Step 1 bis: build DRPMLC-like features from features_dayX.csv
###############################################################################

def build_file_features_from_features(
    features_path,
    id_col="dataset",
    freq_col="freq",
    age_col="age",
    size_col=None,
):
    """
    Build DRPMLC-style features from a precomputed features_dayX.csv file.

    Required columns:
      - id_col: file or job identifier (default: dataset)
      - freq_col: access count in the window (default: freq)
      - age_col: time since last access at end of window in seconds (default: age)

    Optional:
      - size_col: column used as proxy for file size or resource usage
                  (for example cpuRatio). If not provided, we reuse access_count
                  as the file_size_proxy.

    For each file, we build:
      - access_count: from freq_col
      - days_from_last: from age_col
      - adbad: approximate average distance between accesses
               using days_from_last and access_count
      - file_size_proxy: from size_col if available, else access_count
    """
    features = {}

    with open(features_path, "r", newline="") as f:
        rd = csv.DictReader(f)
        required = {id_col, freq_col, age_col}
        missing = required - set(rd.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns in {features_path}: {missing}")

        for row in rd:
            fid = row[id_col].strip()
            if not fid:
                continue

            try:
                freq = float(row[freq_col])
            except (ValueError, TypeError):
                freq = 0.0

            try:
                age_val = float(row[age_col])
            except (ValueError, TypeError):
                age_val = 0.0

            if freq < 0.0:
                freq = 0.0
            if age_val < 0.0:
                age_val = 0.0

            access_count = int(freq)
            days_from_last = age_val

            # Simple approximation for ADBAD when only aggregated features are available
            if access_count <= 1:
                adbad = days_from_last
            else:
                adbad = days_from_last / float(access_count - 1)

            # File size proxy
            file_size_proxy = float(access_count)
            if size_col is not None and size_col in rd.fieldnames:
                val = row.get(size_col, "")
                if val not in (None, ""):
                    try:
                        file_size_proxy = float(val)
                    except (ValueError, TypeError):
                        pass

            features[fid] = {
                "access_count": access_count,
                "adbad": adbad,
                "days_from_last": days_from_last,
                "file_size_proxy": file_size_proxy,
            }

    return features


###############################################################################
# Step 2: build feature matrix for KMeans with simple standardization
###############################################################################

def standardize_feature(values):
    """
    Returns normalized list (x - mean) / std.
    If std is 0, returns zeros.
    """
    n = len(values)
    if n == 0:
        return [], 0.0, 0.0

    mean = sum(values) / float(n)
    var = sum((v - mean) ** 2 for v in values) / float(n)
    std = math.sqrt(var)

    if std <= 0.0:
        return [0.0 for _ in values], mean, std

    return [(v - mean) / std for v in values], mean, std


def build_feature_matrix(file_features):
    """
    Build X matrix for KMeans and mapping index -> file_id.

    Features per file_id:
      1) access_count
      2) adbad
      3) days_from_last
      4) file_size_proxy
    """
    file_ids = sorted(file_features.keys())
    acc = [file_features[fid]["access_count"] for fid in file_ids]
    adb = [file_features[fid]["adbad"] for fid in file_ids]
    dfl = [file_features[fid]["days_from_last"] for fid in file_ids]
    fsp = [file_features[fid]["file_size_proxy"] for fid in file_ids]

    # log1p to reduce strong skewness
    acc_log = [math.log1p(max(0.0, v)) for v in acc]
    adb_log = [math.log1p(max(0.0, v)) for v in adb]
    dfl_log = [math.log1p(max(0.0, v)) for v in dfl]
    fsp_log = [math.log1p(max(0.0, v)) for v in fsp]

    acc_z, _, _ = standardize_feature(acc_log)
    adb_z, _, _ = standardize_feature(adb_log)
    dfl_z, _, _ = standardize_feature(dfl_log)
    fsp_z, _, _ = standardize_feature(fsp_log)

    X = []
    for i in range(len(file_ids)):
        X.append([acc_z[i], adb_z[i], dfl_z[i], fsp_z[i]])

    return file_ids, X


###############################################################################
# Step 3: KMeans clustering and mapping clusters to Hot Warm Cold
###############################################################################

def run_kmeans_clustering(file_features, k=3, random_state=42, max_iter=300):
    """
    Run KMeans on the feature matrix and map clusters to classes.

    Clusters are ordered by average access_count:
      smallest -> Cold, middle -> Warm, largest -> Hot.
    """
    if KMeans is None:
        raise ImportError("scikit-learn is required for DRPMLC (KMeans).")

    file_ids, X = build_feature_matrix(file_features)
    if not X:
        return {}

    k = max(1, min(k, len(file_ids)))

    km = KMeans(
        n_clusters=k,
        random_state=random_state,
        n_init=10,
        max_iter=max_iter,
    )
    labels = km.fit_predict(X)

    # Per cluster average access_count
    cluster_stats = defaultdict(list)
    for fid, lab in zip(file_ids, labels):
        cluster_stats[lab].append(file_features[fid]["access_count"])

    # Smallest access_count -> Cold, middle -> Warm, largest -> Hot
    cluster_order = sorted(
        cluster_stats.keys(),
        key=lambda c: (
            sum(cluster_stats[c]) / float(len(cluster_stats[c]))
            if cluster_stats[c]
            else 0.0
        ),
    )

    cluster_to_class = {}
    if len(cluster_order) == 1:
        cluster_to_class[cluster_order[0]] = "Hot"
    elif len(cluster_order) == 2:
        cluster_to_class[cluster_order[0]] = "Cold"
        cluster_to_class[cluster_order[1]] = "Hot"
    else:
        cluster_to_class[cluster_order[0]] = "Cold"
        cluster_to_class[cluster_order[1]] = "Warm"
        cluster_to_class[cluster_order[2]] = "Hot"

    result = {}
    for fid, lab in zip(file_ids, labels):
        cls = cluster_to_class.get(lab, "Warm")
        result[fid] = {
            "cluster": int(lab),
            "class": cls,
        }

    return result


###############################################################################
# Step 4: assign replication factors as in DRPMLC
###############################################################################

def assign_rf_drpmlc(file_features, clusters, rf_hot=3, rf_warm=2, rf_cold=1):
    """
    In DRPMLC:
      Hot  -> 3x replication
      Warm -> 2x replication
      Cold -> EC RS(6,3) with 50 percent overhead in the original paper.

    Here we keep:
      rf_hot, rf_warm, rf_cold
    and treat rf_cold as the single replica count before EC in your model.
    """
    out = {}
    for fid, feat in file_features.items():
        cinfo = clusters.get(fid)
        if cinfo is None:
            cls = "Warm"
            lab = -1
        else:
            cls = cinfo["class"]
            lab = cinfo["cluster"]

        if cls == "Hot":
            rf = rf_hot
        elif cls == "Warm":
            rf = rf_warm
        else:
            rf = rf_cold

        out[fid] = {
            "cluster": lab,
            "class": cls,
            "rf": int(rf),
            "access_count": feat["access_count"],
            "adbad": feat["adbad"],
            "days_from_last": feat["days_from_last"],
            "file_size_proxy": feat["file_size_proxy"],
        }

    return out


###############################################################################
# Output writers
###############################################################################

def write_drpmlc_rf(out_rf_path, drp_result):
    """
    Write one CSV line per file with its cluster, class, RF and features.
    """
    with open(out_rf_path, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(
            [
                "file_id",
                "cluster",
                "class",
                "rf",
                "access_count",
                "adbad",
                "days_from_last",
                "file_size_proxy",
            ]
        )
        for fid in sorted(drp_result.keys()):
            st = drp_result[fid]
            wr.writerow(
                [
                    fid,
                    st["cluster"],
                    st["class"],
                    st["rf"],
                    st["access_count"],
                    f"{st['adbad']:.6f}",
                    f"{st['days_from_last']:.6f}",
                    f"{st['file_size_proxy']:.6f}",
                ]
            )


def write_drpmlc_summary(out_summary_path, drp_result):
    """
    Write a small summary per class with basic statistics on RF.
    """
    by_class = defaultdict(list)
    for fid, st in drp_result.items():
        by_class[st["class"]].append(st["rf"])

    with open(out_summary_path, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["class", "count", "rf_min", "rf_max", "rf_mean"])
        for cls in sorted(by_class.keys()):
            rfs = by_class[cls]
            if not rfs:
                continue
            rf_min = min(rfs)
            rf_max = max(rfs)
            rf_mean = sum(rfs) / float(len(rfs))
            wr.writerow([cls, len(rfs), rf_min, rf_max, f"{rf_mean:.3f}"])


###############################################################################
# Main
###############################################################################

def main():
    ap = argparse.ArgumentParser(
        description=(
            "DRPMLC-like replication strategy from batch_instance_dayX.csv "
            "or features_dayX.csv."
        )
    )

    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--batch",
        help="batch_instance_dayX.csv input file",
    )
    group.add_argument(
        "--features",
        help="precomputed features_dayX.csv input file",
    )

    ap.add_argument(
        "--out-dir",
        required=True,
        help="Output directory for RF and summary files",
    )
    ap.add_argument(
        "--day",
        required=True,
        help="Tag for this window (for example day1, day2, day3)",
    )
    ap.add_argument(
        "--w-start",
        type=float,
        default=None,
        help="Window start time (seconds, inclusive) when using batch input",
    )
    ap.add_argument(
        "--w-end",
        type=float,
        default=None,
        help="Window end time (seconds, exclusive) when using batch input",
    )

    ap.add_argument(
        "--k",
        type=int,
        default=3,
        help="Number of clusters for KMeans (default 3)",
    )
    ap.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for KMeans (default 42)",
    )

    ap.add_argument(
        "--rf-hot",
        type=int,
        default=3,
        help="Replication factor for Hot files (default 3)",
    )
    ap.add_argument(
        "--rf-warm",
        type=int,
        default=2,
        help="Replication factor for Warm files (default 2)",
    )
    ap.add_argument(
        "--rf-cold",
        type=int,
        default=1,
        help="Replication factor for Cold files (default 1)",
    )

    # Column configuration for features_dayX.csv
    ap.add_argument(
        "--features-id-col",
        default="dataset",
        help="Column name for file id in features CSV (default dataset)",
    )
    ap.add_argument(
        "--features-freq-col",
        default="freq",
        help="Column name for frequency in features CSV (default freq)",
    )
    ap.add_argument(
        "--features-age-col",
        default="age",
        help="Column name for age in seconds in features CSV (default age)",
    )
    ap.add_argument(
        "--features-size-col",
        default=None,
        help=(
            "Optional column name used as file size proxy in features CSV "
            "(for example cpuRatio). If not set, access_count is used."
        ),
    )

    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    # Build per file features
    if args.features is not None:
        print(f"[DRPMLC] Reading features file {args.features}")
        feats = build_file_features_from_features(
            args.features,
            id_col=args.features_id_col,
            freq_col=args.features_freq_col,
            age_col=args.features_age_col,
            size_col=args.features_size_col,
        )
    else:
        print(f"[DRPMLC] Reading batch file {args.batch}")
        feats = build_file_features_from_batch(
            args.batch,
            w_start=args.w_start,
            w_end=args.w_end,
        )

    print(f"[DRPMLC] Built features for {len(feats)} files")

    if not feats:
        print("[DRPMLC] No features found, nothing to do")
        return

    clusters = run_kmeans_clustering(
        feats,
        k=args.k,
        random_state=args.seed,
    )
    print(f"[DRPMLC] Clustered {len(clusters)} files")

    drp_result = assign_rf_drpmlc(
        feats,
        clusters,
        rf_hot=args.rf_hot,
        rf_warm=args.rf_warm,
        rf_cold=args.rf_cold,
    )

    out_rf_path = os.path.join(args.out_dir, f"{args.day}_drpmlc_rf.csv")
    out_summary_path = os.path.join(args.out_dir, f"{args.day}_drpmlc_summary.csv")

    write_drpmlc_rf(out_rf_path, drp_result)
    write_drpmlc_summary(out_summary_path, drp_result)

    print(f"[DRPMLC] RF per file written to {out_rf_path}")
    print(f"[DRPMLC] Summary written to {out_summary_path}")


if __name__ == "__main__":
    main()
