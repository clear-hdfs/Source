#!/usr/bin/env python3
"""
erms_strategy.py

Elastic style replication strategy inspired by ERMS for HDFS.

This script reads either:
  - a raw Alibaba batch_instance_dayX.csv file, or
  - a precomputed features_dayX.csv file,

then derives a simple popularity metric per file and assigns a
replication factor based on popularity and age.

The goal is to compare this strategy with other policies such as CLEAR.
"""

import argparse
import csv
import math
import os
from collections import defaultdict
from typing import Dict, Any


def compute_quantile(values, q):
    """
    Compute the q quantile of a list of numeric values using linear interpolation.
    q must be in [0,1].
    """
    if not values:
        return 0.0

    if q <= 0.0:
        return min(values)
    if q >= 1.0:
        return max(values)

    vals = sorted(values)
    n = len(vals)
    pos = q * (n - 1)
    lower = int(math.floor(pos))
    upper = int(math.ceil(pos))

    if lower == upper:
        return vals[lower]

    frac = pos - lower
    return vals[lower] * (1.0 - frac) + vals[upper] * frac


def build_file_stats_from_batch(
    batch_path: str,
    w_start: float,
    w_end: float,
    id_col: str,
    start_col: str,
    end_col: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Build per file statistics from a batch_instance_dayX.csv file.

    We aggregate for each file id:
      - n_inst: number of instances in the window
      - total_dur: sum of instance durations inside the window
      - last_end: maximum end time for this file inside the window

    Popularity will be based on n_inst.
    Age will use w_end - last_end.
    """
    stats: Dict[str, Dict[str, Any]] = {}

    with open(batch_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        required = {id_col, start_col, end_col}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"Batch file {batch_path} is missing required columns: {missing}"
            )

        for row in reader:
            fid = row[id_col].strip()
            if not fid:
                continue

            try:
                start = float(row[start_col])
                end = float(row[end_col])
            except (ValueError, TypeError):
                continue

            # Ignore instances outside the window
            if end <= w_start or start >= w_end:
                continue

            # Clip to the window
            clipped_start = max(start, w_start)
            clipped_end = min(end, w_end)
            dur = max(0.0, clipped_end - clipped_start)

            s = stats.get(fid)
            if s is None:
                s = {
                    "n_inst": 0,
                    "total_dur": 0.0,
                    "last_end": w_start,
                }
                stats[fid] = s

            s["n_inst"] += 1
            s["total_dur"] += dur
            if end > s["last_end"]:
                s["last_end"] = end

    return stats


def build_file_stats_from_features(
    features_path: str,
    w_start: float,
    w_end: float,
    id_col: str = "dataset",
    freq_col: str = "freq",
    age_col: str = "age",
) -> Dict[str, Dict[str, Any]]:
    """
    Build per file statistics from a precomputed features_dayX.csv file.

    Expected columns by default:
      - dataset: file or job id
      - freq: number of accesses or instances in the window
      - age: time since last access at the end of the window (seconds)

    We map:
      n_inst   := freq
      total_dur := 0.0 (not used by the ERMS classification)
      last_end := w_end - age

    Popularity will be based on n_inst.
    Age will use w_end - last_end.
    """
    if w_end is None:
        raise ValueError("w_end must be provided when using features_dayX.csv")

    stats: Dict[str, Dict[str, Any]] = {}

    with open(features_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        required = {id_col, freq_col, age_col}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"Features file {features_path} is missing required columns: {missing}"
            )

        for row in reader:
            fid = row[id_col].strip()
            if not fid:
                continue

            try:
                freq = float(row[freq_col])
                age = float(row[age_col])
            except (ValueError, TypeError):
                continue

            if freq < 0:
                freq = 0.0
            if age < 0:
                age = 0.0

            n_inst = int(freq)
            last_end = float(w_end) - age

            stats[fid] = {
                "n_inst": n_inst,
                "total_dur": 0.0,
                "last_end": last_end,
            }

    return stats


def classify_erms(
    stats: Dict[str, Dict[str, Any]],
    w_start: float,
    w_end: float,
    q_hot: float,
    q_cold: float,
    age_frac_cold: float,
    rf_hot: int,
    rf_mid: int,
    rf_cold: int,
) -> Dict[str, Dict[str, Any]]:
    """
    Assign an ERMS like replication factor to each file based on:

      - popularity P_f  = n_inst (number of instances in the window)
      - relative age    = (w_end - last_end) / (w_end - w_start)

    Thresholds on popularity:
      - hot  files   have P_f >= quantile(P, q_hot)
      - cold files   have P_f <= quantile(P, q_cold) and age_frac >= age_frac_cold
      - mid  files   are the remaining ones

    Replication factors:
      - hot  -> rf_hot
      - mid  -> rf_mid
      - cold -> rf_cold
    """
    if not stats:
        return {}

    pop_values = [max(0.0, float(s.get("n_inst", 0.0))) for s in stats.values()]
    thr_hot = compute_quantile(pop_values, q_hot)
    thr_cold = compute_quantile(pop_values, q_cold)

    window_span = max(0.0, float(w_end) - float(w_start))

    result: Dict[str, Dict[str, Any]] = {}

    for fid, s in stats.items():
        n_inst = max(0.0, float(s.get("n_inst", 0.0)))
        last_end = float(s.get("last_end", w_start))

        age = max(0.0, float(w_end) - last_end)
        if window_span > 0.0:
            age_frac = max(0.0, min(1.0, age / window_span))
        else:
            age_frac = 0.0

        P_f = n_inst

        if P_f >= thr_hot:
            cls = "hot"
            rf = rf_hot
        elif P_f <= thr_cold and age_frac >= age_frac_cold:
            cls = "cold"
            rf = rf_cold
        else:
            cls = "mid"
            rf = rf_mid

        result[fid] = {
            "class": cls,
            "rf": int(rf),
            "P_f": P_f,
            "age": age,
            "age_frac": age_frac,
        }

    return result


def write_erms_policy_csv(
    out_path: str,
    assignments: Dict[str, Dict[str, Any]],
    day_tag: str,
    w_start: float,
    w_end: float,
):
    """
    Write a detailed CSV with one line per file:

      file_id, day, rf, class, P_f, age_seconds, age_frac, w_start, w_end
    """
    with open(out_path, "w", newline="") as f:
        fieldnames = [
            "file_id",
            "day",
            "rf",
            "class",
            "P_f",
            "age_seconds",
            "age_frac",
            "w_start",
            "w_end",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for fid, info in assignments.items():
            writer.writerow(
                {
                    "file_id": fid,
                    "day": day_tag,
                    "rf": info["rf"],
                    "class": info["class"],
                    "P_f": f"{info['P_f']:.6f}",
                    "age_seconds": f"{info['age']:.6f}",
                    "age_frac": f"{info['age_frac']:.6f}",
                    "w_start": f"{w_start:.3f}",
                    "w_end": f"{w_end:.3f}",
                }
            )


def write_erms_rf_csv(
    out_path: str,
    assignments: Dict[str, Dict[str, Any]],
):
    """
    Write a compact CSV that contains only:

      file_id, rf

    This can feed other parts of the evaluation pipeline.
    """
    with open(out_path, "w", newline="") as f:
        fieldnames = ["file_id", "rf"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for fid, info in assignments.items():
            writer.writerow(
                {
                    "file_id": fid,
                    "rf": info["rf"],
                }
            )


def write_erms_summary_txt(
    out_path: str,
    assignments: Dict[str, Dict[str, Any]],
    thr_hot: float,
    thr_cold: float,
    q_hot: float,
    q_cold: float,
    age_frac_cold: float,
    rf_hot: int,
    rf_mid: int,
    rf_cold: int,
):
    """
    Write a small human readable summary with counts per class and
    the thresholds used by the strategy.
    """
    total = len(assignments)
    counts = defaultdict(int)
    for info in assignments.values():
        counts[info["class"]] += 1

    with open(out_path, "w") as f:
        f.write("ERMS like replication summary\n")
        f.write(f"Total files: {total}\n\n")
        f.write("Class counts:\n")
        for cls in ["hot", "mid", "cold"]:
            f.write(f"  {cls:4s}: {counts.get(cls, 0)}\n")
        f.write("\n")

        f.write("Parameters:\n")
        f.write(f"  q_hot        = {q_hot}\n")
        f.write(f"  q_cold       = {q_cold}\n")
        f.write(f"  age_frac_cold= {age_frac_cold}\n")
        f.write(f"  rf_hot       = {rf_hot}\n")
        f.write(f"  rf_mid       = {rf_mid}\n")
        f.write(f"  rf_cold      = {rf_cold}\n")
        f.write("\n")

        f.write("Derived thresholds on popularity (P_f = n_inst):\n")
        f.write(f"  thr_hot  (q={q_hot})  = {thr_hot}\n")
        f.write(f"  thr_cold (q={q_cold}) = {thr_cold}\n")


def main():
    ap = argparse.ArgumentParser(
        description="Elastic style replication strategy inspired by ERMS for HDFS."
    )

    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--batch",
        help="Alibaba batch_instance_dayX.csv file",
    )
    group.add_argument(
        "--features",
        help="Precomputed features_dayX.csv file with at least dataset,freq,age columns",
    )

    ap.add_argument(
        "--out-dir",
        required=True,
        help="Directory where output CSV and summary will be written",
    )
    ap.add_argument(
        "--day",
        required=True,
        help="Tag for this window, for example day1, day2, or day3",
    )
    ap.add_argument(
        "--w-start",
        type=float,
        required=True,
        help="Window start time in seconds, inclusive",
    )
    ap.add_argument(
        "--w-end",
        type=float,
        required=True,
        help="Window end time in seconds, exclusive",
    )

    # Quantiles and age threshold
    ap.add_argument(
        "--q-hot",
        type=float,
        default=0.8,
        help="Quantile for hot files on popularity P_f (default 0.8).",
    )
    ap.add_argument(
        "--q-cold",
        type=float,
        default=0.2,
        help="Quantile for cold files on popularity P_f (default 0.2).",
    )
    ap.add_argument(
        "--age-frac-cold",
        type=float,
        default=0.5,
        help=(
            "Relative age threshold for cold files. "
            "A file is considered old if age_frac >= this value (default 0.5)."
        ),
    )

    # Replication factors
    ap.add_argument(
        "--rf-hot",
        type=int,
        default=4,
        help="Replication factor for hot files (default 4).",
    )
    ap.add_argument(
        "--rf-mid",
        type=int,
        default=3,
        help="Replication factor for mid popularity files (default 3).",
    )
    ap.add_argument(
        "--rf-cold",
        type=int,
        default=2,
        help="Replication factor for cold files (default 2).",
    )

    # Column configuration for batch and features files
    ap.add_argument(
        "--batch-id-col",
        default="job_name",
        help="Column name for file id in batch_instance CSV (default job_name).",
    )
    ap.add_argument(
        "--batch-start-col",
        default="start_time",
        help="Column name for interval start in batch_instance CSV (default start_time).",
    )
    ap.add_argument(
        "--batch-end-col",
        default="end_time",
        help="Column name for interval end in batch_instance CSV (default end_time).",
    )

    ap.add_argument(
        "--features-id-col",
        default="dataset",
        help="Column name for file id in features CSV (default dataset).",
    )
    ap.add_argument(
        "--features-freq-col",
        default="freq",
        help="Column name for popularity or frequency in features CSV (default freq).",
    )
    ap.add_argument(
        "--features-age-col",
        default="age",
        help="Column name for age in seconds in features CSV (default age).",
    )

    args = ap.parse_args()

    if args.w_end <= args.w_start:
        raise ValueError("w_end must be strictly greater than w_start")

    os.makedirs(args.out_dir, exist_ok=True)

    # Build per file stats
    if args.features is not None:
        print(f"[ERMS] Reading features file {args.features}")
        stats = build_file_stats_from_features(
            args.features,
            w_start=args.w_start,
            w_end=args.w_end,
            id_col=args.features_id_col,
            freq_col=args.features_freq_col,
            age_col=args.features_age_col,
        )
    else:
        print(f"[ERMS] Reading batch file {args.batch}")
        stats = build_file_stats_from_batch(
            args.batch,
            w_start=args.w_start,
            w_end=args.w_end,
            id_col=args.batch_id_col,
            start_col=args.batch_start_col,
            end_col=args.batch_end_col,
        )

    print(f"[ERMS] Found {len(stats)} files with activity in the window")

    if not stats:
        print("[ERMS] No files found, nothing to do")
        return

    # Classify and assign replication factors
    assignments = classify_erms(
        stats=stats,
        w_start=args.w_start,
        w_end=args.w_end,
        q_hot=args.q_hot,
        q_cold=args.q_cold,
        age_frac_cold=args.age_frac_cold,
        rf_hot=args.rf_hot,
        rf_mid=args.rf_mid,
        rf_cold=args.rf_cold,
    )

    # We need thresholds again for the summary
    pop_values = [max(0.0, float(s.get("n_inst", 0.0))) for s in stats.values()]
    thr_hot = compute_quantile(pop_values, args.q_hot)
    thr_cold = compute_quantile(pop_values, args.q_cold)

    # Write outputs
    policy_csv = os.path.join(args.out_dir, f"{args.day}_erms_policy.csv")
    rf_csv = os.path.join(args.out_dir, f"{args.day}_erms_rf.csv")
    summary_txt = os.path.join(args.out_dir, f"{args.day}_erms_summary.txt")

    write_erms_policy_csv(
        policy_csv,
        assignments=assignments,
        day_tag=args.day,
        w_start=args.w_start,
        w_end=args.w_end,
    )
    write_erms_rf_csv(
        rf_csv,
        assignments=assignments,
    )
    write_erms_summary_txt(
        summary_txt,
        assignments=assignments,
        thr_hot=thr_hot,
        thr_cold=thr_cold,
        q_hot=args.q_hot,
        q_cold=args.q_cold,
        age_frac_cold=args.age_frac_cold,
        rf_hot=args.rf_hot,
        rf_mid=args.rf_mid,
        rf_cold=args.rf_cold,
    )

    print(f"[ERMS] Wrote policy CSV   to {policy_csv}")
    print(f"[ERMS] Wrote RF CSV       to {rf_csv}")
    print(f"[ERMS] Wrote summary text to {summary_txt}")


if __name__ == "__main__":
    main()
