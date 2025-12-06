#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import argparse
import statistics

CATEGORIES = ["Hot", "Shared", "Moderate", "Archival"]

# Replication bounds per category
R_MIN = {
    "Hot": 4,
    "Shared": 3,
    "Moderate": 2,
    "Archival": 0,   # 0 = EC only, 1 = 1 full replica + EC
}

R_MAX = {
    "Hot": 5,
    "Shared": 3,     # fixed RF for Shared
    "Moderate": 3,
    "Archival": 1,
}


def load_cluster_labels(path):
    """
    Read cluster_labels.csv.
    Expected columns: cluster,label,S_hot,S_shared,S_moderate,S_archival,s_star,phi_shared,n
    We only really need: cluster,label,s_star,n
    """
    clusters = []  # list of dicts
    with open(path, "r", newline="") as f:
        rd = csv.DictReader(f)
        required = {"cluster", "label", "s_star", "n"}
        missing = required - set(rd.fieldnames)
        if missing:
            raise ValueError(f"Missing columns in {path}: {missing}")
        for row in rd:
            cid = int(row["cluster"])
            label = row["label"].strip()
            if label not in CATEGORIES:
                raise ValueError(f"Unknown label '{label}' for cluster {cid}")
            try:
                s_star = float(row["s_star"])
            except ValueError:
                # fall back to 0 if s_star is missing or invalid
                s_star = 0.0
            n = int(row["n"])
            clusters.append({
                "cluster": cid,
                "label": label,
                "s_star": s_star,
                "n": n,
            })
    return clusters


def compute_med_mad_per_category(clusters):
    """
    For each category k, compute med_k and MAD_k based on s_star of clusters with label k.
    Returns two dicts: med_k, mad_k.
    If a category has no clusters, med_k and mad_k are set to None.
    """
    scores_by_cat = {k: [] for k in CATEGORIES}
    for c in clusters:
        scores_by_cat[c["label"]].append(c["s_star"])

    med = {}
    mad = {}
    for k in CATEGORIES:
        vals = scores_by_cat[k]
        if not vals:
            med[k] = None
            mad[k] = None
            continue
        m = statistics.median(vals)
        abs_dev = [abs(v - m) for v in vals]
        # If only one value, MAD will be 0
        mad_k = statistics.median(abs_dev)
        med[k] = m
        mad[k] = mad_k
    return med, mad


def assign_rf_for_clusters(clusters, med, mad):
    """
    Apply the RF policy per cluster using the strong fit rule.
    Returns a new list of dicts with added fields: R_i, strong_fit, med_k, mad_k.
    """
    out = []
    for c in clusters:
        k = c["label"]
        s_star = c["s_star"]
        rmin = R_MIN[k]
        rmax = R_MAX[k]
        med_k = med[k]
        mad_k = mad[k]

        if med_k is None:
            # No reference for this category, fall back to default R_min
            strong = False
        else:
            threshold = med_k + mad_k
            strong = (s_star >= threshold)

        if strong and rmin < rmax:
            rf = rmin + 1
        else:
            rf = rmin

        cc = dict(c)  # copy
        cc["R_min"] = rmin
        cc["R_max"] = rmax
        cc["R_i"] = rf
        cc["med_k"] = med_k
        cc["mad_k"] = mad_k
        cc["strong_fit"] = 1 if strong else 0
        out.append(cc)
    return out


def write_rf_csv(path, clusters_with_rf):
    """
    Write a CSV with cluster, label, s_star, med_k, mad_k, strong_fit, R_min, R_max, R_i, n.
    """
    fieldnames = [
        "cluster", "label", "s_star",
        "med_k", "mad_k", "strong_fit",
        "R_min", "R_max", "R_i",
        "n",
    ]
    with open(path, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=fieldnames)
        wr.writeheader()
        for c in sorted(clusters_with_rf, key=lambda x: x["cluster"]):
            row = {
                "cluster": c["cluster"],
                "label": c["label"],
                "s_star": f"{c['s_star']:.6f}",
                "med_k": "" if c["med_k"] is None else f"{c['med_k']:.6f}",
                "mad_k": "" if c["mad_k"] is None else f"{c['mad_k']:.6f}",
                "strong_fit": c["strong_fit"],
                "R_min": c["R_min"],
                "R_max": c["R_max"],
                "R_i": c["R_i"],
                "n": c["n"],
            }
            wr.writerow(row)


def main():
    ap = argparse.ArgumentParser(
        description="Assign integer replication factors per cluster based on category scores."
    )
    ap.add_argument("--labels", required=True,
                    help="Input cluster_labels CSV (e.g., day1_cluster_labels.csv)")
    ap.add_argument("--out", required=True,
                    help="Output CSV with RF per cluster (e.g., day1_cluster_rf.csv)")
    args = ap.parse_args()

    clusters = load_cluster_labels(args.labels)
    print(f"[rf] Loaded {len(clusters)} clusters from {args.labels}")

    med, mad = compute_med_mad_per_category(clusters)
    print("[rf] Per category med and MAD:")
    for k in CATEGORIES:
        print(f"  {k}: med={med[k]}, MAD={mad[k]}")

    clusters_with_rf = assign_rf_for_clusters(clusters, med, mad)

    # Small summary
    summary = {}
    for c in clusters_with_rf:
        k = c["label"]
        rf = c["R_i"]
        summary.setdefault(k, {})
        summary[k].setdefault(rf, 0)
        summary[k][rf] += 1

    print("[rf] RF distribution per category:")
    for k in CATEGORIES:
        if k not in summary:
            continue
        print(f"  {k}: ", end="")
        parts = [f"R={rf}: {cnt} clusters" for rf, cnt in sorted(summary[k].items())]
        print(", ".join(parts))

    write_rf_csv(args.out, clusters_with_rf)
    print(f"[rf] Written RF per cluster to {args.out}")


if __name__ == "__main__":
    main()
