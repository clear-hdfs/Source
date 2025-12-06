#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
X-Means++ (split only, spherical BIC) + labelling consistent with the scoring formalism.
Input: normalized CSV with header: dataset,freq,conc,cpuRatio,age,locality
Outputs:
  - dayX_clusters.csv          (dataset,cluster)
  - dayX_centroids.csv         (cluster, mean_*, n)
  - dayX_cluster_labels.csv    (cluster,label,S_*,s_star,phi_shared,n)
  - dayX_cluster_debug.csv     (M, Delta, z per feature + scores + label)
    Note: M_* = mixed center = 0.5*(median + mean) for the cluster
  - dayX_cluster_summary.txt   (count per category)

Important note:
The scores S_hot, S_shared, S_moderate, S_archival are first computed
according to the formalism, then normalized per category (division by the
global maximum of each category over all clusters) before choosing the label.
"""

import os
import sys
import csv
import math
import argparse
import random
from collections import defaultdict

try:
    import numpy as np
except ImportError:
    np = None
    print("[warn] numpy not available. This script requires numpy for efficient X-Means.", file=sys.stderr)

# Expected columns in the normalized CSV
COLUMNS = ["dataset", "freq", "conc", "cpuRatio", "age", "locality"]
# Working order of features
FEATURES = ["freq", "conc", "cpuRatio", "age", "locality"]
# Mapping wRatio -> cpuRatio (depends on dataset)
WRATIO_ALIAS = "cpuRatio"


# --------------------------
# I/O
# --------------------------
def read_normalized_data(path, limit=None, seed=42):
    """
    Read normalized CSV with header: dataset,freq,conc,cpuRatio,age,locality
    Returns: ids (list of str), X (np.ndarray N x d)
    """
    if np is None:
        raise RuntimeError("numpy is required for this script.")

    ids = []
    rows = []
    with open(path, "r", newline="") as f:
        rd = csv.DictReader(f)
        # Header validation
        for col in COLUMNS:
            if col not in rd.fieldnames:
                raise ValueError(f"[load_normalized_data] Missing column: {col} in {path}")
        cnt = 0
        for r in rd:
            try:
                vec = [
                    float(r["freq"]),
                    float(r["conc"]),
                    float(r["cpuRatio"]),
                    float(r["age"]),
                    float(r["locality"]),
                ]
            except Exception as e:
                raise ValueError(f"[load_normalized_data] Invalid row: {r}") from e
            ids.append(r["dataset"])
            rows.append(vec)
            cnt += 1
            if limit is not None and cnt >= limit:
                break
    X = np.asarray(rows, dtype=np.float64)
    return ids, X


# --------------------------
# K-means / X-means
# --------------------------
def kmeans_plus_plus_init(X, K, rng):
    """
    k-means++ initialization on X (N x d), returns centers (K x d)
    """
    N, d = X.shape
    centers = np.empty((K, d), dtype=np.float64)
    # first center at random
    i0 = rng.randrange(N)
    centers[0] = X[i0]
    # squared distances to current centers
    dist2 = np.sum((X - centers[0])**2, axis=1)

    for k in range(1, K):
        s = dist2.sum()
        if s <= 0:
            centers[k] = X[rng.randrange(N)]
            continue
        probs = dist2 / s
        r = rng.random()
        cdf = np.cumsum(probs)
        idx = np.searchsorted(cdf, r, side="right")
        if idx >= N:
            idx = N - 1
        centers[k] = X[idx]
        # update dist2 = min(dist2, ||x - new_center||^2)
        d2_new = np.sum((X - centers[k])**2, axis=1)
        dist2 = np.minimum(dist2, d2_new)
    return centers


def kmeans_lloyd(X, K, max_iter, rng, init_centers=None):
    """
    K-means Lloyd on X, returns (labels, centers, inertia)
    """
    N, d = X.shape
    if init_centers is None:
        centers = kmeans_plus_plus_init(X, K, rng)
    else:
        centers = init_centers.copy()

    labels = np.full(N, -1, dtype=np.int32)
    for _ in range(max_iter):
        # distances to centers
        dist = np.empty((N, K), dtype=np.float64)
        for k in range(K):
            diff = X - centers[k]
            dist[:, k] = np.einsum("ij,ij->i", diff, diff)
        new_labels = np.argmin(dist, axis=1)
        if np.array_equal(new_labels, labels):
            break
        labels = new_labels
        # recompute centers
        for k in range(K):
            mask = (labels == k)
            if not np.any(mask):
                centers[k] = X[rng.randrange(N)]
            else:
                centers[k] = X[mask].mean(axis=0)

    inertia = 0.0
    for k in range(K):
        mask = (labels == k)
        if np.any(mask):
            diff = X[mask] - centers[k]
            inertia += float(np.einsum("ij,ij->", diff, diff))
    return labels, centers, inertia


def bic_spherical_cluster(Xc, sigma_floor=1e-12):
    """
    BIC of one cluster under a spherical Gaussian.
    Xc: points in the cluster (n x d)
    """
    n, d = Xc.shape
    if n <= 0:
        return -np.inf
    mu = Xc.mean(axis=0)
    diff = Xc - mu
    S = float(np.einsum("ij,ij->", diff, diff))
    denom = max(n * d, 1)
    sigma2 = S / denom
    if sigma2 <= 0:
        sigma2 = sigma_floor
    ell = -0.5 * n * d * (math.log(2.0 * math.pi * sigma2) + 1.0)
    nu = d + 1
    bic = ell - 0.5 * nu * math.log(max(n, 1))
    return bic


def xmeans_split_once(X, labels, K, Kmax, tau, m_min, max_iter_km, rng):
    """
    Iterate over each cluster and propose a local K=2 split, compare BIC.
    Respects Kmax strictly through a quota per pass.
    """
    N, d = X.shape
    improved = False
    labels_new = labels.copy()
    current_K = K
    remaining = max(0, Kmax - K)  # quota of accepted splits in this pass

    for parent_k in range(K):
        if remaining <= 0:
            break

        mask = (labels_new == parent_k)
        idxs = np.where(mask)[0]
        n_parent = idxs.size
        if n_parent < 2 * m_min:
            continue

        Xp = X[idxs]
        bic_parent = bic_spherical_cluster(Xp)

        # local split k=2
        centers2 = kmeans_plus_plus_init(Xp, 2, rng)
        lab2, _, _ = kmeans_lloyd(Xp, 2, max_iter_km, rng, init_centers=centers2)

        n1 = int(np.sum(lab2 == 0))
        n2 = int(np.sum(lab2 == 1))
        if n1 < m_min or n2 < m_min:
            continue

        bic_child = bic_spherical_cluster(Xp[lab2 == 0]) + bic_spherical_cluster(Xp[lab2 == 1])

        if bic_child > bic_parent + tau:
            improved = True
            child2_label = current_K
            child1_label = parent_k
            labels_new[idxs[lab2 == 0]] = child1_label
            labels_new[idxs[lab2 == 1]] = child2_label
            current_K += 1
            remaining -= 1  # consumes one split
            if remaining <= 0:
                break

    return improved, labels_new, current_K


def xmeans(X, Kmin=4, Kmax=8, tau=0.0, m_min=20, max_iter_km=50, seed=42):
    """
    X-Means split only:
      - Init k-means++ with Kmin
      - Splits guided by BIC + margin tau and m_min
      - Stop if no split is accepted or K reaches Kmax
    """
    rng = random.Random(seed)

    labels, centers, _ = kmeans_lloyd(X, Kmin, max_iter_km, rng)
    K = Kmin

    while K < Kmax:
        improved, new_labels, K_new = xmeans_split_once(
            X, labels, K, Kmax, tau, m_min, max_iter_km, rng
        )
        labels = new_labels
        K = K_new
        if not improved or K >= Kmax:
            break

    # final centers
    centers = np.zeros((K, X.shape[1]), dtype=np.float64)
    for k in range(K):
        mk = (labels == k)
        if np.any(mk):
            centers[k] = X[mk].mean(axis=0)
        else:
            centers[k] = 0.0
    return labels, centers, K


# --------------------------
# Robust stats and means
# --------------------------
def median_and_mad_global(X):
    """
    Global medians and MAD per feature.
    Returns dicts {feature: median}, {feature: MAD}
    """
    stats_med = {}
    stats_mad = {}
    for j, feat in enumerate(FEATURES):
        col = X[:, j]
        med = float(np.median(col))
        absdev = np.abs(col - med)
        mad = float(np.median(absdev))
        stats_med[feat] = med
        stats_mad[feat] = mad
    return stats_med, stats_mad


def mean_global(X):
    """
    Global means per feature.
    Returns dict {feature: mean}
    """
    stats_mean = {}
    for j, feat in enumerate(FEATURES):
        stats_mean[feat] = float(np.mean(X[:, j]))
    return stats_mean


def cluster_medians(X, labels, K):
    """
    Medians per cluster and per feature.
    Returns: medians[i][feat] = median
    """
    medians = {}
    for k in range(K):
        mask = (labels == k)
        if not np.any(mask):
            medians[k] = {feat: float("nan") for feat in FEATURES}
            continue
        Xk = X[mask]
        medians[k] = {}
        for j, feat in enumerate(FEATURES):
            medians[k][feat] = float(np.median(Xk[:, j]))
    return medians


def cluster_means(X, labels, K):
    """
    Means per cluster and per feature.
    Returns: means[i][feat] = mean
    """
    means = {}
    for k in range(K):
        mask = (labels == k)
        if not np.any(mask):
            means[k] = {feat: float("nan") for feat in FEATURES}
            continue
        Xk = X[mask]
        means[k] = {}
        for j, feat in enumerate(FEATURES):
            means[k][feat] = float(np.mean(Xk[:, j]))
    return means


# --------------------------
# Category scoring (formalism)
# --------------------------
def score_categories(center_glob, mad_glob, center_clu, epsz, epsmod):
    """
    For one cluster:
      - Δ = C_i - C_global, where C = 0.5*(median + mean)
      - z = Δ / (MAD + epsz)
      - Scores S_hot, S_shared, S_moderate, S_archival
    """
    # Δ and z
    delta = {}
    z = {}
    for feat in FEATURES:
        Ci = center_clu[feat]
        Cg = center_glob[feat]
        d = Ci - Cg
        delta[feat] = d
        mad = mad_glob[feat]
        denom = mad + epsz
        if denom == 0.0:
            denom = 1.0
        z[feat] = d / denom

    # Feature sets per category and direction
    H_hot = ["freq", WRATIO_ALIAS, "conc"]
    L_hot = []

    H_shared = ["freq", "conc"]
    L_shared = ["locality"]

    H_arch = ["age"]
    L_arch = ["freq", WRATIO_ALIAS, "conc"]

    F_mod = ["freq", "conc"]

    # Equal weights per category
    w_hot    = 1.0 / float(len(H_hot)    + len(L_hot))
    w_shared = 1.0 / float(len(H_shared) + len(L_shared))
    w_arch   = 1.0 / float(len(H_arch)   + len(L_arch))
    w_mod    = 1.0 / float(len(F_mod))

    def pos(x):
        return x if x > 0.0 else 0.0

    # Directional scores
    S_hot = 0.0
    for p in H_hot:
        S_hot += w_hot * pos(z[p])

    S_shared = 0.0
    for p in H_shared:
        S_shared += w_shared * pos(z[p])
    for p in L_shared:
        S_shared += w_shared * pos(-z[p])

    S_arch = 0.0
    for p in H_arch:
        S_arch += w_arch * pos(z[p])
    for p in L_arch:
        S_arch += w_arch * pos(-z[p])

    # Moderate: 1/(|Δ| + epsmod) on {freq, conc}
    S_mod = 0.0
    for p in F_mod:
        denom = abs(delta[p]) + epsmod
        if denom == 0.0:
            denom = 1.0
        S_mod += w_mod * (1.0 / denom)

    scores = {
        "Hot": S_hot,
        "Shared": S_shared,
        "Moderate": S_mod,
        "Archival": S_arch,
    }
    return delta, z, scores


# --------------------------
# Writers
# --------------------------
def write_clusters_csv(outdir, ids, labels, tag):
    path = os.path.join(outdir, f"{tag}_clusters.csv")
    with open(path, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["dataset", "cluster"])
        for ds, lb in zip(ids, labels):
            wr.writerow([ds, int(lb)])
    print(f"[cluster] Assignments written to: {path}")


def write_centroids_csv(outdir, X, labels, K, tag):
    path = os.path.join(outdir, f"{tag}_centroids.csv")
    with open(path, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["cluster"] + [f"mean_{feat}" for feat in FEATURES] + ["n"])
        for k in range(K):
            mk = (labels == k)
            if np.any(mk):
                means = X[mk].mean(axis=0)
                wr.writerow([k] + [f"{float(means[j]):.6f}" for j in range(len(FEATURES))] + [int(np.sum(mk))])
            else:
                wr.writerow([k] + ["nan"]*len(FEATURES) + [0])
    print(f"[cluster] Centroids written to: {path}")


def write_labels_csv(outdir, cluster_scores, tag):
    path = os.path.join(outdir, f"{tag}_cluster_labels.csv")
    with open(path, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow([
            "cluster","label",
            "S_hot","S_shared","S_moderate","S_archival",
            "s_star","phi_shared","n"
        ])
        for k in sorted(cluster_scores.keys()):
            cs = cluster_scores[k]
            sc = cs["scores"]
            wr.writerow([
                k, cs["label"],
                f"{sc['Hot']:.6f}",
                f"{sc['Shared']:.6f}",
                f"{sc['Moderate']:.6f}",
                f"{sc['Archival']:.6f}",
                f"{cs['S_star']:.6f}",
                f"{cs['phi_shared']:.6f}",
                cs["n"]
            ])
    print(f"[cluster] Cluster labels written to: {path}")


def write_debug_csv(outdir, cluster_debug, tag):
    path = os.path.join(outdir, f"{tag}_cluster_debug.csv")
    feats = FEATURES
    with open(path, "w", newline="") as f:
        wr = csv.writer(f)
        header = ["cluster","n"]
        header += [f"M_{p}" for p in feats]          # mixed center of the cluster
        header += [f"Delta_{p}" for p in feats]      # Delta = C_i - C_glob
        header += [f"z_{p}" for p in feats]
        header += ["S_hot","S_shared","S_moderate","S_archival","label"]
        wr.writerow(header)
        for k in sorted(cluster_debug.keys()):
            dbg = cluster_debug[k]
            row = [k, dbg["n"]]
            row += [f"{dbg['M'][p]:.6f}" for p in feats]
            row += [f"{dbg['Delta'][p]:.6f}" for p in feats]
            row += [f"{dbg['Z'][p]:.6f}" for p in feats]
            row += [
                f"{dbg['scores']['Hot']:.6f}",
                f"{dbg['scores']['Shared']:.6f}",
                f"{dbg['scores']['Moderate']:.6f}",
                f"{dbg['scores']['Archival']:.6f}",
                dbg["label"],
            ]
            wr.writerow(row)
    print(f"[cluster] Detailed debug written to: {path}")


def write_summary(outdir, labels, cluster_scores, tag):
    cats = ["Hot","Shared","Moderate","Archival"]
    cnt_clusters = {c: 0 for c in cats}
    cnt_files = {c: 0 for c in cats}
    for _, cs in cluster_scores.items():
        lab = cs["label"]
        cnt_clusters[lab] += 1
        cnt_files[lab] += cs["n"]
    path = os.path.join(outdir, f"{tag}_cluster_summary.txt")
    with open(path, "w") as f:
        f.write("[cluster] Category summary:\n")
        f.write(f"  Total number of clusters: {len(cluster_scores)}\n")
        f.write(f"  Total number of files: {len(labels)}\n")
        for c in cats:
            f.write(f"  {c}: clusters={cnt_clusters[c]}, files={cnt_files[c]}\n")
    print(f"[cluster] Summary written to: {path}")


# --------------------------
# Main
# --------------------------
def main():
    ap = argparse.ArgumentParser(description="X-Means++ plus labelling consistent with the scoring formalism.")
    ap.add_argument("--norm", required=True, help="Normalized input file (dayX_norm.csv).")
    ap.add_argument("--out", required=True, help="Output directory.")
    ap.add_argument("--kmin", type=int, default=4)
    ap.add_argument("--kmax", type=int, default=8)
    ap.add_argument("--tau", type=float, default=0.0)
    ap.add_argument("--m-min", dest="m_min", type=int, default=20)
    ap.add_argument("--max-iter-km", type=int, default=50)
    ap.add_argument("--eps0", type=float, default=None,
                    help="Legacy option. If provided, this value is used as a single epsilon.")
    ap.add_argument("--epsz", type=float, default=None,
                    help="Epsilon for z = Delta/(MAD + eps), used only if eps0 is not provided.")
    ap.add_argument("--epsmod", type=float, default=None,
                    help="Epsilon for Moderate: 1/(|Delta| + eps), used only if eps0 and epsz are not provided.")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--limit", type=int, default=None, help="Limit the number of loaded rows.")
    args = ap.parse_args()

    # Single epsilon for z and Moderate, purely numeric objective (avoid division by zero)
    default_eps = 1e-6
    if args.eps0 is not None:
        eps = args.eps0
    elif args.epsz is not None:
        eps = args.epsz
    elif args.epsmod is not None:
        eps = args.epsmod
    else:
        eps = default_eps
    epsz = eps
    epsmod = eps
    print(f"[cfg] single epsilon={eps}")

    os.makedirs(args.out, exist_ok=True)

    # Load data
    ids, X = read_normalized_data(args.norm, limit=args.limit, seed=args.seed)
    N, _ = X.shape
    print(f"[load_normalized_data] File {args.norm}: {N} rows read, {N} points loaded.")

    # X-Means
    labels, centers, K = xmeans(
        X, Kmin=args.kmin, Kmax=args.kmax, tau=args.tau,
        m_min=args.m_min, max_iter_km=args.max_iter_km, seed=args.seed
    )
    tag = os.path.splitext(os.path.basename(args.norm))[0].replace("_norm", "")
    print(f"[cluster] X-Means finished with K={K} clusters.")

    write_clusters_csv(args.out, ids, labels, tag)
    write_centroids_csv(args.out, X, labels, K, tag)

    # Global statistics
    med_glob, mad_glob = median_and_mad_global(X)
    mean_glob = mean_global(X)

    # Per cluster statistics
    med_clu = cluster_medians(X, labels, K)
    mean_clu = cluster_means(X, labels, K)

    center_glob = med_glob
    center_clu = med_clu

    # Raw scores per cluster (first pass)
    cats = ["Hot", "Shared", "Moderate", "Archival"]
    raw_scores = {}
    deltas = {}
    zs = {}
    ns = {}
    cluster_M = {}

    for k in range(K):
        mk = (labels == k)
        nk = int(np.sum(mk))
        ns[k] = nk

        if nk == 0:
            # empty cluster, zero scores
            sc = {c: 0.0 for c in cats}
            raw_scores[k] = sc
            deltas[k] = {p: float("nan") for p in FEATURES}
            zs[k] = {p: float("nan") for p in FEATURES}
            cluster_M[k] = {p: float("nan") for p in FEATURES}
            continue

        delta, z, scores = score_categories(center_glob, mad_glob, center_clu[k], epsz, epsmod)
        raw_scores[k] = scores
        deltas[k] = delta
        zs[k] = z
        cluster_M[k] = center_clu[k]

    # Per category normalization (division by global max of each category)
    max_scores = {c: 0.0 for c in cats}
    for k in range(K):
        sc = raw_scores[k]
        for c in cats:
            if sc[c] > max_scores[c]:
                max_scores[c] = sc[c]

    cluster_scores = {}
    cluster_debug = {}

    for k in range(K):
        nk = ns[k]
        sc_raw = raw_scores[k]

        if nk == 0:
            # empty cluster, default label Moderate
            scores_norm = sc_raw
            label = "Moderate"
            S_star = 0.0
            phi_shared = 0.0
        else:
            scores_norm = {}
            for c in cats:
                M_c = max_scores[c]
                if M_c > 0.0:
                    scores_norm[c] = sc_raw[c] / M_c
                else:
                    scores_norm[c] = sc_raw[c]
            label = max(scores_norm.items(), key=lambda kv: kv[1])[0]
            S_star = scores_norm[label]
            phi_shared = scores_norm["Shared"]

        cluster_scores[k] = {
            "scores": scores_norm,
            "label": label,
            "n": nk,
            "S_star": S_star,
            "phi_shared": phi_shared
        }
        cluster_debug[k] = {
            "n": nk,
            "M": cluster_M[k],
            "Delta": deltas[k],
            "Z": zs[k],
            "scores": scores_norm,
            "label": label
        }

    write_labels_csv(args.out, cluster_scores, tag)
    write_debug_csv(args.out, cluster_debug, tag)
    write_summary(args.out, labels, cluster_scores, tag)


if __name__ == "__main__":
    main()
