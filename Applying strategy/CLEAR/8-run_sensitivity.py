#!/usr/bin/env python3

from pathlib import Path
from collections import Counter
import csv
import subprocess
import sys

ROOT = Path("/home/hduser/Source")

NORM = ROOT / "Evaluation/day2_norm.csv"

XMEANS = ROOT / "Applying strategy/CLEAR/5-xmeans_label.py"
RF_SCRIPT = ROOT / "Applying strategy/CLEAR/6-assign_rf_per_cluster.py"

BASELINE_DIR = ROOT / "Evaluation/day2_reference_check"
SENS_DIR = ROOT / "Evaluation/sensitivity_reference"

CONFIGS = [
    ("baseline", 500, 250, BASELINE_DIR, False),
    ("kmax_250", 250, 250, SENS_DIR / "kmax_250", True),
    ("kmax_750", 750, 250, SENS_DIR / "kmax_750", True),
    ("mmin_150", 500, 150, SENS_DIR / "mmin_150", True),
    ("mmin_350", 500, 350, SENS_DIR / "mmin_350", True),
]

def run(cmd):
    print("\n$", " ".join(str(x) for x in cmd))
    subprocess.run(cmd, check=True)

def execute(name, kmax, mmin, out, do_run):
    out.mkdir(parents=True, exist_ok=True)

    if not do_run:
        print("[reuse] baseline:", out)
        return

    run([
        sys.executable, str(XMEANS),
        "--norm", str(NORM),
        "--out", str(out),
        "--kmin", "4",
        "--kmax", str(kmax),
        "--m-min", str(mmin),
        "--seed", "42",
        "--epsz", "1e-6"
    ])

    run([
        sys.executable, str(RF_SCRIPT),
        "--labels", str(out / "day2_cluster_labels.csv"),
        "--out", str(out / "day2_cluster_rf.csv")
    ])

def summarize(name, kmax, mmin, out):
    rfinfo = {}

    with open(out / "day2_cluster_rf.csv", newline="") as f:
        for r in csv.DictReader(f):
            rfinfo[int(r["cluster"])] = {
                "label": r["label"],
                "rf": int(r["R_i"])
            }

    cats = Counter()
    rfs = Counter()

    n = 0
    cost = 0.0

    with open(out / "day2_clusters.csv", newline="") as f:
        for r in csv.DictReader(f):
            info = rfinfo[int(r["cluster"])]

            label = info["label"]
            rf = info["rf"]

            cats[label] += 1
            rfs[rf] += 1
            n += 1

            if label == "Archival":
                cost += 1.5 if rf == 0 else 2.5
            else:
                cost += rf

    overhead = cost / (3.0 * n)

    return {
        "variant": name,
        "kmax": kmax,
        "m_min": mmin,
        "n_clusters": len(rfinfo),

        "hot_files": cats["Hot"],
        "shared_files": cats["Shared"],
        "moderate_files": cats["Moderate"],
        "archival_files": cats["Archival"],

        "hot_pct": 100 * cats["Hot"] / n,
        "shared_pct": 100 * cats["Shared"] / n,
        "moderate_pct": 100 * cats["Moderate"] / n,
        "archival_pct": 100 * cats["Archival"] / n,

        "rf0": rfs[0],
        "rf1": rfs[1],
        "rf2": rfs[2],
        "rf3": rfs[3],
        "rf4": rfs[4],
        "rf5": rfs[5],

        "storage_overhead": overhead,
        "storage_reduction_pct": 100 * (1 - overhead)
    }

def main():
    results = []

    for name, kmax, mmin, out, do_run in CONFIGS:
        print("\n" + "=" * 70)
        print(f"{name}: Kmax={kmax}, m_min={mmin}")
        print("=" * 70)

        execute(name, kmax, mmin, out, do_run)
        results.append(summarize(name, kmax, mmin, out))

    outcsv = SENS_DIR / "day2_sensitivity_reference.csv"

    with open(outcsv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        w.writeheader()
        w.writerows(results)

    print("\n" + "=" * 70)
    print("DAY2 SENSITIVITY")
    print("=" * 70)

    for r in results:
        print(
            f"{r['variant']:12s} "
            f"K={r['n_clusters']:3d}  "
            f"Hot={r['hot_pct']:6.2f}%  "
            f"Shared={r['shared_pct']:6.2f}%  "
            f"Moderate={r['moderate_pct']:6.2f}%  "
            f"Archival={r['archival_pct']:6.2f}%  "
            f"Storage={r['storage_overhead']:.4f}"
        )

    print("\nWritten:", outcsv)

if __name__ == "__main__":
    main()

