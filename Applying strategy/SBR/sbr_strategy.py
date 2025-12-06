#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sbr_strategy.py

Support Based Replication (SBR) strategy for HDFS style clusters.

Inputs:
  - batch_instance_dayX.csv: Alibaba-like trace with job_name, machine_id,
    start_time, end_time
  - nodes.csv: list of HDFS DataNodes and their racks

The script:
  1) Derives per-file global request volume from the batch file.
  2) Derives per-file local request volume for each node (using a stable
     hash to map machine_id to one of your DataNodes).
  3) Computes global support for each file:
         support(f) = req(f) / sum_g req(g)
  4) Classifies files into three categories based on support:
         Cat1: support >= minsupp2      -> RF = 4
         Cat2: minsupp1 <= support < minsupp2 -> RF = 3
         Cat3: support < minsupp1       -> RF = 2
  5) Computes local support per node and applies an SBR-like placement:
         - first replica: node with highest local support
         - second replica: different rack
         - third replica: same rack as first, different node
         - fourth replica: rack not yet used if possible

Outputs:
  - <day>_sbr_rf.csv:
        file_id, support, category, rf
  - <day>_sbr_layout.csv:
        file_id, replica_index, node_id, rack
  - <day>_sbr_summary.csv:
        category, count, rf_min, rf_max, rf_mean
"""

import csv
import argparse
import os
from collections import defaultdict


###############################################################################
# Load nodes
###############################################################################

def load_nodes(nodes_path):
    """
    Load DataNode metadata from nodes.csv.

    Expected columns:
      node_id,rack,free_gb,decommissioned,healthy
    Only node_id and rack are strictly required.
    """
    nodes = {}
    with open(nodes_path, "r", newline="") as f:
        rd = csv.DictReader(f)
        required = {"node_id", "rack"}
        missing = required - set(rd.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns in {nodes_path}: {missing}")
        for row in rd:
            nid = row["node_id"].strip()
            if not nid:
                continue
            rack = row["rack"].strip()
            free_gb = float(row.get("free_gb", "0") or 0)
            decommissioned = int(row.get("decommissioned", "0") or 0)
            healthy = int(row.get("healthy", "1") or 1)
            nodes[nid] = {
                "rack": rack,
                "free_gb": free_gb,
                "decommissioned": decommissioned,
                "healthy": healthy,
            }
    return nodes


###############################################################################
# Extract per file and per node access from batch_instance_dayX.csv
###############################################################################

def build_access_from_batch(batch_path, node_ids, w_start=None, w_end=None):
    """
    Derive per file and per node access volume from batch_instance_dayX.csv.

    Assumptions:
      - job_name identifies a logical file f
      - machine_id is mapped to one of your DataNodes via a stable hash

    Returns:
      freq[f]      total "request volume" for file f (sum of durations)
      df[f][n]     local "request volume" for file f on node n

    We use the duration of each interval as a proxy for the amount of work.
    """
    freq = defaultdict(float)
    df = defaultdict(lambda: defaultdict(float))

    n_nodes = len(node_ids)
    if n_nodes == 0:
        raise ValueError("No node_ids provided")

    with open(batch_path, "r", newline="") as f:
        rd = csv.DictReader(f)
        required = {"job_name", "machine_id", "start_time", "end_time"}
        missing = required - set(rd.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns in {batch_path}: {missing}")

        for row in rd:
            fid = row["job_name"].strip()
            mid = row["machine_id"].strip()
            if not fid or not mid:
                continue

            try:
                start = float(row["start_time"])
                end = float(row["end_time"])
            except ValueError:
                continue
            if not (end > start):
                continue

            # Window filter
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

            # Stable hash machine_id -> DataNode index
            h = sum(ord(ch) for ch in mid)
            idx = int(h % n_nodes)
            nid = node_ids[idx]

            freq[fid] += dur
            df[fid][nid] += dur

    return freq, df


###############################################################################
# Global support and category
###############################################################################

def compute_support(freq):
    """
    Global SBR support at file level:
      support(f) = req(f) / sum_g req(g)
    """
    total = sum(freq.values())
    if total <= 0:
        return {f: 0.0 for f in freq.keys()}
    return {f: freq[f] / total for f in freq.keys()}


def classify_sbr(support, minsupp1, minsupp2):
    """
    SBR style categorization based on support thresholds:

      Cat1: support >= minsupp2          -> RF = 4
      Cat2: minsupp1 <= support < minsupp2 -> RF = 3
      Cat3: support < minsupp1           -> RF = 2
    """
    categories = {}
    rf = {}
    for fid, s in support.items():
        if s >= minsupp2:
            cat = "Cat1"
            r = 4
        elif s >= minsupp1:
            cat = "Cat2"
            r = 3
        else:
            cat = "Cat3"
            r = 2
        categories[fid] = cat
        rf[fid] = r
    return categories, rf


###############################################################################
# Local support and placement
###############################################################################

def compute_local_support(freq, df):
    """
    Local support per file and node:

      local_support[f][n] = df[f][n] / freq[f]
    """
    local = {}
    for fid, freq_f in freq.items():
        if freq_f <= 0:
            continue
        lf = {}
        for nid, v in df[fid].items():
            if v > 0:
                lf[nid] = v / freq_f
        local[fid] = lf
    return local


def place_replicas_sbr(nodes, categories, rf_by_file, local_support):
    """
    Replica placement inspired by the SBR algorithm:

      - First replica: node with highest local support.
      - Second replica: different rack, different node, high local support.
      - Third replica: same rack as the first, different node, high local support.
      - Fourth replica: some remaining rack if possible, high local support.

    If the ideal choice is not possible, we fall back to the first nodes
    in the sorted order.

    Returns:
      layout[f] = [node1, node2, ...] of length rf_by_file[f]
    """
    layout = {}
    node_ids = list(nodes.keys())

    def rack_of(n):
        return nodes[n]["rack"]

    for fid, cat in categories.items():
        r = rf_by_file.get(fid, 2)
        r = max(1, min(r, 4))  # SBR does not go beyond RF = 4

        ls = local_support.get(fid, {})
        # Nodes sorted by decreasing local support, then by node id
        sorted_nodes = sorted(node_ids, key=lambda n: (-ls.get(n, 0.0), n))

        if not sorted_nodes:
            # Fallback when no local info is available
            sorted_nodes = list(node_ids)

        chosen = []

        # First replica
        n1 = sorted_nodes[0]
        chosen.append(n1)

        # Second replica
        if r >= 2:
            rack1 = rack_of(n1)
            n2 = None
            for n in sorted_nodes:
                if n == n1:
                    continue
                if rack_of(n) != rack1:
                    n2 = n
                    break
            if n2 is None:
                # If no different rack available, pick any other node
                for n in sorted_nodes:
                    if n != n1:
                        n2 = n
                        break
            if n2 is None:
                n2 = n1
            chosen.append(n2)

        # Third replica
        if r >= 3:
            rack1 = rack_of(n1)
            n3 = None
            for n in sorted_nodes:
                if n in chosen:
                    continue
                if rack_of(n) == rack1:
                    n3 = n
                    break
            if n3 is None:
                # Fallback: first node not chosen yet
                for n in sorted_nodes:
                    if n not in chosen:
                        n3 = n
                        break
            if n3 is None:
                n3 = n1
            chosen.append(n3)

        # Fourth replica
        if r >= 4:
            used_racks = {rack_of(n) for n in chosen}
            n4 = None
            for n in sorted_nodes:
                if n in chosen:
                    continue
                if rack_of(n) not in used_racks:
                    n4 = n
                    break
            if n4 is None:
                for n in sorted_nodes:
                    if n not in chosen:
                        n4 = n
                        break
            if n4 is None:
                n4 = n1
            chosen.append(n4)

        layout[fid] = chosen[:r]

    return layout


###############################################################################
# Writers
###############################################################################

def write_sbr_rf(out_path, support, categories, rf_by_file):
    """
    Write per file support, category and replication factor.
    """
    with open(out_path, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["file_id", "support", "category", "rf"])
        for fid in sorted(support.keys()):
            wr.writerow(
                [fid, f"{support[fid]:.8f}", categories[fid], rf_by_file[fid]]
            )


def write_sbr_layout(out_path, layout, nodes):
    """
    Write per file replica placement (node and rack).
    """
    with open(out_path, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["file_id", "replica_index", "node_id", "rack"])
        for fid in sorted(layout.keys()):
            reps = layout[fid]
            for idx, nid in enumerate(reps, start=1):
                rack = nodes[nid]["rack"]
                wr.writerow([fid, idx, nid, rack])


def write_sbr_summary(out_path, categories, rf_by_file):
    """
    Write a small summary per SBR category with RF statistics.
    """
    by_cat = defaultdict(list)
    for fid, cat in categories.items():
        by_cat[cat].append(rf_by_file.get(fid, 0))

    with open(out_path, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["category", "count", "rf_min", "rf_max", "rf_mean"])
        for cat in sorted(by_cat.keys()):
            rfs = by_cat[cat]
            if not rfs:
                continue
            rf_min = min(rfs)
            rf_max = max(rfs)
            rf_mean = sum(rfs) / float(len(rfs))
            wr.writerow([cat, len(rfs), rf_min, rf_max, f"{rf_mean:.3f}"])


###############################################################################
# Main
###############################################################################

def main():
    ap = argparse.ArgumentParser(
        description="Support Based Replication (SBR) strategy from batch_instance_dayX.csv."
    )
    ap.add_argument(
        "--batch",
        required=True,
        help="Input batch_instance_dayX.csv file",
    )
    ap.add_argument(
        "--nodes",
        required=True,
        help="nodes.csv with node_id,rack,...",
    )
    ap.add_argument(
        "--out-dir",
        required=True,
        help="Output directory",
    )
    ap.add_argument(
        "--day",
        required=True,
        help="Window tag, for example day1, day2, day3",
    )
    ap.add_argument(
        "--w-start",
        type=float,
        default=None,
        help="Window start time in seconds (inclusive)",
    )
    ap.add_argument(
        "--w-end",
        type=float,
        default=None,
        help="Window end time in seconds (exclusive)",
    )

    # Support thresholds
    ap.add_argument(
        "--minsupp1",
        type=float,
        default=0.001,
        help="Support threshold for Cat2 (default 0.001)",
    )
    ap.add_argument(
        "--minsupp2",
        type=float,
        default=0.01,
        help="Support threshold for Cat1 (default 0.01)",
    )

    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    print(f"[SBR] Loading nodes from {args.nodes}")
    nodes = load_nodes(args.nodes)
    node_ids = sorted(nodes.keys())
    print(f"[SBR] Loaded {len(node_ids)} nodes")

    print(f"[SBR] Reading batch file {args.batch}")
    freq, df = build_access_from_batch(
        args.batch,
        node_ids,
        w_start=args.w_start,
        w_end=args.w_end,
    )
    print(f"[SBR] Found {len(freq)} files with activity in the window")

    support = compute_support(freq)
    categories, rf_by_file = classify_sbr(
        support,
        args.minsupp1,
        args.minsupp2,
    )

    local_support = compute_local_support(freq, df)
    layout = place_replicas_sbr(nodes, categories, rf_by_file, local_support)

    rf_path = os.path.join(args.out_dir, f"{args.day}_sbr_rf.csv")
    layout_path = os.path.join(args.out_dir, f"{args.day}_sbr_layout.csv")
    summary_path = os.path.join(args.out_dir, f"{args.day}_sbr_summary.csv")

    write_sbr_rf(rf_path, support, categories, rf_by_file)
    write_sbr_layout(layout_path, layout, nodes)
    write_sbr_summary(summary_path, categories, rf_by_file)

    print(f"[SBR] RF per file written to {rf_path}")
    print(f"[SBR] Replica layout written to {layout_path}")
    print(f"[SBR] Summary written to {summary_path}")


if __name__ == "__main__":
    main()
