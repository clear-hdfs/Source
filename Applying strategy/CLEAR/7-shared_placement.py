#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import argparse
import os
from collections import defaultdict


def load_nodes(path):
    """
    Read node metadata.
    Expected columns: node_id,rack,free_gb,decommissioned,healthy
    """
    nodes = {}
    with open(path, "r", newline="") as f:
        rd = csv.DictReader(f)
        required = {"node_id", "rack", "free_gb", "decommissioned", "healthy"}
        missing = required - set(rd.fieldnames)
        if missing:
            raise ValueError(f"Missing columns in {path}: {missing}")
        for row in rd:
            nid = row["node_id"].strip()
            rack = row["rack"].strip()
            try:
                free_gb = float(row["free_gb"])
            except ValueError:
                free_gb = 0.0
            decommissioned = row["decommissioned"].strip() in {"1", "true", "True"}
            healthy = row["healthy"].strip() in {"1", "true", "True"}
            nodes[nid] = {
                "rack": rack,
                "free_gb": free_gb,
                "decommissioned": decommissioned,
                "healthy": healthy,
            }
    return nodes


def build_access_from_batch(batch_path, node_ids):
    """
    Build access counts from Alibaba batch_instance_dayX.csv.

    We map each Alibaba machine_id to one of the node_ids using a
    deterministic hash, then we aggregate durations on these logical nodes.

    freq[f] = sum_n d_f(n), d_f(n) = total duration of instances of job f on node n.
    """
    freq = defaultdict(int)
    df = defaultdict(lambda: defaultdict(int))

    n_nodes = len(node_ids)
    if n_nodes == 0:
        raise ValueError("No node_ids provided to build_access_from_batch")

    with open(batch_path, "r", newline="") as f:
        rd = csv.DictReader(f)
        required = {"job_name", "machine_id", "start_time", "end_time"}
        missing = required - set(rd.fieldnames)
        if missing:
            raise ValueError(f"Missing columns in {batch_path}: {missing}")
        for row in rd:
            fid = row["job_name"].strip()
            mid = row["machine_id"].strip()
            try:
                start = int(float(row["start_time"]))
                end = int(float(row["end_time"]))
            except ValueError:
                continue
            dur = max(1, end - start)

            # hash stable sur machine_id pour choisir un DataNode Hadoop
            hidx = sum(ord(ch) for ch in mid) % n_nodes
            nid = node_ids[hidx]

            freq[fid] += dur
            df[fid][nid] += dur

    return freq, df


def select_source_and_dest(fid, replicas, freq_f, df_f):
    if not replicas:
        return None, None

    m_star = None
    min_d = None
    for m in replicas:
        d_m = df_f.get(m, 0)
        if min_d is None or d_m < min_d:
            min_d = d_m
            m_star = m

    n_star = None
    max_d = None
    for n, d_n in df_f.items():
        if n in replicas:
            continue
        if max_d is None or d_n > max_d:
            max_d = d_n
            n_star = n

    if n_star is None:
        return None, None

    return m_star, n_star


def compute_delta_loc(freq_f, d_src, d_dest):
    if freq_f <= 0:
        return 0.0
    return float(d_dest - d_src) / float(freq_f)


def node_checks_ok(fid, m_star, n_star, replicas, nodes, params, moves_per_rack, total_moves):
    if m_star is None or n_star is None:
        return False

    if m_star not in nodes or n_star not in nodes:
        return False

    src = nodes[m_star]
    dst = nodes[n_star]

    if dst["decommissioned"]:
        return False
    if not dst["healthy"] or not src["healthy"]:
        return False
    if dst["free_gb"] < params["min_free_gb"]:
        return False

    racks_before = set(nodes[m]["rack"] for m in replicas if m in nodes)
    racks_after = set(nodes[m]["rack"] for m in replicas if m in nodes)
    if m_star in replicas and m_star in nodes:
        racks_after.discard(nodes[m_star]["rack"])
    racks_after.add(dst["rack"])

    if len(racks_before) < params["min_racks"]:
        return False
    if len(racks_after) < params["min_racks"]:
        return False

    if total_moves >= params["max_moves"]:
        return False

    rack_dst = dst["rack"]
    if moves_per_rack[rack_dst] >= params["max_moves_per_rack"]:
        return False

    return True


def plan_shared_moves(files, freq, df, nodes, params):
    moves = []
    moves_per_rack = defaultdict(int)
    total_moves = 0

    for fid, meta in files.items():
        if meta["label"] != "Shared":
            continue

        replicas = meta["replicas"]
        if len(replicas) < 1:
            continue

        freq_f = freq.get(fid, 0)
        if freq_f < params["min_access"]:
            continue

        df_f = df.get(fid, {})

        m_star, n_star = select_source_and_dest(fid, replicas, freq_f, df_f)
        if m_star is None or n_star is None:
            continue

        d_src = df_f.get(m_star, 0)
        d_dst = df_f.get(n_star, 0)
        delta_loc = compute_delta_loc(freq_f, d_src, d_dst)

        if delta_loc <= params["gamma"]:
            continue

        if not node_checks_ok(fid, m_star, n_star, replicas, nodes,
                              params, moves_per_rack, total_moves):
            continue

        moves.append({
            "file_id": fid,
            "src": m_star,
            "dst": n_star,
            "freq": freq_f,
            "d_src": d_src,
            "d_dst": d_dst,
            "delta_loc": delta_loc,
        })

        dst_rack = nodes[n_star]["rack"]
        moves_per_rack[dst_rack] += 1
        total_moves += 1

    return moves


def write_moves(path, moves):
    fieldnames = ["file_id", "src", "dst", "freq", "d_src", "d_dst", "delta_loc"]
    with open(path, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=fieldnames)
        wr.writeheader()
        for m in moves:
            row = {
                "file_id": m["file_id"],
                "src": m["src"],
                "dst": m["dst"],
                "freq": m["freq"],
                "d_src": m["d_src"],
                "d_dst": m["d_dst"],
                "delta_loc": f"{m['delta_loc']:.6f}",
            }
            wr.writerow(row)


def build_shared_files_and_access(clusters_path, labels_path,
                                  freq_all, df_all, nodes,
                                  shared_files_path, shared_access_path,
                                  rf_shared=3):
    cluster_label = {}
    with open(labels_path, "r", newline="") as f:
        rd = csv.DictReader(f)
        required = {"cluster", "label"}
        missing = required - set(rd.fieldnames)
        if missing:
            raise ValueError(f"Missing columns in {labels_path}: {missing}")
        for row in rd:
            cid = int(row["cluster"])
            lab = row["label"].strip()
            cluster_label[cid] = lab

    cluster_files = defaultdict(list)
    with open(clusters_path, "r", newline="") as f:
        rd = csv.DictReader(f)
        required = {"dataset", "cluster"}
        missing = required - set(rd.fieldnames)
        if missing:
            raise ValueError(f"Missing columns in {clusters_path}: {missing}")
        for row in rd:
            fid = row["dataset"].strip()
            cid = int(row["cluster"])
            cluster_files[cid].append(fid)

    nodes_by_rack = defaultdict(list)
    for nid, info in nodes.items():
        rack = info["rack"]
        nodes_by_rack[rack].append(nid)
    racks = sorted(nodes_by_rack.keys())
    if not racks:
        raise ValueError("No racks found in nodes description")

    interleaved_nodes = []
    max_len = max(len(v) for v in nodes_by_rack.values())
    for i in range(max_len):
        for r in racks:
            lst = nodes_by_rack[r]
            if i < len(lst):
                interleaved_nodes.append(lst[i])
    if not interleaved_nodes:
        raise ValueError("No nodes available for fake replicas")

    files_shared = {}
    freq_shared = {}
    df_shared = {}

    with open(shared_files_path, "w", newline="") as f_files, \
         open(shared_access_path, "w", newline="") as f_acc:

        wr_files = csv.writer(f_files)
        wr_files.writerow(["file_id", "label", "replicas"])

        wr_acc = csv.writer(f_acc)
        wr_acc.writerow(["file_id", "node_id", "count"])

        idx = 0
        n_all = len(interleaved_nodes)
        for cid, file_list in cluster_files.items():
            lab = cluster_label.get(cid)
            if lab != "Shared":
                continue
            for fid in file_list:
                replicas = []
                for j in range(rf_shared):
                    replicas.append(interleaved_nodes[(idx + j) % n_all])
                replicas_set = set(replicas)

                files_shared[fid] = {
                    "label": "Shared",
                    "replicas": replicas_set,
                }

                freq_f = freq_all.get(fid, 0)
                df_f = df_all.get(fid, {})
                freq_shared[fid] = freq_f
                df_shared[fid] = dict(df_f)

                wr_files.writerow([fid, "Shared", ";".join(sorted(replicas_set))])

                for nid, c in df_f.items():
                    wr_acc.writerow([fid, nid, c])

                idx += 1

    return files_shared, freq_shared, df_shared


def main():
    ap = argparse.ArgumentParser(
        description="Shared placement pipeline using Alibaba batch_instance_dayX.csv."
    )
    ap.add_argument("--day", required=True,
                    help="Tag for this window (e.g., day1, day2, day3)")
    ap.add_argument("--batch", required=True,
                    help="Alibaba batch_instance_dayX.csv file")
    ap.add_argument("--clusters", required=True,
                    help="CSV dayX_clusters.csv (dataset,cluster)")
    ap.add_argument("--labels", required=True,
                    help="CSV dayX_cluster_labels.csv (cluster,label,...)")
    ap.add_argument("--nodes", required=True,
                    help="CSV with node metadata (node_id,rack,free_gb,decommissioned,healthy)")
    ap.add_argument("--out-dir", required=True,
                    help="Output directory")
    ap.add_argument("--gamma", type=float, default=0.05,
                    help="Minimum net locality gain Delta_loc to accept a move")
    ap.add_argument("--min-access", type=int, default=50,
                    help="Minimum freq(f) to avoid reacting to noise")
    ap.add_argument("--min-racks", type=int, default=2,
                    help="Minimum rack diversity to preserve after move")
    ap.add_argument("--min-free-gb", type=float, default=10.0,
                    help="Minimum free GB on destination node")
    ap.add_argument("--max-moves", type=int, default=10000,
                    help="Maximum moves per window")
    ap.add_argument("--max-moves-per-rack", type=int, default=1000,
                    help="Maximum moves per rack per window")
    ap.add_argument("--rf-shared", type=int, default=3,
                    help="Replication factor used for Shared files in the simulation")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    nodes = load_nodes(args.nodes)
    print(f"[shared] Loaded {len(nodes)} nodes from {args.nodes}")
    node_ids = sorted(nodes.keys())
    freq_all, df_all = build_access_from_batch(args.batch, node_ids)
    print(f"[shared] Built access counts from {args.batch} for {len(freq_all)} jobs")

    shared_files_path = os.path.join(args.out_dir, f"{args.day}_shared_files.csv")
    shared_access_path = os.path.join(args.out_dir, f"{args.day}_shared_access_counts.csv")
    moves_path = os.path.join(args.out_dir, f"{args.day}_shared_moves.csv")

    files_shared, freq_shared, df_shared = build_shared_files_and_access(
        args.clusters,
        args.labels,
        freq_all,
        df_all,
        nodes,
        shared_files_path,
        shared_access_path,
        rf_shared=args.rf_shared,
    )
    print(f"[shared] Built shared_files: {len(files_shared)} Shared jobs")
    print(f"[shared] Written {shared_files_path} and {shared_access_path}")

    params = {
        "gamma": args.gamma,
        "min_access": args.min_access,
        "min_racks": args.min_racks,
        "min_free_gb": args.min_free_gb,
        "max_moves": args.max_moves,
        "max_moves_per_rack": args.max_moves_per_rack,
    }

    moves = plan_shared_moves(files_shared, freq_shared, df_shared, nodes, params)
    print(f"[shared] Planned {len(moves)} moves for Shared jobs")

    write_moves(moves_path, moves)
    print(f"[shared] Moves written to {moves_path}")


if __name__ == "__main__":
    main()
