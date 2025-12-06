#!/usr/bin/env python3
import csv, sys, os, binascii, subprocess, tempfile, heapq

# Usage:
#   python3 build_features_extsort_2stage.py <BATCH_FILE> <USAGE_FILE> <OUTPUT_FILE> <WSTART> <WEND>
# Examples:
#   Day1: python3 build_features_extsort_2stage.py batch_instance_day1.csv machine_usage_day1_used.csv features_day1.csv 0 86400
#   Day3: python3 build_features_extsort_2stage.py batch_instance_day3.csv machine_usage_day3_used.csv features_day3.csv 172800 259200
#
# Optional environment variables:
#   SHARDS=96         number of shards (avoids too many file descriptors)
#   TMPDIR=/tmp       fast temporary directory (avoids /mnt/c under WSL)
#   SORT_MEM=2G       memory for sort (-S)
#   SORT_PAR=4        threads for sort (--parallel)

def die(msg, code=1):
    print(msg, file=sys.stderr)
    sys.exit(code)

if len(sys.argv) != 6:
    die("Usage: python3 build_features_extsort_2stage.py <BATCH_FILE> <USAGE_FILE> <OUTPUT_FILE> <WSTART> <WEND>")

BATCH_FILE   = sys.argv[1]
USAGE_FILE   = sys.argv[2]
OUTPUT_FILE  = sys.argv[3]
WSTART       = int(sys.argv[4])
WEND         = int(sys.argv[5])

N_SHARDS = int(os.environ.get("SHARDS", "96"))
TMPDIR   = os.environ.get("TMPDIR", "/tmp")
SORT_MEM = os.environ.get("SORT_MEM", "2G")
SORT_PAR = os.environ.get("SORT_PAR", "4")

print(f"[i] Batch={BATCH_FILE}  Usage={USAGE_FILE}  Out={OUTPUT_FILE}  Window=[{WSTART},{WEND})")
print(f"[i] SHARDS={N_SHARDS}  TMPDIR={TMPDIR}  SORT_MEM={SORT_MEM}  SORT_PAR={SORT_PAR}")

os.makedirs(TMPDIR, exist_ok=True)
workdir = tempfile.mkdtemp(prefix="bf2s_", dir=TMPDIR)

def shard_idx(job: str) -> int:
    return (binascii.crc32(job.encode("utf-8")) & 0x7fffffff) % N_SHARDS

# --------------------------------------------------------------------
# Phase 0: average net_in per machine (small dict in RAM, OK)
# --------------------------------------------------------------------
print("[i] Reading machine_usage for net_in averages ...")
machine_sum = {}
machine_cnt = {}
with open(USAGE_FILE, "r", newline="") as fu:
    ru = csv.reader(fu)
    _ = next(ru, None)
    for row in ru:
        if not row or len(row) < 9:
            continue
        mid = row[0]
        try:
            net_in = float(row[6])
        except:
            continue
        machine_sum[mid] = machine_sum.get(mid, 0.0) + net_in
        machine_cnt[mid] = machine_cnt.get(mid, 0) + 1
machine_avg_netin = {m: (machine_sum[m] / machine_cnt[m]) for m in machine_sum if machine_cnt[m] > 0}
print(f"[i] Machines with average net_in: {len(machine_avg_netin)}")

# --------------------------------------------------------------------
# Phase 1: read batch -> 3 sharded streams plus global CPU stats
#   a) intervals.<s>.csv      : job,start,end
#   b) jobstats.<s>.csv       : job,cpu_avg,start
#   c) jobmachines.<s>.csv    : job,machine_id
# No per-job dict in RAM.
# --------------------------------------------------------------------
print("[i] Sharding batch -> intervals, jobstats, jobmachines ...")
int_paths, js_paths, jm_paths = [], [], []
int_files, js_files, jm_files = [], [], []
for i in range(N_SHARDS):
    p_int = os.path.join(workdir, f"intervals.{i}.csv")
    p_js  = os.path.join(workdir, f"jobstats.{i}.csv")
    p_jm  = os.path.join(workdir, f"jobmachines.{i}.csv")
    int_paths.append(p_int); js_paths.append(p_js); jm_paths.append(p_jm)
    int_files.append(open(p_int, "w", buffering=1024*1024))
    js_files.append(open(p_js,  "w", buffering=512*1024))
    jm_files.append(open(p_jm,  "w", buffering=512*1024))

global_cpu_sum = 0.0
global_cpu_cnt = 0

with open(BATCH_FILE, "r", newline="") as fb:
    rb = csv.reader(fb)
    _ = next(rb, None)
    for row in rb:
        if not row or len(row) < 14:
            continue
        job = row[2]
        try:
            st = int(row[5]); en = int(row[6])
        except:
            continue
        mid = row[7]
        try:
            cpu_avg = float(row[10])
        except:
            cpu_avg = 0.0

        global_cpu_sum += cpu_avg
        global_cpu_cnt += 1

        si = shard_idx(job)
        # a) clipped intervals
        a = st if st > WSTART else WSTART
        b = en if en < WEND   else WEND
        if a < b:
            int_files[si].write(f"{job},{a},{b}\n")
        # b) small raw stats
        js_files[si].write(f"{job},{cpu_avg},{st}\n")
        # c) machine for locality (unique plus mean via reduce)
        jm_files[si].write(f"{job},{mid}\n")

for f in int_files + js_files + jm_files:
    f.close()

global_cpu_avg = (global_cpu_sum / global_cpu_cnt) if global_cpu_cnt else 1.0
print(f"[i] Global CPU avg: {global_cpu_avg:.6f}  (records: {global_cpu_cnt})")

# --------------------------------------------------------------------
# Helpers for sort
# --------------------------------------------------------------------
def run_sort(in_path: str, out_path: str, sort_key: str):
    # sort_key example: "-k1,1 -k2,2n"
    cmd = [
        "bash","-lc",
        f"export LC_ALL=C; sort -t, {sort_key} -S {SORT_MEM} --parallel={SORT_PAR} -T '{TMPDIR}' '{in_path}' -o '{out_path}'"
    ]
    r = subprocess.run(cmd)
    if r.returncode != 0:
        die(f"[!] sort failed: {in_path}")

# --------------------------------------------------------------------
# Phase 2: process shard by shard and write features directly
#   - intervals.sorted -> concurrency peak per job
#   - jobstats.sorted  -> reduce: count, cpu_sum, cpu_cnt, min_start
#   - jobmachines.sorted -> unique per job,machine then sum_netin, mcount
#   - local join on 'job', final computation, append to OUTPUT_FILE
# --------------------------------------------------------------------
print("[i] Reducing per shard and writing features ...")
out_first = True

for i in range(N_SHARDS):
    p_int_in  = int_paths[i]
    p_js_in   = js_paths[i]
    p_jm_in   = jm_paths[i]

    p_int_s  = p_int_in + ".sorted"
    p_js_s   = p_js_in  + ".sorted"
    p_jm_s   = p_jm_in  + ".sorted"

    # Sort
    if os.path.getsize(p_int_in) > 0:
        run_sort(p_int_in, p_int_s, "-k1,1 -k2,2n")
    else:
        open(p_int_s, "w").close()
    if os.path.getsize(p_js_in) > 0:
        run_sort(p_js_in,  p_js_s,  "-k1,1")
    else:
        open(p_js_s, "w").close()
    if os.path.getsize(p_jm_in) > 0:
        run_sort(p_jm_in,  p_jm_s,  "-k1,1 -k2,2")
    else:
        open(p_jm_s, "w").close()

    # a) concurrency per job
    conc = {}
    with open(p_int_s, "r", newline="") as fi:
        cur_job, heap, peak = None, [], 0
        def flush_job(j, pk):
            if j is not None:
                conc[j] = pk
        for line in fi:
            if not line:
                continue
            try:
                j, a_str, b_str = line.rstrip("\n").split(",", 2)
                a = int(a_str); b = int(b_str)
            except:
                continue
            if j != cur_job:
                flush_job(cur_job, peak)
                cur_job, heap, peak = j, [], 0
            while heap and heap[0] <= a:
                heapq.heappop(heap)
            heapq.heappush(heap, b)
            if len(heap) > peak:
                peak = len(heap)
        flush_job(cur_job, peak)

    # b) reduce jobstats -> count, cpu_sum, cpu_cnt, min_start
    stats = {}
    with open(p_js_s, "r", newline="") as fj:
        rj = csv.reader(fj)
        last, cnt, cpu_sum, cpu_cnt, min_start = None, 0, 0.0, 0, None
        for row in rj:
            if not row or len(row) < 3:
                continue
            j, cpu_str, st_str = row[0], row[1], row[2]
            try:
                cpu = float(cpu_str); st = int(st_str)
            except:
                continue
            if j != last and last is not None:
                stats[last] = (cnt, cpu_sum, cpu_cnt, min_start)
                cnt, cpu_sum, cpu_cnt, min_start = 0, 0.0, 0, None
            last = j
            cnt += 1
            cpu_sum += cpu
            cpu_cnt += 1
            eff_start = st if st >= WSTART else WSTART
            if min_start is None or eff_start < min_start:
                min_start = eff_start
        if last is not None:
            stats[last] = (cnt, cpu_sum, cpu_cnt, min_start)

    # c) jobmachines unique -> sum_netin, mcount
    loc_aggr = {}
    with open(p_jm_s, "r", newline="") as fm:
        rm = csv.reader(fm)
        last_job, last_mid = None, None
        sum_net, mcount = 0.0, 0
        for row in rm:
            if not row or len(row) < 2:
                continue
            j, mid = row[0], row[1]
            if j != last_job:
                if last_job is not None:
                    loc_aggr[last_job] = (sum_net, mcount)
                last_job, last_mid = j, None
                sum_net, mcount = 0.0, 0
            # uniqueness on (job,machine) thanks to sorting
            if mid != last_mid:
                last_mid = mid
                if mid in machine_avg_netin:
                    sum_net += machine_avg_netin[mid]
                    mcount += 1
        if last_job is not None:
            loc_aggr[last_job] = (sum_net, mcount)

    # d) local join and write to OUTPUT
    mode = "w" if out_first else "a"
    with open(OUTPUT_FILE, mode, newline="") as fout:
        w = csv.writer(fout)
        if out_first:
            w.writerow(["dataset","freq","conc","cpuRatio","age","locality"])
            out_first = False

        # iterate over all keys seen in this shard
        keys = set()
        keys.update(stats.keys())
        keys.update(conc.keys())
        keys.update(loc_aggr.keys())

        for j in keys:
            cnt, cpu_sum, cpu_cnt, min_start = stats.get(j, (0,0.0,0,None))
            freq = cnt
            pc = conc.get(j, 0)
            job_cpu_avg = (cpu_sum / cpu_cnt) if cpu_cnt else 0.0
            cpuRatio = (job_cpu_avg / global_cpu_avg) if global_cpu_avg > 0 else 0.0
            if min_start is None:
                age = 0
            else:
                age = WEND - min_start
                if age < 0:
                    age = 0
            s_net, m_cnt = loc_aggr.get(j, (0.0,0))
            if m_cnt > 0:
                avg_remote_in = s_net / m_cnt
                loc = 1.0 - (avg_remote_in / 100.0)
                if loc < 0.0: loc = 0.0
                if loc > 1.0: loc = 1.0
            else:
                loc = ""
            w.writerow([j, freq, pc, cpuRatio, age, loc])

    # shard cleanup
    for p in (p_int_in, p_int_s, p_js_in, p_js_s, p_jm_in, p_jm_s):
        try:
            os.remove(p)
        except:
            pass

# Final cleanup
try:
    os.rmdir(workdir)
except:
    pass

print("[i] Done ->", OUTPUT_FILE)
