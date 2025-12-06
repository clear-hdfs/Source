#!/usr/bin/env python3
import csv, sys, subprocess, os, heapq

# Usage:
# python3 build_features_extsort.py <BATCH_FILE> <USAGE_FILE> <OUTPUT_FILE> <WINDOW_START> <WINDOW_END>
# ex Day1:
#   python3 build_features_extsort.py batch_instance_day1.csv machine_usage_day1_used.csv features_day1.csv 0 86400
# ex Day2:
#   python3 build_features_extsort.py batch_instance_day2.csv machine_usage_day2_used.csv features_day2.csv 86400 172800

if len(sys.argv) != 6:
    print("Usage: python3 build_features_extsort.py <BATCH_FILE> <USAGE_FILE> <OUTPUT_FILE> <WINDOW_START> <WINDOW_END>")
    sys.exit(1)

BATCH_FILE   = sys.argv[1]
USAGE_FILE   = sys.argv[2]
OUTPUT_FILE  = sys.argv[3]
WINDOW_START = int(sys.argv[4])
WINDOW_END   = int(sys.argv[5])

print(f"[i] Batch={BATCH_FILE}  Usage={USAGE_FILE}  Out={OUTPUT_FILE}  Window=[{WINDOW_START},{WINDOW_END})")

# 1) First pass over batch: per-job stats + dump of clipped intervals to disk
job_stats = {}   # job -> {count,cpu_sum,cpu_cnt,min_start,machines:set}
global_cpu_sum = 0.0
global_cpu_cnt = 0

intervals_tmp = "intervals.tmp.csv"
with open(BATCH_FILE, "r", newline="") as f_in, open(intervals_tmp, "w", newline="") as f_iv:
    r = csv.reader(f_in)
    w_iv = csv.writer(f_iv)
    _ = next(r, None)  # header
    for row in r:
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

        st_job = job_stats.get(job)
        if st_job is None:
            st_job = {"count":0, "cpu_sum":0.0, "cpu_cnt":0, "min_start":max(st, WINDOW_START), "machines": set()}
            job_stats[job] = st_job

        st_job["count"]   += 1
        st_job["cpu_sum"] += cpu_avg
        st_job["cpu_cnt"] += 1
        eff_start = st if st >= WINDOW_START else WINDOW_START
        if eff_start < st_job["min_start"]:
            st_job["min_start"] = eff_start
        st_job["machines"].add(mid)

        # clip to the time window for concurrency
        a = st if st > WINDOW_START else WINDOW_START
        b = en if en < WINDOW_END else WINDOW_END
        if a < b:
            # job_name,start,end
            w_iv.writerow([job, a, b])

global_cpu_avg = (global_cpu_sum / global_cpu_cnt) if global_cpu_cnt else 1.0
print(f"[i] Jobs={len(job_stats)}  CPUavg_global={global_cpu_avg:.6f}")

# 2) External sort of intervals by job, then start
intervals_sorted = "intervals.sorted.csv"
print("[i] External sort of intervals...")
# -t,  comma separator
# -k1,1 sort on column 1 (job) then -k2,2n numeric sort on start
# LC_ALL=C for speed
cmd = ["bash","-lc", f"export LC_ALL=C; : ${{TMPDIR:=/mnt/c/tmp}}; mkdir -p \"$TMPDIR\"; "
                     f"sort -t, -k1,1 -k2,2n -S 32M --parallel=1 -T \"$TMPDIR\" "
                     f"'{intervals_tmp}' > '{intervals_sorted}'"]

ret = subprocess.run(cmd)
if ret.returncode != 0:
    print("[!] External sort failed")
    sys.exit(2)
try:
    os.remove(intervals_tmp)
except:
    pass

# 3) Compute concurrency per job in one pass with a min-heap of end times
print("[i] Computing concurrencies per job...")
job_conc = {}  # job -> peak
with open(intervals_sorted, "r", newline="") as f_iv:
    r = csv.reader(f_iv)
    cur_job = None
    heap = []   # min-heap of end times for the current job
    peak = 0

    def flush_job(j):
        if j is None:
            return
        job_conc[j] = peak

    for row in r:
        if not row or len(row) < 3:
            continue
        j, a_str, b_str = row[0], row[1], row[2]
        try:
            a = int(a_str); b = int(b_str)
        except:
            continue
        if j != cur_job:
            # finalize previous job
            flush_job(cur_job)
            # reset state for the new job
            cur_job = j
            heap = []
            peak = 0

        # pop all end times <= a
        while heap and heap[0] <= a:
            heapq.heappop(heap)
        # push current end time
        heapq.heappush(heap, b)
        if len(heap) > peak:
            peak = len(heap)

    # last job
    flush_job(cur_job)

try:
    os.remove(intervals_sorted)
except:
    pass

print(f"[i] Concurrencies computed for {len(job_conc)} jobs")

# 4) Average net_in per machine
print("[i] Computing average net_in per machine...")
machine_sum = {}
machine_cnt = {}
with open(USAGE_FILE, "r", newline="") as f_u:
    r = csv.reader(f_u)
    _ = next(r, None)   # header
    for row in r:
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

# 5) Write features
print(f"[i] Writing {OUTPUT_FILE}")
with open(OUTPUT_FILE, "w", newline="") as out:
    w = csv.writer(out)
    w.writerow(["dataset","freq","conc","cpuRatio","age","locality"])

    for job, st in job_stats.items():
        freq = st["count"]
        conc = job_conc.get(job, 0)

        job_cpu_avg = (st["cpu_sum"] / st["cpu_cnt"]) if st["cpu_cnt"] else 0.0
        cpuRatio = (job_cpu_avg / global_cpu_avg) if global_cpu_avg > 0 else 0.0

        age = WINDOW_END - st["min_start"]
        if age < 0:
            age = 0

        vals = [machine_avg_netin[m] for m in st["machines"] if m in machine_avg_netin]
        if vals:
            avg_remote_in = sum(vals) / len(vals)
            loc = 1.0 - (avg_remote_in / 100.0)
            if   loc < 0.0: loc = 0.0
            elif loc > 1.0: loc = 1.0
        else:
            loc = ""

        w.writerow([job, freq, conc, cpuRatio, age, loc])

print("[i] Done")
