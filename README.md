# CLEAR: CLustering-Enabled Adaptive Replication for HDFS

CLEAR is an adaptive replication framework for the Hadoop Distributed File System (HDFS).

The project combines:

- multi-feature file characterization,
- X-Means clustering with K-means++ initialization,
- semantic classification into Hot, Shared, Moderate, and Archival categories,
- confidence-based replication-factor selection,
- RS(6,3) erasure coding for archival data,
- locality-aware replica relocation for shared files,
- offline trace-driven evaluation,
- online HDFS evaluation.

The repository also contains the baseline implementations, evaluation scripts, sensitivity and ablation material, and the supplementary appendices associated with the CLEAR study.

---

## 1. Repository Structure

```text
Source/
|
|-- Applying strategy/
|   |-- CLEAR/
|   |-- DRPMLC/
|   |-- ERMS/
|   `-- SBR/
|
|-- Evaluation/
|   |-- ablation/
|   |-- sensitivity/
|   |-- MapReduce Job/
|   `-- ...
|
|-- Appendix_1 Illustrative Example of CLEAR.pdf
|-- Appendix_2 Per-policy Online Behavior.pdf
`-- README.md
```

All paths below are relative to the repository root.

---

## 2. CLEAR Implementation

Path:

```text
Applying strategy/CLEAR/
```

### 2.1 Trace Preparation

```text
1-Generate Day Batch instance and machine usage.txt
```

Instructions for extracting daily subsets from the Alibaba Cluster Trace.

```text
2-Generate_machine_used.txt
```

Instructions for restricting machine telemetry to machines involved in the selected workload window.

---

### 2.2 Feature Construction

```text
3-build_features_extsort_sharded.py
```

Builds per-file features from the trace using external sorting and disk-based sharding.

The feature-processing stage derives the signals used by CLEAR, including:

- access frequency,
- file age,
- write activity,
- locality,
- access concurrency.

The script is designed to process large trace files without loading the complete dataset into memory.

---

### 2.3 CLEAR Pipeline

```text
4-run_pipeline.py
```

High-level driver for the CLEAR offline pipeline.

It coordinates the main processing stages, including:

1. feature preparation,
2. normalization,
3. clustering,
4. semantic category assignment,
5. replication-factor assignment.

---

### 2.4 X-Means Clustering and Category Assignment

```text
5-xmeans_label.py
```

Implements the clustering and semantic labelling stage.

Main responsibilities:

- load normalized features,
- run X-Means with K-means++ initialization,
- apply the cluster-size constraint,
- compute global and per-cluster statistics,
- assign each cluster to one of the CLEAR categories:
  - Hot,
  - Shared,
  - Moderate,
  - Archival.

Typical outputs include:

```text
dayX_clusters.csv
dayX_centroids.csv
dayX_cluster_labels.csv
dayX_cluster_debug.csv
dayX_cluster_summary.txt
```

---

### 2.5 Replication-Factor Assignment

```text
6-assign_rf_per_cluster.py
```

Assigns a replication level to each cluster according to its semantic category and confidence score.

CLEAR uses category-specific replication bounds and a strong-fit rule to decide whether a cluster keeps the minimum replication level or receives one additional replica within the configured limit.

For archival data, the offline model also accounts for RS(6,3) erasure coding.

Typical output:

```text
dayX_cluster_rf.csv
```

---

### 2.6 Shared Replica Placement

```text
7-shared_placement.py
```

Builds the placement plan for files classified as Shared.

The placement stage aims to improve locality while preserving operational constraints such as:

- rack diversity,
- available capacity,
- node health,
- relocation thresholds,
- movement budgets.

The module relocates existing replicas rather than increasing the replication factor of Shared files.

---

### 2.7 Sensitivity Analysis

```text
8-run_sensitivity.py
```

Runs the Day2 sensitivity analysis used in the manuscript.

The analysis varies selected clustering parameters while keeping the remaining configuration unchanged.

Result file:

```text
Evaluation/sensitivity/day2_sensitivity.csv
```

---

## 3. Baseline Strategies

The repository contains the baseline implementations used in the evaluation.

### 3.1 DRPMLC

Path:

```text
Applying strategy/DRPMLC/
```

Main script:

```text
drpmlc_strategy.py
```

Implements the DRPMLC replication policy and generates per-file replication decisions compatible with the evaluation pipeline.

---

### 3.2 ERMS

Path:

```text
Applying strategy/ERMS/
```

Main script:

```text
erms_strategy.py
```

Implements the ERMS policy and produces replication decisions from file activity information.

---

### 3.3 SBR

Path:

```text
Applying strategy/SBR/
```

Main script:

```text
sbr_strategy.py
```

Implements the support-based replication policy used as a comparison baseline.

---

## 4. Evaluation Directory

Path:

```text
Evaluation/
```

This directory contains the files and scripts used for the Day2 evaluation and the online HDFS workload.

It includes:

- normalized Day2 features,
- cluster assignments,
- CLEAR replication decisions,
- baseline replication decisions,
- top-file selection files,
- per-strategy replication lists,
- HDFS application scripts,
- MapReduce workload material,
- sensitivity results,
- ablation results.

Typical Day2 files include:

```text
day2_norm.csv
day2_clusters.csv
day2_cluster_labels.csv
day2_cluster_rf.csv
day2_clear_rf_10k.csv
day2_drpmlc_rf.csv
day2_erms_rf.csv
day2_sbr_rf.csv
top10000_day2.txt
```

---

## 5. Sensitivity Analysis

Path:

```text
Evaluation/sensitivity/
```

Result file:

```text
day2_sensitivity.csv
```

The sensitivity analysis examines the effect of selected X-Means configuration parameters on the CLEAR decision pipeline.

The CSV contains the configurations evaluated in the manuscript together with the corresponding category distribution and storage-overhead indicators.

---

## 6. Ablation Analysis

Path:

```text
Evaluation/ablation/
```

Result file:

```text
day2_ablation.csv
```

The ablation analysis separates the contribution of the main CLEAR control components.

The evaluated variants are:

- **Full CLEAR**: confidence-based replication-factor selection and replica placement enabled.
- **No-placement**: CLEAR replication decisions retained while replica relocation is disabled.
- **Fixed-RF**: CLEAR category assignments retained while the minimum configured replication level is used for each category.

The CSV contains the ablation results reported in the manuscript.

---

## 7. Online HDFS Evaluation

The online evaluation uses a common Day2 workload for all compared strategies.

The workflow includes:

1. selecting the target Day2 files,
2. materializing the files in HDFS,
3. preparing the per-strategy replication plan,
4. applying the replication factors,
5. running the same weighted read workload under each strategy,
6. collecting system and job-level measurements.

The compared strategies are:

- HDFS default,
- CLEAR,
- DRPMLC,
- ERMS,
- SBR.

Collected information includes:

- YARN job history,
- MapReduce counters,
- HDFS capacity reports,
- HDFS read information,
- CPU activity,
- memory usage,
- network traffic,
- per-node monitoring information.

---

## 8. MapReduce Workload

Path:

```text
Evaluation/MapReduce Job/
```

This directory contains the MapReduce workload and the related artifacts used during the online evaluation.

The workload replays weighted accesses to the selected Day2 HDFS files so that all strategies are evaluated under the same logical demand.

---

## 9. Supplementary Appendices

Two supplementary documents are provided at the repository root.

### Appendix 1

```text
Appendix_1 Illustrative Example of CLEAR.pdf
```

Provides an illustrative example of the CLEAR decision process, including the main clustering, classification, replication, and placement stages.

### Appendix 2

```text
Appendix_2 Per-policy Online Behavior.pdf
```

Provides additional online execution and monitoring material for the evaluated replication strategies.

---

## 10. Typical Day2 Workflow

### Step 1: Prepare the Day2 trace files

Follow:

```text
Applying strategy/CLEAR/1-Generate Day Batch instance and machine usage.txt
Applying strategy/CLEAR/2-Generate_machine_used.txt
```

### Step 2: Build features

Use:

```text
Applying strategy/CLEAR/3-build_features_extsort_sharded.py
```

to generate the Day2 feature table.

### Step 3: Normalize the feature table

Generate:

```text
Evaluation/day2_norm.csv
```

### Step 4: Cluster files and assign categories

Example:

```bash
python3 "Applying strategy/CLEAR/5-xmeans_label.py" \
    --norm Evaluation/day2_norm.csv \
    --out Evaluation \
    --kmin 4 \
    --kmax 500 \
    --m-min 250 \
    --seed 42 \
    --epsz 1e-6
```

### Step 5: Assign replication factors

```bash
python3 "Applying strategy/CLEAR/6-assign_rf_per_cluster.py" \
    --labels Evaluation/day2_cluster_labels.csv \
    --out Evaluation/day2_cluster_rf.csv
```

### Step 6: Build the top-file replication plans

Prepare the per-file replication-factor lists for:

```text
HDFS default
CLEAR
DRPMLC
ERMS
SBR
```

### Step 7: Apply the replication policy in HDFS

Use the scripts in:

```text
Evaluation/
```

to apply the appropriate replication configuration.

### Step 8: Run the online workload

Run the MapReduce workload under each strategy and collect the corresponding HDFS, YARN, and monitoring information.

### Step 9: Run the sensitivity analysis

```bash
python3 "Applying strategy/CLEAR/8-run_sensitivity.py"
```

The summarized sensitivity output is stored in:

```text
Evaluation/sensitivity/day2_sensitivity.csv
```

---

## 11. Reproducibility Material

The repository contains the main artifacts associated with the CLEAR study:

- CLEAR implementation scripts,
- baseline implementations,
- Day2 evaluation files,
- sensitivity-analysis output,
- ablation-analysis output,
- MapReduce workload material,
- HDFS evaluation files,
- supplementary appendices.

The repository is organized so that each stage has explicit input and output files, allowing the main experimental pipeline to be inspected and rerun independently.

---

## 12. Main Supplementary Files

```text
Appendix_1 Illustrative Example of CLEAR.pdf
Appendix_2 Per-policy Online Behavior.pdf
Evaluation/sensitivity/day2_sensitivity.csv
Evaluation/ablation/day2_ablation.csv
```

---

## 13. Repository

Public repository:

https://github.com/clear-hdfs/Source
