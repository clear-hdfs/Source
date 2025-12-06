Project: CLEAR - Clustering based Locality Enhanced Adaptive Replication for HDFS
==============================================================================

All paths below are relative to the project root.  
In the archive, this root corresponds to the directory:  Source/


1. Goal of the project
----------------------

This code base contains everything needed to:

- Build per file features from Alibaba 2018 cluster traces (batch_instance.csv, machine_usage.csv).
- Cluster files with X-Means++ and assign a semantic category per cluster:
  Hot, Shared, Moderate, Archival.
- Derive integer replication factors per cluster for the CLEAR strategy.
- Implement three baseline replication strategies:
  DRPMLC, ERMS, SBR.
- Apply each replication policy to a synthetic HDFS workload of 10 000 files.
- Run a read intensive MapReduce job (WeightedHeavyRead) to compare strategies
  in terms of job time, throughput and resource usage.


2. Repository layout
--------------------

Root (Source/)
  - build_features_extsort.py
      External sort based feature builder from Alibaba traces.

  - Applying strategy/
      Scripts to compute and apply replication policies.

  - Evaluation/
      Scripts, intermediate CSV files and MapReduce job for the 10k file
      evaluation on Day 2.


2.1. Applying strategy/
-----------------------

Path:  Source/Applying strategy/

This folder groups all replication strategies.

2.1.1 CLEAR
-----------

Path:  Source/Applying strategy/CLEAR/

Files:

  1-Generate Day Batch instance and machine usage.txt
      Text instructions to extract per day subsets from the Alibaba traces.
      It explains how to build, for each day:
        - batch_instance_dayX.csv
        - machine_usage_dayX.csv
      by cutting the global windows into:
        Day1 = [0, 86400)
        Day2 = [86400, 172800)
        Day3 = [172800, 259200)

  2-Generate_machine_used.txt
      Text instructions to restrict machine telemetry to the machines
      that appear in batch_instance_dayX.csv. Typical outputs are:
        - machines_used_dayX.txt
        - machine_usage_dayX_used.csv
      This keeps only relevant machines and reduces telemetry size.

  3-build_features_extsort_sharded.py
      Python feature builder that works in streaming mode with external sort.
      Inputs:
        - batch_instance_dayX.csv
        - machine_usage_dayX_used.csv
        - a time window [WSTART, WEND)
      Output:
        - features_dayX.csv with one line per job or per file, and columns:
            dataset,freq,conc,cpuRatio,age,locality
      The script shards intermediate data on disk, uses Unix sort and
      merges results in order to stay within memory limits. Environment
      variables SHARDS, TMPDIR, SORT_MEM and SORT_PAR control sharding and
      sort parameters.

  4-run_pipeline.py
      High level driver for the CLEAR offline pipeline for a given day.
      It coordinates the following steps:
        - feature extraction with 3-build_features_extsort_sharded.py,
        - global statistics and normalisation of features,
        - clustering and labelling with X-Means++,
        - replication factor assignment per cluster.
      See the command line help of this script for exact usage.

  5-xmeans_label.py
      Implementation of X-Means++ clustering with BIC split criterion and
      semantic labelling.

      Main steps:
        - Load normalised features (e.g. day2_norm.csv) with columns:
            dataset,freq,conc,cpuRatio,age,locality
        - Run X-Means in split only mode between Kmin and Kmax with:
            - k-means++ initialisation,
            - Lloyd iterations,
            - BIC spherical model per cluster,
            - minimum cluster size m_min.
        - Compute robust statistics:
            - global median and MAD per feature,
            - per cluster medians,
            - mixed centres M_i per feature (C_i = median or mixed centre).
        - Compute category scores for each cluster:
            Hot, Shared, Moderate, Archival.
          Scores are based on:
            - delta = C_i - C_global,
            - z scores using MAD,
            - positive contributions for relevant features in each category,
            - a closeness based score for Moderate.

      Outputs:
        - dayX_clusters.csv
            dataset,cluster
        - dayX_centroids.csv
            cluster,mean_freq,mean_conc,mean_cpuRatio,mean_age,mean_locality,n
        - dayX_cluster_labels.csv
            cluster,label,S_hot,S_shared,S_moderate,S_archival,s_star,phi_shared,n
        - dayX_cluster_debug.csv
            Detailed per cluster information:
            mixed centre M, delta, z, scores, final label.
        - dayX_cluster_summary.txt
            Summary of how many clusters and files fall in each category.

  6-assign_rf_per_cluster.py
      Replication factor assignment per cluster, based on the semantic
      label and the strength of the fit.

      Inputs:
        - dayX_cluster_labels.csv
          with at least columns:
            cluster,label,s_star,n

      Logic:
        - For each category k in {Hot, Shared, Moderate, Archival},
          collect s_star values of clusters that have label k.
        - Compute, per category:
            med_k  = median(s_star)
            MAD_k  = median absolute deviation of s_star
        - For each cluster i of category k:
            - strong_fit if s_star_i >= med_k + MAD_k
            - R_min[k] and R_max[k] are predefined per category:
                Hot:      R_min=4, R_max=5
                Shared:   R_min=3, R_max=3
                Moderate: R_min=2, R_max=3
                Archival: R_min=0, R_max=1
            - If strong_fit and R_min < R_max, then R_i = R_min + 1,
              otherwise R_i = R_min.

      Output:
        - dayX_cluster_rf.csv with columns:
            cluster,label,s_star,med_k,MAD_k,strong_fit,R_min,R_max,R_i,n

  7-shared_placement.py
      Script that builds the final CLEAR placement plan for shared data.
      It is responsible for:
        - mapping cluster level replication factors R_i to per file
          replication and placement decisions,
        - ensuring that Shared clusters benefit from a balanced and
          locality aware placement,
        - preparing the CSV mapping used later for HDFS setrep calls.

      See this script for concrete input and output formats.


2.1.2 DRPMLC
------------

Path:  Source/Applying strategy/DRPMLC/

  drpmlc_strategy.py
      Implementation of the DRPMLC baseline (Dynamic Replication Policy
      with Machine Learning and Clustering).

      Typical responsibilities:
        - load per file statistics,
        - apply the DRPMLC decision logic to assign a replication factor
          per file,
        - write a CSV of the form:
              file_id,rf
          that will be consumed by the generic evaluation scripts.


2.1.3 ERMS
-----------

Path:  Source/Applying strategy/ERMS/

  erms_strategy.py
      Implementation of the ERMS baseline (Elastic Replication Management
      System).

      Typical responsibilities:
        - read load or popularity metrics per file,
        - map them to discrete RF levels according to the ERMS policy,
        - output a CSV:
              file_id,rf
          compatible with the evaluation tooling.


2.1.4 SBR
----------

Path:  Source/Applying strategy/SBR/

  sbr_strategy.py
      Implementation of the SBR baseline (Support Based Replication).

      Typical responsibilities:
        - compute a demand support metric for each HDFS block or file,
        - assign a discrete RF, usually in a small set such as {4,3,2},
        - optionally favour nodes with higher local support for placement,
        - export a CSV:
              file_id,rf
          that can be filtered and applied on the 10k file workload.


2.2. Root level feature builder
-------------------------------

Path:  Source/build_features_extsort.py

This script is an external sort based feature builder for Alibaba traces.
It has the same goal as 3-build_features_extsort_sharded.py but is kept
at the root for convenience. It reads:

  - batch_instance_dayX.csv
  - machine_usage_dayX_used.csv

and produces:

  - features_dayX.csv

with the same columns as described previously:

  dataset,freq,conc,cpuRatio,age,locality

Exact command line usage is given in the script help.


3. Evaluation/
--------------

Path:  Source/Evaluation/

This directory contains everything that is specific to the Day 2 evaluation
on the 10 000 most popular files, plus the MapReduce job used to generate
the read intensive workload.

The workflow is the following:

  1. Identify the 10 000 most frequent files from Day 2 and normalised
     features (day2_norm.csv).
  2. Create 10 000 synthetic text files of about 64 MB each in HDFS.
  3. Build per file replication factor lists for each strategy.
  4. Apply replication factors in HDFS.
  5. Run the WeightedHeavyRead MapReduce job under each strategy.


3.1. Files and scripts in Evaluation/
-------------------------------------

  1-Generate 10000 files from day2norm.txt
      Text file that documents how the list of top 10 000 files was derived
      from day2_norm.csv. It explains which columns are used (access
      frequency and percentile) and how to extract:
        - top10000_day2.txt
        - 10000_with_freq_percentile.csv

  10000_with_freq_percentile.csv
      CSV containing, for each candidate file, its frequency and possibly
      a percentile or rank. It is an intermediate file used to build
      the final top10000 list.

  2-Create 10000 files on Datanodes.txt
      Text instructions to generate actual HDFS files under:
        /exp/day2/data
      For each file_id in top10000_day2.txt, a synthetic text file is
      created, usually with a helper script similar to:
        gen_text_files_hdfs_day2.py
      Each file is around 64 MB and contains random words from a small
      vocabulary related to Hadoop.

  3-filter_rf_by_list.py
      Python script that filters an RF CSV by a list of file_id.

      Command line:
        --top      text file with one file_id per line
                   (for example top10000_day2.txt)
        --rf       input CSV containing at least the columns that hold
                   file_id and rf
        --col-id   column name that contains the file_id in the RF CSV
        --col-rf   column name that contains the RF value in the RF CSV
        --out      output CSV with exactly two columns: file_id,rf

      Example:
        python3 3-filter_rf_by_list.py \
            --top top10000_day2.txt \
            --rf day2_sbr_rf.csv \
            --col-id file_id \
            --col-rf rf \
            --out day2_sbr_rf_10k.csv

  4-build_clear_rf_10k.py
      Build a file_id,rf CSV for CLEAR from cluster level RF.

      Inputs:
        - top10000_day2.txt
            list of file_id
        - day2_clusters.csv
            dataset,cluster mapping for all files
        - day2_cluster_rf.csv
            cluster,R_i for CLEAR

      Command line options:
        --top                  list of file_id (top10000_day2.txt)
        --labels               day2_clusters.csv (file -> cluster)
        --clusters             day2_cluster_rf.csv (cluster -> rf)
        --col-file             column name for file_id in labels
                               default: dataset
        --col-cluster-label    column name for cluster in labels
                               default: cluster
        --col-cluster-rf       column name for cluster in clusters
                               default: cluster
        --col-rf               column name for rf in clusters
                               default: rf
        --out                  output CSV file_id,rf
                               for example day2_clear_rf_10k.csv

      Example:
        python3 4-build_clear_rf_10k.py \
            --top top10000_day2.txt \
            --labels day2_clusters.csv \
            --clusters day2_cluster_rf.csv \
            --out day2_clear_rf_10k.csv

  5-apply_rf_from_csv.py
      Apply replication factors in HDFS from a file_id,rf CSV.

      Command line options:
        --csv       input CSV with columns file_id,rf
        --base      base HDFS directory that contains the files
                    for example /exp/day2/data
        --dry-run   if set, print the hdfs dfs -setrep commands
                    without executing them

      For each record, the script builds:
        hdfs_path = <base>/<file_id>
      and runs:
        hdfs dfs -setrep -w <rf> <hdfs_path>

      Example:
        python3 5-apply_rf_from_csv.py \
            --csv day2_sbr_rf_10k.csv \
            --base /exp/day2/data

  apply_clear_rf.sh
      Small shell helper that runs the CLEAR specific RF application
      pipeline. It typically chains:
        - 4-build_clear_rf_10k.py
        - 5-apply_rf_from_csv.py
      for the CLEAR strategy.

  day2_norm.csv
      Normalised feature file for Day 2, with columns:
        dataset,freq,conc,cpuRatio,age,locality
      It is the input of 5-xmeans_label.py.

  day2_clusters.csv
      Output of 5-xmeans_label.py with:
        dataset,cluster

  day2_cluster_labels.csv
      Output of 5-xmeans_label.py containing, for each cluster:
        cluster,label,S_hot,S_shared,S_moderate,S_archival,s_star,phi_shared,n

  day2_cluster_rf.csv
      Output of 6-assign_rf_per_cluster.py with:
        cluster,label,s_star,med_k,MAD_k,strong_fit,R_min,R_max,R_i,n

  day2_clear_rf_10k.csv
      CLEAR replication factors restricted to the 10k most frequent files,
      built by 4-build_clear_rf_10k.py.

  day2_clear_path_rf_10k.csv
      Variant that stores, for each of the 10k files, the HDFS path and
      the RF used for the CLEAR strategy. Used for logging and sanity checks.

  day2_default_rf_10k.csv
      Reference RF for the default HDFS configuration (for example RF 3
      for all files) restricted to the 10k files.

  day2_drpmlc_rf.csv
      RF per file for the DRPMLC baseline on the full dataset.

  day2_drpmlc_rf_10k.csv
      Same as above, but restricted to the 10k files using 3-filter_rf_by_list.py.

  day2_erms_rf.csv
      RF per file for the ERMS baseline on the full dataset.

  day2_erms_rf_10k.csv
      Same as above, but restricted to the 10k files.

  day2_sbr_rf.csv
      RF per file for the SBR baseline on the full dataset.

  day2_sbr_rf_10k.csv
      Same as above, but restricted to the 10k files.

  top10000_day2.txt
      Plain text file with one file_id per line. These are the 10 000
      most frequently accessed files from Day 2 according to the
      pre processing described in the text notes.


3.2. MapReduce Job
------------------

Path:  Source/Evaluation/MapReduce Job/

Files:

  WeightedHeavyRead.java
      Java MapReduce job that generates a heavy read workload with weights.
      It reads a small CSV file of the form:
        path,weight,nreads
      and, for each record, reads the HDFS file at "path" nreads times,
      accumulating word counts scaled by "weight". This simulates skewed,
      popularity based read patterns over the same set of paths.

  WeightedHeavyRead.class
  WeightedHeavyRead$HeavyMapper.class
  WeightedHeavyRead$HeavyCounter.class
      Compiled classes of the MapReduce job.

  weightedheavyread.jar
      Packaged MapReduce jar ready to be submitted with:
        hadoop jar weightedheavyread.jar WeightedHeavyRead <args>

Typical use in the experiments:

  - prepare a weights file for the 10k files under /exp/day2/data,
    all strategies share the same logical demand (same number of
    reads per file, same weights),
  - run:
      hadoop jar weightedheavyread.jar WeightedHeavyRead \
          -D mapred.some.config=value \
          /exp/day2/data /exp/day2/out \
          -weights /path/to/weights.csv

  - repeat the same job under:
      - default HDFS RF,
      - CLEAR RF,
      - DRPMLC RF,
      - ERMS RF,
      - SBR RF,
    and compare:
      - job execution time,
      - HDFS throughput,
      - per node CPU, disk, and network metrics from Ganglia.


4. Typical end to end workflow
------------------------------

Below is a high level summary of how all pieces fit together for Day 2.

  1. Extract per day traces
       - Follow the instructions in:
           Applying strategy/CLEAR/1-Generate Day Batch instance and machine usage.txt
           Applying strategy/CLEAR/2-Generate_machine_used.txt
       - Obtain:
           batch_instance_day2.csv
           machine_usage_day2_used.csv

  2. Build features
       - From the project root:
           python3 "Applying strategy/CLEAR/3-build_features_extsort_sharded.py" \
               batch_instance_day2.csv machine_usage_day2_used.csv \
               features_day2.csv 86400 172800
       - Adjust WSTART and WEND according to the chosen day.

  3. Normalise features
       - Use your normalisation script to produce:
           day2_norm.csv
         from features_day2.csv.

  4. Cluster and label
       - Run X-Means++ to obtain clusters and labels:
           python3 "Applying strategy/CLEAR/5-xmeans_label.py" \
               --norm day2_norm.csv \
               --out . \
               --kmin 4 --kmax 8 --m-min 20
       - This produces:
           day2_clusters.csv
           day2_cluster_labels.csv
           day2_cluster_debug.csv
           day2_cluster_summary.txt

  5. Assign replication factors per cluster (CLEAR)
       - Run:
           python3 "Applying strategy/CLEAR/6-assign_rf_per_cluster.py" \
               --labels day2_cluster_labels.csv \
               --out day2_cluster_rf.csv

  6. Build per file RF for CLEAR on the 10k files
       - Run:
           python3 "Evaluation/4-build_clear_rf_10k.py" \
               --top Evaluation/top10000_day2.txt \
               --labels Evaluation/day2_clusters.csv \
               --clusters Evaluation/day2_cluster_rf.csv \
               --out Evaluation/day2_clear_rf_10k.csv

  7. Build per file RF for baselines on the 10k files
       - Use DRPMLC, ERMS and SBR scripts to produce global RF CSVs, then
         restrict them to the 10k files:
           python3 "Evaluation/3-filter_rf_by_list.py" \
               --top Evaluation/top10000_day2.txt \
               --rf Evaluation/day2_sbr_rf.csv \
               --col-id file_id \
               --col-rf rf \
               --out Evaluation/day2_sbr_rf_10k.csv
         and similarly for ERMS and DRPMLC.

  8. Apply RFs in HDFS
       - For each strategy S in {default, CLEAR, DRPMLC, ERMS, SBR}:
           python3 "Evaluation/5-apply_rf_from_csv.py" \
               --csv Evaluation/day2_<S>_rf_10k.csv \
               --base /exp/day2/data
         Run with --dry-run first to check commands.

  9. Run WeightedHeavyRead and collect metrics
       - Prepare a common weights.csv for the 10k files.
       - Submit the job under each strategy using weightedheavyread.jar.
       - Collect:
           - job runtime from YARN and job logs,
           - per node metrics from Ganglia (CPU, disk I/O, network),
           - HDFS read throughput.

This completes the experimental pipeline comparing CLEAR with DRPMLC,
ERMS, SBR and the default HDFS replication policy on a realistic
10k file workload derived from Alibaba traces.


5. Remarks
----------

- All scripts print a small help message when called with -h or --help.
- The exact paths for the Alibaba traces and HDFS directories are not
  hard coded in this readme and must be adapted to the local cluster.
- The code is organised so that each step has a clear input and output
  CSV, which simplifies debugging and re running individual phases.
