# RAP FGCS handoff package

This package is for continuing the paper in a new conversation or local Python environment.

## Included

- `HANDOFF_RAP_FGCS.md`: complete scientific and experimental recap.
- `scripts/figure_style.py`: shared PNG-only FGCS plotting style.
- `scripts/plot_main_comparison.py`: main baseline comparison.
- `scripts/plot_ablation.py`: readable full-width 2x2 ablation.
- `scripts/plot_adaptation.py`: workload-boundary adaptation.
- `scripts/plot_scalability.py`: latency + memory scalability.
- `scripts/plot_sensitivity.py`: optional sensitivity figure.
- `scripts/compute_main_statistics.py`: exact paired Wilcoxon + Holm correction + rank-biserial effect size, without SciPy.
- `data/main_comparison_aggregate.csv`: frozen aggregate placement metrics.
- `data/adaptation.csv`: frozen adaptation summary.
- `data/scalability.csv`: frozen scalability summary.
- `data/sensitivity.csv`: frozen sensitivity summary.
- `data/ablation_2x2_summary.json`: uploaded frozen ablation JSON when available.
- `figures_latex.tex`: LaTeX insertion snippets.
- `requirements_figures.txt`: NumPy/Matplotlib requirements.

## Figure rules

All new experimental figures should be:
- PNG only
- 600 dpi
- full-width 190 mm by default
- no internal plot title
- readable with at least 8 pt final font
- grayscale-readable
- free of numeric labels placed over lines or error bars

The previous single-column ablation attempts were rejected as unreadable and must not be reused.

## Generate the figures

Run from the package root:

```bash
python scripts/plot_main_comparison.py \
  --input data/main_comparison_aggregate.csv \
  --output figures/main_comparison.png

python scripts/plot_ablation.py \
  --input data/ablation_2x2_summary.json \
  --output figures/ablation_2x2.png

python scripts/plot_adaptation.py \
  --input data/adaptation.csv \
  --output figures/adaptation.png

python scripts/plot_scalability.py \
  --input data/scalability.csv \
  --output figures/scalability.png

python scripts/plot_sensitivity.py \
  --input data/sensitivity.csv \
  --output figures/sensitivity.png
```

## Main inferential statistics

The aggregate CSV is not sufficient for significance testing.

Build `data/main_comparison_per_trace.csv` with:

```text
scenario,seed,method,node_local,cross_rack,node_cv
```

It must contain the same 20 held-out traces for each method:
W1-W4 x seeds 23,37,53,71,89.

Methods:
- HDFS Default
- ANODE
- DARB
- CBCARP
- RAP

Then run:

```bash
python scripts/compute_main_statistics.py \
  --input data/main_comparison_per_trace.csv \
  --output results/main_statistics.csv
```

Do not use individual windows as independent statistical samples.

## Critical DARB rule

Use only the final DARB 2% results under:

```text
/home/hduser/DARB/results/test/...
```

Never use the archived 10% no-op results under:

```text
/home/hduser/DARB/results/archive/darb_threshold_10pct/...
```

## Experimental freeze

Do not:
- retrain RAP
- reopen/rerun final test
- rerun scalability
- retune parameters after final test
- reinterpret sensitivity as tuning
