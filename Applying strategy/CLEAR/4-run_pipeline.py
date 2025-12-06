#!/usr/bin/env python3
import argparse
import csv
import math
import os
import random
import statistics
import sys
from typing import Dict, List, Tuple, Optional

EXPECTED_HEADER = ["dataset", "freq", "conc", "cpuRatio", "age", "locality"]
NUMERIC_COLS = ["freq", "conc", "cpuRatio", "age", "locality"]


class ColumnStats:
    """
    Global statistics for one numeric column, with
    - min
    - max
    - median and MAD approximated via reservoir sampling
    """

    def __init__(self, name: str, reservoir_size: int = 10000) -> None:
        self.name = name
        self.count = 0              # number of valid values
        self.min_val = math.inf
        self.max_val = -math.inf
        self.sum_val = 0.0
        self.sumsq_val = 0.0
        self.reservoir_size = reservoir_size
        self.sample: List[float] = []
        self.median = float("nan")
        self.mad = float("nan")

    def update(self, x: float, rng: random.Random) -> None:
        self.count += 1
        if x < self.min_val:
            self.min_val = x
        if x > self.max_val:
            self.max_val = x
        self.sum_val += x
        self.sumsq_val += x * x

        # Reservoir sampling to approximate median and MAD
        if self.count <= self.reservoir_size:
            self.sample.append(x)
        else:
            j = rng.randint(0, self.count - 1)
            if j < self.reservoir_size:
                self.sample[j] = x

    def finalize(self) -> None:
        if self.count == 0 or not self.sample:
            self.median = float("nan")
            self.mad = float("nan")
            return

        s = sorted(self.sample)
        self.median = statistics.median(s)
        deviations = [abs(v - self.median) for v in s]
        self.mad = statistics.median(deviations)

    def as_dict(self) -> Dict[str, float]:
        return {
            "min": self.min_val,
            "max": self.max_val,
            "median": self.median,
            "mad": self.mad,
            "count": self.count,
        }


def check_header(fieldnames: List[str], path: str) -> None:
    if fieldnames is None:
        raise ValueError(f"{path}: empty file or missing CSV header")
    if fieldnames != EXPECTED_HEADER:
        raise ValueError(
            f"{path}: unexpected header.\n"
            f"Found: {fieldnames}\n"
            f"Expected: {EXPECTED_HEADER}"
        )


def is_missing(val: str) -> bool:
    if val is None:
        return True
    s = val.strip()
    if s == "":
        return True
    s_lower = s.lower()
    return s_lower in ("na", "nan", "null")


def scan_stats(
    path: str,
    limit: Optional[int],
    chunk_size: int,
    reservoir_size: int,
    seed: int,
) -> Tuple[Dict[str, ColumnStats], int]:
    """
    First pass: streaming read, compute global statistics.
    Missing values are ignored for the corresponding column.
    Returns (stats_per_column, number_of_rows_with_at_least_one_valid_value)
    """
    rng = random.Random(seed)
    stats: Dict[str, ColumnStats] = {
        col: ColumnStats(col, reservoir_size=reservoir_size) for col in NUMERIC_COLS
    }

    total_rows = 0
    used_rows = 0

    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        check_header(reader.fieldnames, path)

        for row in reader:
            total_rows += 1
            if limit is not None and used_rows >= limit:
                break

            row_had_value = False
            for col in NUMERIC_COLS:
                val_str = row[col]
                if is_missing(val_str):
                    # missing value for this column, skip from stats
                    continue
                try:
                    x = float(val_str)
                except ValueError as e:
                    raise ValueError(
                        f"Invalid numeric value at line {total_rows} "
                        f"in column {col}: {val_str!r}"
                    ) from e
                stats[col].update(x, rng)
                row_had_value = True

            if row_had_value:
                used_rows += 1

            if chunk_size > 0 and (total_rows % chunk_size == 0):
                print(f"[scan_stats] {total_rows} rows read...", file=sys.stderr)

    for col in NUMERIC_COLS:
        stats[col].finalize()

    print(
        f"[scan_stats] Done for {path}. "
        f"Total rows in file: {total_rows}. "
        f"Rows with at least one valid value: {used_rows}.",
        file=sys.stderr,
    )

    return stats, used_rows


def write_stats_csv(
    stats: Dict[str, ColumnStats],
    out_path: str,
) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["feature", "min", "max", "median", "MAD", "count"])
        for col in NUMERIC_COLS:
            s = stats[col]
            writer.writerow(
                [
                    col,
                    s.min_val,
                    s.max_val,
                    s.median,
                    s.mad,
                    s.count,
                ]
            )


def normalize_file(
    in_path: str,
    out_path: str,
    stats: Dict[str, ColumnStats],
    limit: Optional[int],
    chunk_size: int,
) -> int:
    """
    Second pass: min max normalization over [0,1] in streaming mode.
    For a missing value, use the global median of the column.
    Writes a new CSV with the same columns as the input,
    but with numeric features normalized.
    Returns the number of written rows.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    mins = {col: stats[col].min_val for col in NUMERIC_COLS}
    maxs = {col: stats[col].max_val for col in NUMERIC_COLS}

    def norm_value(col: str, x: float) -> float:
        mn = mins[col]
        mx = maxs[col]
        if not math.isfinite(mn) or not math.isfinite(mx):
            return float("nan")
        if mx == mn:
            return 0.0
        return (x - mn) / (mx - mn)

    total_rows = 0
    written_rows = 0

    with open(in_path, "r", newline="") as fin, open(out_path, "w", newline="") as fout:
        reader = csv.DictReader(fin)
        check_header(reader.fieldnames, in_path)

        writer = csv.DictWriter(fout, fieldnames=EXPECTED_HEADER)
        writer.writeheader()

        for row in reader:
            total_rows += 1
            if limit is not None and written_rows >= limit:
                break

            out_row = {"dataset": row["dataset"]}

            for col in NUMERIC_COLS:
                val_str = row[col]
                if is_missing(val_str):
                    # impute with global median
                    x = stats[col].median
                    if not math.isfinite(x):
                        # fallback to min, then to 0.0
                        if math.isfinite(stats[col].min_val):
                            x = stats[col].min_val
                        else:
                            x = 0.0
                else:
                    try:
                        x = float(val_str)
                    except ValueError as e:
                        raise ValueError(
                            f"Invalid numeric value at line {total_rows} "
                            f"in column {col}: {val_str!r}"
                        ) from e

                nx = norm_value(col, x)
                out_row[col] = f"{nx:.6f}"

            writer.writerow(out_row)
            written_rows += 1

            if chunk_size > 0 and (total_rows % chunk_size == 0):
                print(
                    f"[normalize] {total_rows} rows read, "
                    f"{written_rows} rows written",
                    file=sys.stderr,
                )

    print(
        f"[normalize] Done for {in_path}. "
        f"Rows written to {out_path}: {written_rows}.",
        file=sys.stderr,
    )
    return written_rows


def infer_output_names(features_path: str, out_dir: str) -> Tuple[str, str]:
    """
    Convention:
    - features_day1.csv -> day1_norm.csv and day1_globstats.csv
    - features_day2.csv -> day2_norm.csv and day2_globstats.csv
    - features_day3.csv -> day3_norm.csv and day3_globstats.csv
    Otherwise:
    - basename.csv -> basename_norm.csv and basename_globstats.csv
    """
    base = os.path.basename(features_path)
    name, _ = os.path.splitext(base)
    day_tag: Optional[str] = None

    if name.startswith("features_") and len(name) > len("features_"):
        day_tag = name[len("features_") :]

    if day_tag:
        norm_name = f"{day_tag}_norm.csv"
        stats_name = f"{day_tag}_globstats.csv"
    else:
        norm_name = f"{name}_norm.csv"
        stats_name = f"{name}_globstats.csv"

    norm_path = os.path.join(out_dir, norm_name)
    stats_path = os.path.join(out_dir, stats_name)
    return norm_path, stats_path


def cmd_validate(args: argparse.Namespace) -> None:
    stats, used_rows = scan_stats(
        path=args.features,
        limit=args.limit,
        chunk_size=args.chunk_size,
        reservoir_size=args.reservoir_size,
        seed=args.seed,
    )

    print(f"File: {args.features}")
    print(f"Rows with at least one valid value: {used_rows}")
    print("Summary per feature (valid values only):")
    for col in NUMERIC_COLS:
        s = stats[col]
        print(
            f"  - {col}: "
            f"min={s.min_val:.6f}, "
            f"max={s.max_val:.6f}, "
            f"median={s.median:.6f}, "
            f"MAD={s.mad:.6f}, "
            f"count={s.count}"
        )

    if args.out_stats:
        write_stats_csv(stats, args.out_stats)
        print(f"Global stats written to: {args.out_stats}")


def cmd_normalize(args: argparse.Namespace) -> None:
    norm_path, stats_path = infer_output_names(args.features, args.out)
    print(f"[normalize] Normalized output: {norm_path}")
    print(f"[normalize] Stats output: {stats_path}")

    stats, used_rows = scan_stats(
        path=args.features,
        limit=args.limit,
        chunk_size=args.chunk_size,
        reservoir_size=args.reservoir_size,
        seed=args.seed,
    )

    write_stats_csv(stats, stats_path)
    print(f"[normalize] Global stats written to: {stats_path}")

    normalize_file(
        in_path=args.features,
        out_path=norm_path,
        stats=stats,
        limit=args.limit,
        chunk_size=args.chunk_size,
    )

    print("[normalize] Done.")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Hadoop replication pipeline: validation and min max normalization."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=123,
        help="Random seed for reservoir sampling (default: 123).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # validate
    p_val = subparsers.add_parser(
        "validate",
        help="Validate a features_* file and print global statistics.",
    )
    p_val.add_argument(
        "--features",
        required=True,
        help="Input CSV file (features_dayX.csv).",
    )
    p_val.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of rows read for quick tests.",
    )
    p_val.add_argument(
        "--chunk-size",
        type=int,
        default=200000,
        help="Logical block size for progress messages.",
    )
    p_val.add_argument(
        "--reservoir-size",
        type=int,
        default=10000,
        help="Reservoir size to approximate median and MAD.",
    )
    p_val.add_argument(
        "--out-stats",
        default=None,
        help="Optional: CSV path where global stats will be written.",
    )
    p_val.set_defaults(func=cmd_validate)

    # normalize
    p_norm = subparsers.add_parser(
        "normalize",
        help="Normalize a features_* file with min max and generate *_norm.csv and *_globstats.csv.",
    )
    p_norm.add_argument(
        "--features",
        required=True,
        help="Input CSV file (features_dayX.csv).",
    )
    p_norm.add_argument(
        "--out",
        required=True,
        help="Output directory for generated CSV files.",
    )
    p_norm.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of processed rows (useful for tests).",
    )
    p_norm.add_argument(
        "--chunk-size",
        type=int,
        default=200000,
        help="Logical block size for progress messages.",
    )
    p_norm.add_argument(
        "--reservoir-size",
        type=int,
        default=10000,
        help="Reservoir size to approximate median and MAD.",
    )
    p_norm.set_defaults(func=cmd_normalize)

    return parser


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
