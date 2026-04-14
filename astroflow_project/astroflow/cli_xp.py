from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List

import pandas as pd

from .xp import download_xp_sampled_mean_spectrum, xp_sampled_to_long
from .xp_continuous import download_xp_continuous_mean_spectrum


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="astroflow-xp",
        description=(
            "Download Gaia DR3 XP sampled and/or continuous mean spectra "
            "for source_ids from a CSV."
        ),
    )
    parser.add_argument(
        "input_csv",
        help="Input CSV path (must contain source_id column).",
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help=(
            "Output path prefix or file path. "
            "For --kind both, this will be used as prefix."
        ),
    )
    parser.add_argument(
        "--source-id-col",
        default="source_id",
        help="Column name containing Gaia source_id.",
    )
    parser.add_argument(
        "--kind",
        choices=["sampled", "continuous", "both"],
        default="sampled",
        help="What to download.",
    )
    parser.add_argument(
        "--mode",
        choices=["wide", "long"],
        default="wide",
        help="Sampled output format: wide or long. Ignored for continuous.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Chunk size for downloads.",
    )
    parser.add_argument(
        "--keep-ra-dec",
        action="store_true",
        help="Include ra/dec in sampled long mode if available.",
    )
    return parser


def _read_source_ids(input_csv: Path, source_id_col: str) -> List[int]:
    df = pd.read_csv(input_csv)

    if source_id_col not in df.columns:
        raise SystemExit(
            f"Missing column '{source_id_col}' in {input_csv}."
        )

    source_ids: List[int] = (
        pd.to_numeric(df[source_id_col], errors="coerce")
        .dropna()
        .astype("int64")
        .drop_duplicates()
        .tolist()
    )

    if not source_ids:
        raise SystemExit("No valid source_id values found in input.")

    return source_ids


def _write_df(
    df_out: pd.DataFrame,
    out_path: Path,
    *,
    allow_wide_csv: bool = False,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.suffix.lower() == ".parquet":
        df_out.to_parquet(out_path, index=False)
    else:
        if not allow_wide_csv:
            raise SystemExit("This output is better saved as .parquet.")
        df_out.to_csv(out_path, index=False)

    print(str(out_path))


def main() -> None:
    args = _build_parser().parse_args()

    if not os.getenv("GAIA_AIP_TOKEN"):
        print("Missing GAIA_AIP_TOKEN environment variable.", file=sys.stderr)
        raise SystemExit(1)

    in_path = Path(args.input_csv)
    out_path = Path(args.output)

    source_ids = _read_source_ids(in_path, args.source_id_col)

    if args.kind == "sampled":
        df_sampled = download_xp_sampled_mean_spectrum(
            source_ids=source_ids,
            chunk_size=args.chunk_size,
        )

        if args.mode == "long":
            df_out = xp_sampled_to_long(
                df_sampled,
                keep_ra_dec=args.keep_ra_dec,
            )
            _write_df(df_out, out_path, allow_wide_csv=True)
        else:
            df_out = df_sampled
            if out_path.suffix.lower() != ".parquet":
                raise SystemExit(
                    "Wide sampled output should be saved as .parquet."
                )
            _write_df(df_out, out_path)

    elif args.kind == "continuous":
        df_cont = download_xp_continuous_mean_spectrum(
            source_ids=source_ids,
            chunk_size=args.chunk_size,
        )

        if out_path.suffix.lower() not in [".parquet", ".csv"]:
            raise SystemExit(
                "Use .parquet or .csv for continuous output."
            )

        _write_df(df_cont, out_path, allow_wide_csv=True)

    elif args.kind == "both":
        base = out_path
        if base.suffix:
            base = base.with_suffix("")

        sampled_out = base.parent / f"{base.name}_sampled.parquet"
        continuous_out = base.parent / f"{base.name}_continuous.parquet"

        df_sampled = download_xp_sampled_mean_spectrum(
            source_ids=source_ids,
            chunk_size=args.chunk_size,
        )

        if args.mode == "long":
            df_sampled = xp_sampled_to_long(
                df_sampled,
                keep_ra_dec=args.keep_ra_dec,
            )
            sampled_out = base.parent / f"{base.name}_sampled.csv"

        df_cont = download_xp_continuous_mean_spectrum(
            source_ids=source_ids,
            chunk_size=args.chunk_size,
        )

        if args.mode == "long":
            _write_df(df_sampled, sampled_out, allow_wide_csv=True)
        else:
            _write_df(df_sampled, sampled_out)

        _write_df(df_cont, continuous_out, allow_wide_csv=True)


if __name__ == "__main__":
    main()