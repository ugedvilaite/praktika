from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional

from .enrich import GAIA_PRESETS, enrich_coordinates_csv
from .gaia_tap import create_session


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="astroflow-enrich",
        description="Enrich a CSV (RA/DEC) with Gaia DR3 parameters.",
    )

    parser.add_argument("input_csv", help="Input CSV path (must contain RA/DEC).")

    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Output CSV path.",
    )

    parser.add_argument("--ra-col", default="ra", help="RA column name (deg or sexagesimal).")
    parser.add_argument("--dec-col", default="dec", help="DEC column name (deg or sexagesimal).")

    parser.add_argument(
        "--radius",
        type=float,
        default=2.0,
        help="Matching radius in arcseconds.",
    )

    parser.add_argument(
        "--preset",
        default=None,
        choices=sorted(GAIA_PRESETS.keys()),
        help="Gaia column preset to download.",
    )

    parser.add_argument(
        "--gaia-columns",
        nargs="+",
        default=None,
        help=(
            "Explicit Gaia columns to download from gaiadr3.gaia_source "
            "(overrides --preset). Example: --gaia-columns source_id ra dec bp_rp"
        ),
    )

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if not os.getenv("GAIA_AIP_TOKEN"):
        print("Missing GAIA_AIP_TOKEN environment variable.", file=sys.stderr)
        raise SystemExit(1)

    session = create_session()

    out = enrich_coordinates_csv(
        session=session,
        input_csv=Path(args.input_csv),
        output_csv=Path(args.output),
        ra_col=args.ra_col,
        dec_col=args.dec_col,
        radius_arcsec=args.radius,
        preset=args.preset,
        gaia_columns=args.gaia_columns,
    )

    print(str(out))


if __name__ == "__main__":
    main()