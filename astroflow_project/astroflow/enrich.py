"""
CSV enrichment utilities for AstroFlow.

Pipeline:
1) Read CSV with RA/DEC.
2) Match each coordinate to the nearest Gaia DR3 source_id (sequential).
3) Download selected Gaia columns by source_id.
4) Write an enriched CSV output.

Notes
-----
- Matching is done row-by-row via `nearest_source` for AIP TAP compatibility.
- RA/DEC inputs support either:
  * numeric degrees (float/int), or
  * sexagesimal strings in two columns:
      RA like "18 43 53.22" (hours) and DEC like "+43 52 32.05" (deg)
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import requests

from .gaia_download import download_by_ids
from .gaia_tap import nearest_source, parse_coords

PathLike = Union[str, Path]

# Default columns (kept for backwards compatibility)
DEFAULT_GAIA_COLS: List[str] = [
    "source_id",
    "ra",
    "dec",
    "phot_g_mean_mag",
    "phot_bp_mean_mag",
    "phot_rp_mean_mag",
    "bp_rp",
    "parallax",
    "pmra",
    "pmdec",
    "ruwe",
]

# Presets: named subsets of Gaia columns for convenience
GAIA_PRESETS: Dict[str, List[str]] = {
    "basic": [
        "source_id",
        "ra",
        "dec",
        "phot_g_mean_mag",
        "bp_rp",
        "parallax",
        "pmra",
        "pmdec",
        "ruwe",
    ],
    "photometry": [
        "source_id",
        "ra",
        "dec",
        "phot_g_mean_mag",
        "phot_bp_mean_mag",
        "phot_rp_mean_mag",
        "bp_rp",
    ],
    "astrometry": [
        "source_id",
        "ra",
        "dec",
        "parallax",
        "pmra",
        "pmdec",
        "ruwe",
    ],
    "full_default": DEFAULT_GAIA_COLS,
}


def _ensure_row_id(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    """Ensure a stable row id column exists for merging."""
    if id_col not in df.columns:
        df = df.copy()
        df[id_col] = range(1, len(df) + 1)
    return df


def _to_deg(ra_raw: object, dec_raw: object) -> Tuple[float, float]:
    """Parse RA/DEC into degrees.

    Accepts numeric degrees or sexagesimal strings (RA hourangle, DEC deg).
    """
    try:
        ra = float(ra_raw)  # type: ignore[arg-type]
        dec = float(dec_raw)  # type: ignore[arg-type]
        return ra, dec
    except (TypeError, ValueError):
        ra, dec = parse_coords(str(ra_raw), dec=str(dec_raw))
        return float(ra), float(dec)


def _resolve_gaia_columns(
    *,
    gaia_columns: Optional[Sequence[str]],
    preset: Optional[str],
) -> List[str]:
    """Resolve Gaia column list from explicit columns or preset.

    Priority:
    1) gaia_columns (if provided)
    2) preset (if provided)
    3) DEFAULT_GAIA_COLS
    """
    if gaia_columns is not None:
        cols = [str(c).strip() for c in gaia_columns if str(c).strip()]
        if not cols:
            raise ValueError("gaia_columns was provided but empty after cleaning.")
        return cols

    if preset is not None:
        preset = str(preset).strip()
        if preset not in GAIA_PRESETS:
            raise ValueError(
                f"Unknown preset '{preset}'. Available: {', '.join(sorted(GAIA_PRESETS.keys()))}"
            )
        return GAIA_PRESETS[preset]

    return list(DEFAULT_GAIA_COLS)


def _match_loop(
    session: requests.Session,
    df_coords: pd.DataFrame,
    *,
    id_col: str,
    ra_col: str,
    dec_col: str,
    radius_arcsec: float,
) -> pd.DataFrame:
    """Match coordinates to nearest Gaia source sequentially."""
    rows: List[dict] = []

    for _, r in df_coords.iterrows():
        rid = int(r[id_col])

        ra_raw = r[ra_col]
        dec_raw = r[dec_col]

        ra, dec = _to_deg(ra_raw, dec_raw)

        match = nearest_source(
            session=session,
            ra_deg=ra,
            dec_deg=dec,
            radius_arcsec=radius_arcsec,
            debug=False,
        )

        if match is None:
            rows.append(
                {
                    id_col: rid,
                    "source_id": pd.NA,
                    "matched_ra": pd.NA,
                    "matched_dec": pd.NA,
                    "separation_arcsec": pd.NA,
                }
            )
            continue

        rows.append(
            {
                id_col: rid,
                "source_id": int(match["source_id"]),
                "matched_ra": float(match["matched_ra"]),
                "matched_dec": float(match["matched_dec"]),
                "separation_arcsec": float(match["separation_arcsec"]),
            }
        )

    return pd.DataFrame(rows)


def enrich_df(
    session: requests.Session,
    df: pd.DataFrame,
    *,
    ra_col: str = "ra",
    dec_col: str = "dec",
    id_col: str = "__rowid__",
    radius_arcsec: float = 2.0,
    preset: Optional[str] = None,
    gaia_columns: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """Enrich a DataFrame containing RA/DEC with Gaia DR3 fields."""
    df = _ensure_row_id(df, id_col=id_col)

    if ra_col not in df.columns or dec_col not in df.columns:
        raise KeyError(f"Input df must contain columns '{ra_col}' and '{dec_col}'.")

    coords = df[[id_col, ra_col, dec_col]].copy()

    matches = _match_loop(
        session=session,
        df_coords=coords,
        id_col=id_col,
        ra_col=ra_col,
        dec_col=dec_col,
        radius_arcsec=radius_arcsec,
    )

    # Safer numeric conversion
    source_ids = (
        pd.to_numeric(matches["source_id"], errors="coerce")
        .dropna()
        .astype("int64")
        .tolist()
    )

    columns = _resolve_gaia_columns(gaia_columns=gaia_columns, preset=preset)

    if source_ids:
        gaia_df = download_by_ids(
            source_ids,
            token=None,  # relies on GAIA_AIP_TOKEN env var
            table="gaiadr3.gaia_source",
            columns=columns,
        )
    else:
        gaia_df = pd.DataFrame(columns=columns)

    enriched = df.merge(matches, on=id_col, how="left").merge(
        gaia_df, on="source_id", how="left"
    )

    return enriched


def enrich_coordinates_csv(
    session: requests.Session,
    input_csv: PathLike,
    output_csv: PathLike,
    *,
    ra_col: str = "ra",
    dec_col: str = "dec",
    id_col: str = "__rowid__",
    radius_arcsec: float = 2.0,
    preset: Optional[str] = None,
    gaia_columns: Optional[Sequence[str]] = None,
) -> Path:
    """Read a CSV with coordinates and write an enriched CSV with Gaia DR3 fields."""
    in_path = Path(input_csv)
    out_path = Path(output_csv)

    df = pd.read_csv(in_path)

    enriched = enrich_df(
        session=session,
        df=df,
        ra_col=ra_col,
        dec_col=dec_col,
        id_col=id_col,
        radius_arcsec=radius_arcsec,
        preset=preset,
        gaia_columns=gaia_columns,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(out_path, index=False)
    return out_path