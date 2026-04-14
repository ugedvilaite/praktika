"""
Gaia DR3 XP spectra helpers (AstroFlow).

Implements download of sampled mean XP spectra:
- gaiadr3.xp_sampled_mean_spectrum

Notes
-----
- xp_sampled_mean_spectrum contains array columns:
  flux float[] and flux_error float[]
- The wavelength grid is shared for all spectra and is documented as being
  stored in the `wavelength` field of table `XpMerge` (Gaia DR3 docs at AIP).
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Sequence, Union

import numpy as np
import pandas as pd

from .gaia_download import download_join_chunked

PathLike = Union[str, Path]

XP_SAMPLED_TABLE = "gaiadr3.xp_sampled_mean_spectrum"


def download_xp_sampled_mean_spectrum(
    source_ids: Sequence[int],
    *,
    token: Optional[str] = None,
    chunk_size: int = 500,
    out_dir: PathLike = "out_xp_downloads",
    save_chunks_parquet: bool = False,
) -> pd.DataFrame:
    """Download Gaia DR3 sampled mean XP spectra for given source_ids.

    Returns a DataFrame with at least:
      - source_id
      - ra, dec
      - flux (array)
      - flux_error (array)

    Uses TAP async + SJS join under the hood via download_join_chunked().
    """
    ids = [int(x) for x in source_ids]
    if not ids:
        return pd.DataFrame()

    df = download_join_chunked(
        source_ids=ids,
        join_table=XP_SAMPLED_TABLE,
        token=token,
        chunk_size=chunk_size,
        out_dir=out_dir,
        save_chunks_parquet=save_chunks_parquet,
    )

    # Minimal sanity: expected cols (not hard-fail, bet good error if missing)
    expected = {"source_id", "flux", "flux_error"}
    missing = expected.difference(df.columns)
    if missing:
        raise RuntimeError(
            f"XP download finished but missing columns: {sorted(missing)}. "
            f"Got columns: {list(df.columns)}"
        )

    return df

def xp_sampled_to_long(
    df_xp: pd.DataFrame,
    *,
    wavelength: Optional[Sequence[float]] = None,
    keep_ra_dec: bool = False,
) -> pd.DataFrame:
    """Convert sampled XP spectra DataFrame into long format.

    Output columns:
      - source_id
      - idx (0..N-1)
      - wavelength (if provided; otherwise NaN)
      - flux
      - flux_error
      - (optional) ra, dec
    """
    if df_xp.empty:
        cols = ["source_id", "idx", "wavelength", "flux", "flux_error"]
        if keep_ra_dec:
            cols += ["ra", "dec"]
        return pd.DataFrame(columns=cols)

    rows = []
    wl = None if wavelength is None else np.asarray(list(wavelength), dtype=float)

    for _, r in df_xp.iterrows():
        sid = int(r["source_id"])

        flux = np.asarray(r["flux"], dtype=float)
        ferr = np.asarray(r["flux_error"], dtype=float)

        if flux.shape != ferr.shape:
            raise ValueError(f"source_id={sid}: flux and flux_error lengths differ.")

        n = flux.size
        if wl is not None and wl.size != n:
            raise ValueError(
                f"wavelength length ({wl.size}) != flux length ({n}) for source_id={sid}."
            )

        base = {
            "source_id": np.full(n, sid, dtype=np.int64),
            "idx": np.arange(n, dtype=np.int32),
            "flux": flux,
            "flux_error": ferr,
            "wavelength": wl if wl is not None else np.full(n, np.nan),
        }

        if keep_ra_dec and "ra" in df_xp.columns and "dec" in df_xp.columns:
            base["ra"] = np.full(n, float(r["ra"]))
            base["dec"] = np.full(n, float(r["dec"]))

        rows.append(pd.DataFrame(base))

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()