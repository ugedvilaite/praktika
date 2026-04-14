from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence, Union

import pandas as pd

from astroflow.gaia_download import download_join_chunked

PathLike = Union[str, Path]

XP_CONTINUOUS_TABLE = "gaiadr3.xp_continuous_mean_spectrum"


def download_xp_continuous_mean_spectrum(
    source_ids: Sequence[int],
    *,
    token: Optional[str] = None,
    chunk_size: int = 500,
    out_dir: PathLike = "out_xp_continuous",
    save_chunks_parquet: bool = False,
) -> pd.DataFrame:
    """Download Gaia DR3 XP continuous mean spectra for given source_ids."""
    ids = [int(x) for x in source_ids]
    if not ids:
        return pd.DataFrame()

    df = download_join_chunked(
        source_ids=ids,
        join_table=XP_CONTINUOUS_TABLE,
        token=token,
        chunk_size=chunk_size,
        out_dir=out_dir,
        save_chunks_parquet=save_chunks_parquet,
    )

    if df.empty:
        return df

    if "source_id" not in df.columns:
        raise RuntimeError(
            f"Continuous XP download finished but no 'source_id' column found. "
            f"Got columns: {list(df.columns)}"
        )

    return df