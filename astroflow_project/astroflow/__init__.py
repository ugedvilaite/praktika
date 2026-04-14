from __future__ import annotations

# Core session helper
from .gaia_tap import create_session

# Enrichment
from .enrich import (
    GAIA_PRESETS,
    DEFAULT_GAIA_COLS,
    enrich_coordinates_csv,
    enrich_df,
)

# XP spectra
from .xp import (
    download_xp_sampled_mean_spectrum,
    xp_sampled_to_long,
)

from .xp_continuous import download_xp_continuous_mean_spectrum

__all__ = [
    "create_session",
    # enrich
    "GAIA_PRESETS",
    "DEFAULT_GAIA_COLS",
    "enrich_df",
    "enrich_coordinates_csv",
    # xp
    "download_xp_sampled_mean_spectrum",
    "xp_sampled_to_long",
    "download_xp_continuous_mean_spectrum",
]