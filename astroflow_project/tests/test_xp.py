import os
import pandas as pd

from astroflow import create_session, enrich_df
from astroflow.xp import download_xp_sampled_mean_spectrum, xp_sampled_to_long


def test_xp_sampled_smoke(tmp_path):
    if not os.getenv("GAIA_AIP_TOKEN"):
        return

    # 1) doing source enrichment to get some real source_ids for testing XP download
    df_in = pd.DataFrame(
        {
            "ra": [280.97177, 10.6847083],
            "dec": [43.87557, 41.26875],
        }
    )

    session = create_session()
    df_enriched = enrich_df(session=session, df=df_in, radius_arcsec=20.0, preset="basic")

    source_ids = (
        pd.to_numeric(df_enriched["source_id"], errors="coerce")
        .dropna()
        .astype("int64")
        .tolist()
    )
    assert source_ids, "Expected at least one matched source_id."

    # 2) download XP sampled mean spectrum for the first source_id
    df_xp = download_xp_sampled_mean_spectrum(source_ids=source_ids[:1], chunk_size=200)
    assert not df_xp.empty
    assert "source_id" in df_xp.columns
    assert "flux" in df_xp.columns
    assert "flux_error" in df_xp.columns

    # 3) check conversion to long format
    df_long = xp_sampled_to_long(df_xp)
    assert not df_long.empty
    assert set(["source_id", "idx", "flux", "flux_error"]).issubset(df_long.columns)