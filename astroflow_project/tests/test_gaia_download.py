import os
import pytest

from astroflow.gaia_download import download_join_by_ids, tap_sync, create_session


@pytest.mark.integration
def test_download_join_by_ids_gaia_source():
    token = os.getenv("GAIA_AIP_TOKEN")
    if not token:
        pytest.skip("GAIA_AIP_TOKEN not set; skipping integration test.")

    session = create_session(token)

    # Get a guaranteed existing ID from the same TAP service
    t = tap_sync(
        session,
        "SELECT TOP 1 source_id FROM gaiadr3.gaia_source",
        timeout_s=180,
    )
    source_id = int(t["source_id"][0])

    df = download_join_by_ids(
        source_ids=[source_id],
        join_table="gaiadr3.gaia_source",
        token=token,
        out_dir="out_test_download",
    )

    assert len(df) >= 1
    assert "source_id" in df.columns
    assert int(df["source_id"].iloc[0]) == source_id