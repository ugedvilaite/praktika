import os
import pandas as pd

from astroflow import create_session, enrich_coordinates_csv


def test_enrich_smoke(tmp_path):
    # Skip test if no token is set, to avoid failures in CI or for users without access
    if not os.getenv("GAIA_AIP_TOKEN"):
        return

    inp = tmp_path / "input_coords.csv"
    out = tmp_path / "enriched.csv"

    pd.DataFrame(
        {
            "ra": [280.97177, 10.6847083],
            "dec": [43.87557, 41.26875],
        }
    ).to_csv(inp, index=False)

    session = create_session()

    enrich_coordinates_csv(
        session=session,
        input_csv=inp,
        output_csv=out,
        ra_col="ra",
        dec_col="dec",
        radius_arcsec=20.0,  # bigger radius to ensure matches for test coords
        preset="basic",      # preset argument
    )

    df = pd.read_csv(out)

    # Check that Gaia columns are present and at least one match was found
    assert "source_id" in df.columns
    assert "matched_ra" in df.columns
    assert "separation_arcsec" in df.columns

    # Assert that at least one row has a non-null source_id, indicating a successful match
    assert df["source_id"].notna().any()