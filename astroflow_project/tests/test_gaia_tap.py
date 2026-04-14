import os

import pytest

from astroflow.gaia_tap import create_session, nearest_source_from, parse_coords


def test_parse_deg_string():
    ra, dec = parse_coords("280.97177 43.87557")
    assert abs(ra - 280.97177) < 1e-9
    assert abs(dec - 43.87557) < 1e-9


def test_parse_hms_dms_string():
    ra, dec = parse_coords("18 43 53.2248017160 +43 52 32.059767288")
    # sanity check: deg ribos
    assert 0.0 <= ra < 360.0
    assert -90.0 <= dec <= 90.0


@pytest.mark.integration
def test_integration_any_input():
    token = os.getenv("GAIA_AIP_TOKEN")
    if not token:
        pytest.skip("GAIA_AIP_TOKEN not set; skipping integration test.")

    session = create_session(token)

    res = nearest_source_from(
        session,
        "18 43 53.2248017160 +43 52 32.059767288",
        radius_arcsec=2.0,
        debug=False,
    )
    assert res is not None
    assert "source_id" in res
    assert isinstance(res["source_id"], int)