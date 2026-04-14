# AstroFlow

AstroFlow is a Python package for retrieving and processing **Gaia DR3 astrophysical data** through the **AIP TAP service**.

It provides tools for:

- matching sky coordinates to Gaia `source_id`
- downloading Gaia DR3 source parameters
- enriching coordinate catalogs with Gaia data
- downloading Gaia DR3 XP sampled mean spectra

---

## Features

- Flexible coordinate parsing:
  - decimal degrees
  - sexagesimal coordinates
- Nearest-source matching in `gaiadr3.gaia_source`
- Catalog enrichment from CSV input
- Gaia column presets for common use cases
- Download of Gaia DR3 XP sampled spectra
- Conversion of XP spectra from wide to long format
- Command-line scripts for matching, enrichment, and XP download

---

## Installation

Clone the repository and install in editable mode:

```bash
pip install -e .
```

For development:

```bash
pip install -e .[dev]
```

Python version required: **3.9+**

---

## Authentication

AstroFlow uses the **AIP Gaia TAP** service and requires an authentication token.

Set the token as an environment variable.

### Windows (PowerShell)

```powershell
$env:GAIA_AIP_TOKEN="Token YOUR_TOKEN_HERE"
```

### Linux / macOS

```bash
export GAIA_AIP_TOKEN="Token YOUR_TOKEN_HERE"
```

You can also pass a token directly to `create_session()`.

---

## Package Overview

AstroFlow is built around three main workflows:

1. **Coordinate matching**  
   Match RA/DEC coordinates to the nearest Gaia DR3 source.

2. **Catalog enrichment**  
   Start with a CSV containing coordinates, match them to Gaia sources, and download selected Gaia parameters.

3. **XP spectra download**  
   Download Gaia DR3 sampled mean XP spectra for known `source_id` values.

---

## Basic Usage

### Create an authenticated session

```python
from astroflow import create_session

session = create_session()
```

---

## 1. Coordinate Matching

Use `nearest_source_from()` to find the closest Gaia DR3 source to given coordinates.

```python
from astroflow import create_session
from astroflow.gaia_tap import nearest_source_from

session = create_session()

result = nearest_source_from(
    session=session,
    coords="280.97177 43.87557",
    radius_arcsec=2.0,
)

print(result)
```

Example output:

```python
{
    "source_id": 4049114675190544896,
    "separation_arcsec": 0.12,
    "matched_ra": 280.97177,
    "matched_dec": 43.87557
}
```

### Accepted coordinate formats

AstroFlow accepts coordinates in several forms:

```python
nearest_source_from(session, "280.97177 43.87557")
nearest_source_from(session, "18 43 53.22 +43 52 32.05")
nearest_source_from(session, (280.97177, 43.87557))
nearest_source_from(session, 280.97177, dec=43.87557)
```

---

## 2. Catalog Enrichment

AstroFlow can enrich a CSV file containing sky coordinates with Gaia DR3 information.

The enrichment workflow is:

```text
input coordinates -> nearest Gaia source_id -> Gaia parameter download -> enriched CSV
```

### Example

```python
from astroflow import create_session, enrich_coordinates_csv

session = create_session()

enrich_coordinates_csv(
    session=session,
    input_csv="input_coords.csv",
    output_csv="enriched_coords.csv",
    ra_col="ra",
    dec_col="dec",
    radius_arcsec=2.0,
)
```

### Enrich a pandas DataFrame directly

```python
import pandas as pd
from astroflow import create_session, enrich_df

df = pd.DataFrame({
    "ra": [280.97177],
    "dec": [43.87557],
})

session = create_session()

df_enriched = enrich_df(
    session=session,
    df=df,
    ra_col="ra",
    dec_col="dec",
    radius_arcsec=2.0,
)

print(df_enriched.head())
```

---

## Gaia Column Presets

AstroFlow includes several built-in presets for commonly used Gaia columns.

### Available presets

- `basic`
- `photometry`
- `astrometry`
- `full_default`

### Example

```python
from astroflow import create_session, enrich_coordinates_csv

session = create_session()

enrich_coordinates_csv(
    session=session,
    input_csv="input_coords.csv",
    output_csv="enriched_coords.csv",
    preset="basic",
)
```

### Default Gaia columns

The default enrichment columns are:

- `source_id`
- `ra`
- `dec`
- `phot_g_mean_mag`
- `phot_bp_mean_mag`
- `phot_rp_mean_mag`
- `bp_rp`
- `parallax`
- `pmra`
- `pmdec`
- `ruwe`

### Custom column selection

You can also specify explicit Gaia columns:

```python
from astroflow import create_session, enrich_coordinates_csv

session = create_session()

enrich_coordinates_csv(
    session=session,
    input_csv="input_coords.csv",
    output_csv="enriched_coords.csv",
    gaia_columns=[
        "source_id",
        "ra",
        "dec",
        "phot_g_mean_mag",
        "bp_rp",
        "parallax",
    ],
)
```

---

## 3. Download Gaia DR3 XP Sampled Mean Spectra

AstroFlow supports downloading data from:

- `gaiadr3.xp_sampled_mean_spectrum`

This table contains sampled mean XP spectra, including:

- `source_id`
- `ra`
- `dec`
- `flux`
- `flux_error`

### Example

```python
from astroflow import download_xp_sampled_mean_spectrum

source_ids = [4049114675190544896]

df_xp = download_xp_sampled_mean_spectrum(
    source_ids=source_ids,
    chunk_size=500,
)

print(df_xp.head())
```

---

## 4. Convert XP Spectra to Long Format

XP spectra are returned in wide format, with array columns such as `flux` and `flux_error`.

You can convert them into long format:

```python
from astroflow import xp_sampled_to_long

df_long = xp_sampled_to_long(df_xp, keep_ra_dec=True)
print(df_long.head())
```

Typical long-format columns:

- `source_id`
- `idx`
- `wavelength`
- `flux`
- `flux_error`
- optionally `ra`, `dec`

If no wavelength grid is provided, the `wavelength` column will contain `NaN`.

---

## Command-Line Interface

AstroFlow exposes several CLI entry points.

### 1. Match coordinates manually

Script name:

```bash
astroflow-match
```

Example:

```bash
astroflow-match "280.97177 43.87557"
```

Or:

```bash
astroflow-match 280.97177 43.87557
```

### 2. Enrich a CSV catalog

Script name:

```bash
astroflow-enrich
```

Example:

```bash
astroflow-enrich input_coords.csv -o enriched_coords.csv
```

With custom columns:

```bash
astroflow-enrich input_coords.csv -o enriched_coords.csv --gaia-columns source_id ra dec phot_g_mean_mag bp_rp
```

With preset:

```bash
astroflow-enrich input_coords.csv -o enriched_coords.csv --preset photometry
```

### 3. Download XP spectra from a CSV of source IDs

Script name:

```bash
astroflow-xp
```

Example:

```bash
astroflow-xp input_ids.csv -o xp_output.parquet
```

Long format:

```bash
astroflow-xp input_ids.csv -o xp_long.parquet --mode long --keep-ra-dec
```

> Note: if `astroflow-xp` is not yet listed in `pyproject.toml`, add it under `[project.scripts]`:
>
> ```toml
> astroflow-xp = "astroflow.cli_xp:main"
> ```

---

## Expected Input Formats

### Coordinate CSV for enrichment

Your input CSV should contain coordinate columns such as:

```csv
ra,dec
280.97177,43.87557
281.00210,43.90124
```

Sexagesimal coordinates are also supported if stored in separate RA and DEC columns.

### Source ID CSV for XP download

```csv
source_id
4049114675190544896
5853498713190525696
```

---

## Project Structure

## Project Structure

```text
astroflow_project/
│
├── astroflow/
│   ├── __init__.py
│   ├── cli_download.py
│   ├── cli_enrich.py
│   ├── cli_tap.py
│   ├── cli_xp.py
│   ├── enrich.py
│   ├── gaia_download.py
│   ├── gaia_tap.py
│   └── xp.py
│
├── astroflow.egg-info/
│
├── examples/
│   ├── input_coords.csv
│   ├── mini_cat_coord.csv
│   └── out_gaia.csv
│
├── out/
│   ├── enriched.csv
│   ├── xp_long_preview.csv
│   ├── xp_long.parquet
│   ├── xp_preview.csv
│   └── xp.parquet
│
├── out_xp_downloads/
│
├── tests/
│   ├── test_enrich.py
│   ├── test_gaia_download.py
│   ├── test_gaia_tap.py
│   └── test_xp.py
│
├── .gitignore
├── pyproject.toml
└── README.md

---

## Main Modules

### `gaia_tap.py`
Functions for:

- creating authenticated TAP sessions
- parsing coordinates
- finding the nearest Gaia DR3 source

### `gaia_download.py`
Functions for:

- direct TAP downloads by `source_id`
- async TAP job submission
- Simple Join Service downloads
- chunked downloads for large source lists

### `enrich.py`
Functions for:

- matching CSV/DataFrame coordinates to Gaia sources
- downloading Gaia parameters
- writing enriched output files

### `xp.py`
Functions for:

- downloading Gaia DR3 XP sampled mean spectra
- converting XP spectra to long format

---

## Dependencies

Main dependencies:

- `numpy`
- `pandas`
- `requests`
- `astropy`
- `pyvo`

Optional development dependency:

- `pytest`

---

## Testing

Run tests with:

```bash
pytest
```

Integration tests can be marked separately if needed.

---

## Notes

- Matching is performed sequentially row by row for compatibility with the AIP TAP workflow.
- XP sampled spectra contain array columns, so **Parquet** is recommended for storage.
- CSV output is supported for XP data only in **long** mode.
- If no valid matches are found during enrichment, the Gaia columns will remain empty.

---

## Purpose

AstroFlow provides a reusable and transparent interface for Gaia DR3 data access in Python.

It is intended for small-to-medium astrophysical data workflows, especially those involving:

- coordinate-based source matching
- Gaia DR3 catalog enrichment
- XP spectrum retrieval
- preparation of data for downstream stellar classification tasks