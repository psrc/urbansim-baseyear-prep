# FLU Crosswalk Builder (`old_flu_crosswalk.py`)

Builds a crosswalk between two Future Land Use (FLU) vintages — matching zones
from the **current** FLU year to the **previous** FLU year so downstream
base-year models can translate old zone codes to new ones (and vice versa).

## What the script does

Given one CSV of new-vintage zones, one shapefile of old-vintage zones, and an
old description lookup, the script produces a Excel crosswalk by running:

1. **Load sources** — new FLU CSV, old FLU shapefile (+ its description
   Excel), and the new FLU GDB polygons used for the spatial step.
2. **Unique zone lists** — one row per `(jurisdiction, zone)` in each vintage.
3. **Name-based match** (per jurisdiction, three passes):
   - **exact** — identical zone strings (`confidence = 1.0`)
   - **near_match** — equal after lowercasing and stripping separators
     (`confidence = 0.8`)
   - **fuzzy** — best `SequenceMatcher` ratio above `FUZZY_MATCH_CUTOFF`
     (`confidence = ratio`)
   - Any new zone with no old match gets `match_type = 'new_zone'`.
4. **Spatial fill** — for any row still flagged `needs_review`, look at the
   polygon overlap between the new zone and every old zone and take the old
   zone with the greatest share of new-zone area. If the overlap is at or
   above `SPATIAL_OVERLAP_CUTOFF` the row is auto-accepted
   (`match_type = 'spatial'`, `needs_review = False`); otherwise it stays
   flagged for review but the spatial suggestion is recorded.
5. **Manual overrides** — re-reads the working Excel (from a previous run)
   and applies any user edits in the `manual_match` / `confirmed_new` columns.
6. **Write outputs** — working Excel (for iterative review) and a cleaned-up
   `Full_FLU_Master_Corres_File.xlsx` for downstream use.

## Inputs

| Setting | Default | Notes |
| --- | --- | --- |
| `NEW_FLU_CSV_PATH` | `Zoning_2026_d2_cleaned.csv` | New FLU zones table. Must have `Juris`, `juris_zn`, and `Definition` columns. |
| `NEW_FLU_CSV_ENCODING` | `latin-1` | |
| `NEW_FLU_GDB_PATH` / `NEW_FLU_GDB_LAYER` | `FLU_draft2.gdb` / `FLU2025_cleaned` | New FLU polygons used only for spatial matching. |
| `OLD_FLU_SHP_PATH` | `flu19_reviewed.shp` | Old FLU polygons. Must have `Jurisdicti`, `Juris_zn` columns. |
| `OLD_XWALK_PATH` / `OLD_XWALK_SHEET` | `Full_FLU_Master_Corres_File.xlsx` / `Full Master FLU Corres File` | Source of old-vintage zone descriptions (`FLUadj_Key` -> `FLUadj_Definition`). |

## Outputs (in `future_land_use/`)

- `flu_crosswalk_{OLD_YR}_to_{NEW_YR}_working.xlsx` — full crosswalk for
  iterative manual review. Edit the `manual_match` and `confirmed_new`
  columns in place and rerun the script to apply them.
- `flu_crosswalk_{OLD_YR}_to_{NEW_YR}_working_backup_{timestamp}.xlsx` —
  automatic backup written every time the script re-reads the working file.
- `old_zones_{OLD_YR}.xlsx` — the unique old-vintage zones, useful when
  filling in `manual_match` values.
- `Full_FLU_Master_Corres_File.xlsx` — final cleaned master crosswalk with
  standardized column names (`FLU_master_id`, `jurisdiction`,
  `juris_zn_{NEW_YR}`, `desc_{NEW_YR}`, `juris_zn_{OLD_YR}`,
  `desc_{OLD_YR}`).

## Output column glossary

| Column | Meaning |
| --- | --- |
| `match_type` | `exact`, `near_match`, `fuzzy`, `new_zone`, `spatial`, or `manual`. |
| `confidence` | 1.0 for exact/manual, 0.8 for near_match, fuzzy ratio for fuzzy, percent overlap for spatial, 0.0 for new_zone. |
| `needs_review` | `True` for rows the user should inspect (fuzzy, new_zone, or low-overlap spatial). |
| `manual_match` | User-provided override. Must match a value in `zone_{OLD_YR}` of `old_zones_{OLD_YR}.xlsx`. |
| `confirmed_new` | Set to `TRUE` when the user confirms a new zone has no old-vintage equivalent. |
| `Juris_zn_{OLD_YR}_spatial` / `pct_overlap_spatial` / `likely_match_spatial` | Raw spatial suggestion, preserved for reference even after the row is auto-accepted. |

## Manual review loop

1. Run the script once to produce the initial working Excel.
2. Open `flu_crosswalk_{OLD_YR}_to_{NEW_YR}_working.xlsx` and filter for
   `needs_review == TRUE`.
3. For each reviewable row, either:
   - Fill `manual_match` with the correct old-vintage zone code, **or**
   - Set `confirmed_new` to `TRUE` if the new zone genuinely has no old
     equivalent.
4. Save the Excel and rerun the script. Overrides are applied and a timestamped
   backup of the edited file is saved automatically.
5. Repeat until `needs_review` is empty.

## Configuration — changing years, inputs, or thresholds

All user-editable settings live at the top of
[`old_flu_crosswalk.py`](old_flu_crosswalk.py) under
`# Settings / configuration`. Edit these in place:

- `CURRENT_FLU_YEAR` / `OLD_FLU_YEAR` — 4-digit integers. Every year
  reference in the script is derived from these two values, including output
  file names and suffixed column names.
- Input paths (`NEW_FLU_CSV_PATH`, `NEW_FLU_GDB_PATH`, `NEW_FLU_GDB_LAYER`,
  `OLD_FLU_SHP_PATH`, `OLD_XWALK_PATH`, `OLD_XWALK_SHEET`,
  `NEW_FLU_CSV_ENCODING`).
- `FUZZY_MATCH_CUTOFF` (default `0.4`) — lower to accept looser fuzzy
  matches, raise to force more rows into `new_zone` / manual review.
- `SPATIAL_OVERLAP_CUTOFF` (default `0.9`) — minimum overlap share required
  to auto-accept a spatial match without manual review.
- `JURISDICTION_ALIASES` — extend when a jurisdiction is spelled differently
  between the two vintages (e.g. `"mlt" -> "mountlaketerrace"`).

### If source column names change

The script assumes the source data uses these base column names:

| Source | Jurisdiction | Zone | Description |
| --- | --- | --- | --- |
| New CSV | `Juris` | `juris_zn` | `Definition` |
| Old shapefile | `Jurisdicti` | `Juris_zn` | `FLUadj_Definition` (joined from `OLD_XWALK_PATH`) |

After the script applies `.add_suffix(f"_{NEW_YR}")` / `f"_{OLD_YR}"` the
corresponding constants become e.g. `NEW_JURIS_COL = f"Juris_{NEW_YR}"`.
If a future vintage uses different base names (e.g. `Jurisdiction` instead of
`Juris`), update the `*_COL` constants in the derived section.

## Running with `uv`

The repo is configured for [uv](https://docs.astral.sh/uv/) — you do **not**
need to create a virtual environment manually. All dependencies (`geopandas`,
`openpyxl`) are declared in the repo's [`pyproject.toml`](../pyproject.toml).

### 1. Install `uv`

Windows PowerShell:

```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Or via `winget`:

```powershell
winget install --id=astral-sh.uv -e
```

See the [uv install docs](https://docs.astral.sh/uv/getting-started/installation/)
for macOS / Linux alternatives.

### 2. Sync the environment

From the repository root (`urbansim-baseyear-prep/`):

```powershell
uv sync
```

This creates `.venv/` and installs everything declared in `pyproject.toml`.
`requires-python` is `>=3.14`; if you don't have that Python installed, uv
will fetch it automatically. You can also pre-install it explicitly:

```powershell
uv python install 3.14
```

### 3. Run the script

```powershell
uv run python future_land_use/old_flu_crosswalk.py
```

`uv run` uses the synced environment without requiring manual activation.

### 4. Adding packages later

```powershell
uv add <package-name>
```

This updates `pyproject.toml` and `uv.lock` and installs into `.venv/`.
