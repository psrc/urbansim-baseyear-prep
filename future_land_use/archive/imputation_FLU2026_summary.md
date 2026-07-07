# Summary of `imputation_FLU2026.R`

## Purpose
Clean the 2026 Future Land Use (FLU) zoning table and fill in (impute) missing density, height, and FAR values using statistically estimated coefficients, producing a finished table for downstream constraint-unrolling.

## Steps

1. **Load data** — Reads the new 2026 FLU table (via `load_FLU2026.R`), the old 2019 FLU table, and a master lookup/crosswalk. Joins a shared `FLU_master_id` onto both old and new FLU records.

2. **Tag collected values** — Marks density/height columns that already have real collected data with a `_src = 'collected'` source flag.

3. **Initial cleaning** — Processes the `rural` flag and runs general cleanup (`process_rural`, `more_cleaning`).

4. **Assert values from related uses** — Fills gaps by borrowing values across uses (e.g., use mixed-use height/FAR for a specific use, or take the max across allowed uses for mixed-use), tagging these as `'asserted'`.

5. **Derive floor heights** — Computes assumed per-floor heights from the old FLU (from height, lot coverage, and FAR), clipped to the interquartile range, for later FAR estimation.

6. **Estimate FAR / DU from height** — Where only height is available, estimates FAR using height × lot coverage / floor height, then converts FAR to dwelling-units-per-acre using efficiency and square-footage-per-unit assumptions (tagged `'estimated'`).

7. **Backfill from previous (2019) FLU** — Joins old to new and, where new values are missing, copies the prior values into `_imp` columns (tagged `'prev'`), for both densities and heights.

8. **Impute via regression model** — Uses estimated coefficients to fill remaining missing residential DU/acre and height (log-linear model based on height, lot coverage, and rural status), and imputes non-residential FAR/height from height and lot coverage (tagged `'imputed'`).

9. **Finalize** — Drops helper/`_prev` columns, renames `_imp`/`_new` columns to clean names, removes duplicate rows, and orders columns.

10. **Output & QC** — Writes a "kitchen sink" QC file (`temp_flu_imputed_*.csv`) and the final file (`final_flu_imputed_*.csv`) for use with `unroll_constraints2026.py`, then prints summary tables of value sources (counts and percentages).

## Key Theme
A **waterfall of imputation strategies** — collected → asserted → estimated → previous → model-imputed — with every filled value tracked by a `_src` source tag.
