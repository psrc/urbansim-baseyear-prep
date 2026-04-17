"""Build a crosswalk between two Future Land Use (FLU) vintages.

The script matches zones from a new FLU vintage to the previous vintage using
three passes (exact -> normalized -> fuzzy name match), then fills any
remaining gaps via spatial overlap. Finally, any manual overrides recorded in
a working Excel file are applied. See old_flu_crosswalk.md for usage.
"""

import re
from difflib import SequenceMatcher
from pathlib import Path

import geopandas as gpd
import pandas as pd

# ---------------------------------------------------------------------------
# Settings / configuration
# ---------------------------------------------------------------------------

# Years (used everywhere; no hardcoded year literals elsewhere in the script).
CURRENT_FLU_YEAR = 2026
OLD_FLU_YEAR = 2019

# Input data paths
NEW_FLU_CSV_PATH = Path(
    "C:/Users/JKolberg/OneDrive - PSRC/GIS - Projects/FLU/Zoning_2026_d2_cleaned.csv"
)
NEW_FLU_CSV_ENCODING = "latin-1"

NEW_FLU_GDB_PATH = Path(
    "C:/Users/JKolberg/OneDrive - PSRC/GIS - Projects/FLU/FLU_draft2.gdb"
)
NEW_FLU_GDB_LAYER = "FLU2025_cleaned"

OLD_FLU_SHP_PATH = Path("W:/gis/projects/compplan_zoning/flu19_reviewed.shp")

OLD_XWALK_PATH = Path(
    "J:/Staff/Christy/usim-baseyear/flu/Full_FLU_Master_Corres_File.xlsx"
)
OLD_XWALK_SHEET = "Full Master FLU Corres File"

# Output directory (files written here).
OUTPUT_DIR = Path(__file__).parent

# Matching parameters
FUZZY_MATCH_CUTOFF = 0.4       # SequenceMatcher ratio required to accept a fuzzy name match
SPATIAL_OVERLAP_CUTOFF = 0.9   # share of new-zone area overlapping an old zone to auto-accept

# Jurisdiction alias mapping: normalized form -> canonical normalized form.
# Used so the same jurisdiction under different spellings groups together.
# Note: "snohomish" (city) is intentionally NOT mapped to "snohomishcounty".
JURISDICTION_ALIASES = {
    "mlt": "mountlaketerrace",
    "bainbridge": "bainbridgeisland",
    "mercer": "mercerisland",
    "up": "universityplace",
    "snoco": "snohomishcounty",
    "pierce": "piercecounty",
    "kitsap": "kitsapcounty",
    "king": "kingcounty",
}

# ---------------------------------------------------------------------------
# Derived values (don't edit unless source column base names change)
# ---------------------------------------------------------------------------

NEW_YR = str(CURRENT_FLU_YEAR)[-2:]
OLD_YR = str(OLD_FLU_YEAR)[-2:]

# Output file paths
WORKING_XLSX = OUTPUT_DIR / f"flu_crosswalk_{OLD_YR}_to_{NEW_YR}_working.xlsx"
OLD_ZONES_XLSX = OUTPUT_DIR / f"old_zones_{OLD_YR}.xlsx"
FINAL_MASTER_XLSX = OUTPUT_DIR / "Full_FLU_Master_Corres_File.xlsx"
FINAL_MASTER_SHEET = "Full FLU Master Corres File"

# Column names after add_suffix is applied to the two source tables.
# New (current) FLU table: CSV columns + _{NEW_YR}
NEW_JURIS_COL = f"Juris_{NEW_YR}"
NEW_ZONE_COL = f"juris_zn_{NEW_YR}"
NEW_DESC_COL = f"Definition_{NEW_YR}"

# Old FLU attribute table: shapefile columns + _{OLD_YR}
OLD_JURIS_COL = f"Jurisdicti_{OLD_YR}"
OLD_ZONE_COL = f"Juris_zn_{OLD_YR}"
OLD_DESC_COL = f"FLUadj_Definition_{OLD_YR}"

# Unified short column names used inside the crosswalk DataFrame.
ZONE_NEW = f"zone_{NEW_YR}"
ZONE_OLD = f"zone_{OLD_YR}"
DESC_NEW = f"desc_{NEW_YR}"
DESC_OLD = f"desc_{OLD_YR}"

# Spatial overlay keys (kept separate from ZONE_NEW/OLD so the suffixed merge
# produced by add_suffix('_spatial') doesn't collide with existing columns).
SPATIAL_NEW_KEY = f"Juris_zn_{NEW_YR}"
SPATIAL_OLD_KEY = f"Juris_zn_{OLD_YR}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_zone(s) -> str:
    """Normalize a zone string for near-matching: lowercase, strip, collapse separators."""
    s = str(s).strip().lower()
    return re.sub(r"[-_\s,/]+", "", s)


def normalize_jurisdiction(s) -> str:
    """Normalize a jurisdiction name and apply known aliases."""
    norm = normalize_zone(s)
    return JURISDICTION_ALIASES.get(norm, norm)


# ---------------------------------------------------------------------------
# Load source data
# ---------------------------------------------------------------------------

def load_sources():
    """Load the new-vintage CSV, old-vintage shapefile, and old description crosswalk."""
    old_xwalk = (
        pd.read_excel(OLD_XWALK_PATH, sheet_name=OLD_XWALK_SHEET)
        [["FLUadj_Key", "FLUadj_Definition"]]
        .dropna()
    )

    flu_table = pd.read_csv(NEW_FLU_CSV_PATH, encoding=NEW_FLU_CSV_ENCODING)
    flu_table = (
        flu_table
        .drop(columns=[c for c in flu_table.columns if "Unnamed" in c])
        .add_suffix(f"_{NEW_YR}")
    )

    old_flu = gpd.read_file(OLD_FLU_SHP_PATH).merge(
        old_xwalk, left_on="Juris_zn", right_on="FLUadj_Key", how="left"
    )
    old_flu_table = old_flu.drop(columns=["geometry"]).copy().add_suffix(f"_{OLD_YR}")
    old_flu_shp = old_flu[["Juris_zn", "FLUadj_Definition", "geometry"]].copy()

    flu_shp = gpd.read_file(NEW_FLU_GDB_PATH, layer=NEW_FLU_GDB_LAYER)[
        ["Juris_zn", "Definition", "geometry"]
    ]

    return flu_table, old_flu_table, old_flu_shp, flu_shp


# ---------------------------------------------------------------------------
# Build unique per-jurisdiction zone lists
# ---------------------------------------------------------------------------

def build_zone_lists(flu_table, old_flu_table):
    old_zones = (
        old_flu_table[[OLD_JURIS_COL, OLD_ZONE_COL, OLD_DESC_COL]]
        .drop_duplicates()
        .rename(columns={
            OLD_JURIS_COL: "jurisdiction",
            OLD_ZONE_COL: ZONE_OLD,
            OLD_DESC_COL: DESC_OLD,
        })
    )

    new_zones = (
        flu_table[[NEW_JURIS_COL, NEW_ZONE_COL, NEW_DESC_COL]]
        .drop_duplicates()
        .rename(columns={
            NEW_JURIS_COL: "jurisdiction",
            NEW_ZONE_COL: ZONE_NEW,
            NEW_DESC_COL: DESC_NEW,
        })
        .drop_duplicates(subset=["jurisdiction", ZONE_NEW], keep="first")
    )

    print(f"Old zones: {len(old_zones)} unique rows")
    print(f"New zones: {len(new_zones)} unique rows")
    return old_zones, new_zones


# ---------------------------------------------------------------------------
# Name-based crosswalk (exact -> normalized -> fuzzy)
# ---------------------------------------------------------------------------

def build_name_crosswalk(new_zones: pd.DataFrame, old_zones: pd.DataFrame) -> pd.DataFrame:
    """Match new zones to old zones by name, within each jurisdiction."""
    new_zones = new_zones.copy()
    old_zones = old_zones.copy()
    new_zones["jurisdiction_norm"] = new_zones["jurisdiction"].apply(normalize_jurisdiction)
    old_zones["jurisdiction_norm"] = old_zones["jurisdiction"].apply(normalize_jurisdiction)

    rows = []

    for juris_norm in new_zones["jurisdiction_norm"].unique():
        new_j = new_zones[new_zones["jurisdiction_norm"] == juris_norm]
        old_j = old_zones[old_zones["jurisdiction_norm"] == juris_norm]
        juris = new_j["jurisdiction"].iloc[0]  # canonical new-vintage name

        matched_old: set = set()
        matched_new: set = set()

        # Pass 1: exact match on zone name
        for ni, nrow in new_j.iterrows():
            for oi, orow in old_j.iterrows():
                if oi in matched_old:
                    continue
                if str(nrow[ZONE_NEW]).strip() == str(orow[ZONE_OLD]).strip():
                    rows.append({
                        "jurisdiction": juris,
                        ZONE_OLD: orow[ZONE_OLD], DESC_OLD: orow[DESC_OLD],
                        ZONE_NEW: nrow[ZONE_NEW], DESC_NEW: nrow[DESC_NEW],
                        "match_type": "exact", "confidence": 1.0,
                    })
                    matched_old.add(oi)
                    matched_new.add(ni)
                    break

        # Pass 2: normalized near-match on zone name
        remaining_new = new_j[~new_j.index.isin(matched_new)]
        remaining_old = old_j[~old_j.index.isin(matched_old)]
        for ni, nrow in remaining_new.iterrows():
            norm_new = normalize_zone(nrow[ZONE_NEW])
            for oi, orow in remaining_old.iterrows():
                if oi in matched_old:
                    continue
                if norm_new == normalize_zone(orow[ZONE_OLD]):
                    rows.append({
                        "jurisdiction": juris,
                        ZONE_OLD: orow[ZONE_OLD], DESC_OLD: orow[DESC_OLD],
                        ZONE_NEW: nrow[ZONE_NEW], DESC_NEW: nrow[DESC_NEW],
                        "match_type": "near_match", "confidence": 0.8,
                    })
                    matched_old.add(oi)
                    matched_new.add(ni)
                    break

        # Pass 3: fuzzy match for any still-unmatched new zones
        remaining_new = new_j[~new_j.index.isin(matched_new)]
        remaining_old = old_j[~old_j.index.isin(matched_old)]
        for ni, nrow in remaining_new.iterrows():
            best_score = 0.0
            best_oi = None
            best_orow = None
            for oi, orow in remaining_old.iterrows():
                if oi in matched_old:
                    continue
                zone_sim = SequenceMatcher(
                    None,
                    normalize_zone(nrow[ZONE_NEW]),
                    normalize_zone(orow[ZONE_OLD]),
                ).ratio()
                if zone_sim > best_score:
                    best_score = zone_sim
                    best_oi = oi
                    best_orow = orow

            if best_score >= FUZZY_MATCH_CUTOFF and best_orow is not None:
                rows.append({
                    "jurisdiction": juris,
                    ZONE_OLD: best_orow[ZONE_OLD], DESC_OLD: best_orow[DESC_OLD],
                    ZONE_NEW: nrow[ZONE_NEW], DESC_NEW: nrow[DESC_NEW],
                    "match_type": "fuzzy", "confidence": round(best_score, 3),
                })
                matched_old.add(best_oi)
            else:
                # New zone with no old match -- still included for completeness
                rows.append({
                    "jurisdiction": juris,
                    ZONE_OLD: None, DESC_OLD: None,
                    ZONE_NEW: nrow[ZONE_NEW], DESC_NEW: nrow[DESC_NEW],
                    "match_type": "new_zone", "confidence": 0.0,
                })

    crosswalk = pd.DataFrame(rows)
    print(crosswalk["match_type"].value_counts())
    print(f"\nTotal crosswalk rows: {len(crosswalk)}")
    print(
        f"Total new zones: {len(new_zones)} -- all accounted for: "
        f"{len(crosswalk) == len(new_zones)}"
    )
    return crosswalk


def annotate_review_columns(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Add needs_review / manual_match / confirmed_new helper columns."""
    crosswalk = crosswalk.copy()
    crosswalk["needs_review"] = crosswalk["match_type"].isin(["fuzzy", "new_zone"])
    crosswalk["manual_match"] = ""
    crosswalk["confirmed_new"] = False

    print("=== Match Summary ===")
    print(
        crosswalk.groupby("match_type")["confidence"]
        .describe()[["count", "mean", "min", "max"]]
    )
    print(f"\nRows needing manual review: {crosswalk['needs_review'].sum()}")
    print(f"Rows auto-matched (exact + near): {(~crosswalk['needs_review']).sum()}")
    return crosswalk


# ---------------------------------------------------------------------------
# Spatial crosswalk (area-overlap match)
# ---------------------------------------------------------------------------

def build_spatial_xwalk(flu_shp: gpd.GeoDataFrame, old_flu_shp: gpd.GeoDataFrame) -> pd.DataFrame:
    """For each new zone, find the old zone with the greatest area overlap."""
    old_aligned = (
        old_flu_shp.to_crs(flu_shp.crs)
        if flu_shp.crs != old_flu_shp.crs
        else old_flu_shp
    )

    new_zones_geo = (
        flu_shp.dissolve(by="Juris_zn").reset_index()[["Juris_zn", "geometry"]]
        .rename(columns={"Juris_zn": SPATIAL_NEW_KEY})
    )
    old_zones_geo = (
        old_aligned.dissolve(by="Juris_zn").reset_index()[["Juris_zn", "geometry"]]
        .rename(columns={"Juris_zn": SPATIAL_OLD_KEY})
    )

    new_zones_geo["new_area"] = new_zones_geo.geometry.area

    overlay = gpd.overlay(
        new_zones_geo, old_zones_geo, how="intersection", keep_geom_type=True
    )
    overlay["intersection_area"] = overlay.geometry.area
    overlay["pct_overlap"] = overlay["intersection_area"] / overlay["new_area"]

    spatial_xwalk = (
        overlay
        .sort_values("pct_overlap", ascending=False)
        .drop_duplicates(subset=SPATIAL_NEW_KEY, keep="first")
        [[SPATIAL_NEW_KEY, SPATIAL_OLD_KEY, "pct_overlap"]]
        .sort_values("pct_overlap")
        .reset_index(drop=True)
    )

    spatial_xwalk["likely_match"] = spatial_xwalk["pct_overlap"] >= SPATIAL_OVERLAP_CUTOFF

    print(f"Total new zones: {len(spatial_xwalk)}")
    print(
        f"Matches above {SPATIAL_OVERLAP_CUTOFF:.0%} cutoff: "
        f"{spatial_xwalk['likely_match'].sum()}"
    )
    print(f"Below cutoff (needs review): {(~spatial_xwalk['likely_match']).sum()}")
    return spatial_xwalk


def merge_spatial_and_fill(
    crosswalk: pd.DataFrame,
    spatial_xwalk: pd.DataFrame,
    old_zones: pd.DataFrame,
) -> pd.DataFrame:
    """Merge spatial results onto the crosswalk and use them to fill unmatched rows."""
    spatial_suffixed = spatial_xwalk.add_suffix("_spatial")
    spatial_new_key_col = f"{SPATIAL_NEW_KEY}_spatial"
    spatial_old_key_col = f"{SPATIAL_OLD_KEY}_spatial"
    pct_overlap_col = "pct_overlap_spatial"

    crosswalk = crosswalk.merge(
        spatial_suffixed,
        left_on=ZONE_NEW,
        right_on=spatial_new_key_col,
        how="left",
    )

    unmatched = (
        crosswalk["needs_review"]
        & ~crosswalk["confirmed_new"]
        & crosswalk[spatial_old_key_col].notna()
    )

    for idx in crosswalk.loc[unmatched].index:
        spatial_zone = crosswalk.loc[idx, spatial_old_key_col]
        old_match = old_zones[old_zones[ZONE_OLD] == spatial_zone]
        if old_match.empty:
            continue
        pct = crosswalk.loc[idx, pct_overlap_col]
        crosswalk.loc[idx, ZONE_OLD] = spatial_zone
        crosswalk.loc[idx, DESC_OLD] = old_match.iloc[0][DESC_OLD]
        crosswalk.loc[idx, "match_type"] = "spatial"
        crosswalk.loc[idx, "confidence"] = pct
        crosswalk.loc[idx, "needs_review"] = pct < SPATIAL_OVERLAP_CUTOFF

    spatial_matched = (crosswalk["match_type"] == "spatial").sum()
    print(f"Spatially matched: {spatial_matched}")
    print(f"Rows still needing review: {crosswalk['needs_review'].sum()}")
    print(crosswalk["match_type"].value_counts())
    return crosswalk


# ---------------------------------------------------------------------------
# Manual overrides from working Excel file
# ---------------------------------------------------------------------------

def apply_manual_overrides(crosswalk: pd.DataFrame, old_zones: pd.DataFrame) -> pd.DataFrame:
    """Apply confirmed_new flags and manual_match overrides from the working Excel.

    Does nothing (with a notice) if the working Excel doesn't exist yet, so the
    first run can proceed and produce the initial working file.
    """
    if not WORKING_XLSX.exists():
        print(f"No existing {WORKING_XLSX.name} found -- skipping manual override step.")
        return crosswalk

    manual = pd.read_excel(WORKING_XLSX)

    # Backup before applying edits
    now = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
    backup_path = WORKING_XLSX.with_name(f"{WORKING_XLSX.stem}_backup_{now}.xlsx")
    manual.to_excel(backup_path, index=False)

    # Apply confirmed_new flags
    if "confirmed_new" in manual.columns:
        confirmed = manual[
            manual["confirmed_new"].astype(str).str.strip().str.upper()
            .isin(["TRUE", "1", "YES"])
        ]
        for _, row in confirmed.iterrows():
            mask = (
                (crosswalk["jurisdiction"] == row["jurisdiction"])
                & (crosswalk[ZONE_NEW] == row[ZONE_NEW])
            )
            if mask.any():
                crosswalk.loc[mask, "confirmed_new"] = True
                crosswalk.loc[mask, "needs_review"] = False
                print(f"  Confirmed new: {row['jurisdiction']} / {row[ZONE_NEW]}")
        print(f"Applied {len(confirmed)} confirmed_new flags")

    # Apply manual_match overrides
    manual_edits = manual[
        manual["manual_match"].notna()
        & (manual["manual_match"].astype(str).str.strip() != "")
    ]
    print(f"Found {len(manual_edits)} manual adjustments")

    for _, row in manual_edits.iterrows():
        mask = (
            (crosswalk["jurisdiction"] == row["jurisdiction"])
            & (crosswalk[ZONE_NEW] == row[ZONE_NEW])
        )
        if not mask.any():
            print(f"  WARNING: No match in crosswalk for {row['jurisdiction']} / {row[ZONE_NEW]}")
            continue

        manual_val = str(row["manual_match"]).strip()
        crosswalk.loc[mask, "manual_match"] = manual_val
        old_match = old_zones[old_zones[ZONE_OLD] == manual_val]
        if not old_match.empty:
            crosswalk.loc[mask, ZONE_OLD] = manual_val
            crosswalk.loc[mask, DESC_OLD] = old_match.iloc[0][DESC_OLD]
            crosswalk.loc[mask, "match_type"] = "manual"
            crosswalk.loc[mask, "confidence"] = 1.0
            crosswalk.loc[mask, "needs_review"] = False
        print(f"  Updated: {row['jurisdiction']} / {row[ZONE_NEW]} -> {manual_val}")

    print("\n=== Updated Match Summary ===")
    print(crosswalk["match_type"].value_counts())
    print(f"Rows still needing review: {crosswalk['needs_review'].sum()}")
    return crosswalk


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_working_files(crosswalk: pd.DataFrame, old_zones: pd.DataFrame) -> None:
    crosswalk.to_excel(WORKING_XLSX, index=False)
    old_zones.to_excel(OLD_ZONES_XLSX, index=False)
    print(f"Saved {WORKING_XLSX.name} and {OLD_ZONES_XLSX.name}")


def write_final_master(crosswalk: pd.DataFrame) -> None:
    """Write the final, cleaned-up master corres file with standardized column names."""
    final = crosswalk.copy()
    final["FLU_master_id"] = final.index + 1
    juris_zn_new = f"juris_zn_{NEW_YR}"
    juris_zn_old = f"juris_zn_{OLD_YR}"
    final = final.rename(columns={ZONE_NEW: juris_zn_new, ZONE_OLD: juris_zn_old})
    out_cols = [
        "FLU_master_id",
        "jurisdiction",
        juris_zn_new,
        DESC_NEW,
        juris_zn_old,
        DESC_OLD,
    ]
    final[out_cols].to_excel(
        FINAL_MASTER_XLSX, index=False, sheet_name=FINAL_MASTER_SHEET
    )
    print(f"Saved {FINAL_MASTER_XLSX.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    flu_table, old_flu_table, old_flu_shp, flu_shp = load_sources()

    old_zones, new_zones = build_zone_lists(flu_table, old_flu_table)

    crosswalk = build_name_crosswalk(new_zones, old_zones)
    crosswalk = annotate_review_columns(crosswalk)

    spatial_xwalk = build_spatial_xwalk(flu_shp, old_flu_shp)
    crosswalk = merge_spatial_and_fill(crosswalk, spatial_xwalk, old_zones)

    crosswalk = apply_manual_overrides(crosswalk, old_zones)

    write_working_files(crosswalk, old_zones)
    write_final_master(crosswalk)


if __name__ == "__main__":
    main()
