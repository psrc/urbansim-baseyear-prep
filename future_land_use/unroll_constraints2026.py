import os
import pandas as pd
import geopandas as gpd
import numpy as np
from datetime import date
from itertools import combinations

from util.elmer_helpers import read_from_elmer_geo
from util.unroll_constraints_functions import check_multi_pins

# config ----------------------------------------------------

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# root dir
ROOT = "Q:/Projects/2023_Baseyear/FLU_and_Lockouts"

# output dir
OUTPUT = os.path.join(ROOT, "unroll_constraints")

# flu gis layer
# flu_shp_path = os.path.join(flu_input_dir, "FLU_draft2.gdb")
# flu_layer = "FLU2025" # name of layer within gdb
FLU_SHP_PATH = os.path.join(ROOT, "GIS/FLU_2025/FLU_20260522/QC/FLU2025_final.shp")
juris_zn_shp_id = 'Juris_zn' # unique id column

# imputed FLU data directory (output from imputation.R)
FLU_INPUT_DIR = os.path.join(ROOT, "imputation_data")
FLU_IMP =  "final_flu_imputed_2026-06-03.csv"
FLU_IMP_PATH = os.path.join(FLU_INPUT_DIR, FLU_IMP)
juris_zn_imputed_id = 'juris_zn' # unique id column

# parcels file
BASE_YEAR_PRCL_LAYER = "parcels_urbansim_2023_pts" # ElmerGeo layer name for parcel points

# urbansim baseyear cache
CACHE = 'L:/base_year_2023_inputs/JobParcel/base_year_data/2023/parcels'  # BY 2023

# read/process files ----------------------------------------------------

# read in flu gis layer
flu_shp = (
    # gpd.read_file(flu_shp_path, layer = flu_layer)
    gpd.read_file(FLU_SHP_PATH)
    .rename(columns={juris_zn_shp_id:'juris_zn'})
)[['juris_zn', 'geometry']]
flu_shp = flu_shp.to_crs(epsg=2285)

# read in flu imputed data
f = (
    pd.read_csv(FLU_IMP_PATH)
    .rename(columns={juris_zn_imputed_id:'juris_zn'})
)

# clean up f; remove extra/unecessary fields before join
f_col_keep = [col for col in f.columns if col not in ['Juris', 'Key', 'Zone', 'Definition'] + list(f.columns[f.columns.str.endswith("src")])]
f = f[f_col_keep]
f['plan_type_id'] = np.arange(len(f)) + 1 # assign plan_type_id

# join imputed data back to FLU shapefile
flu = flu_shp.merge(f, on = ['juris_zn'], how = 'left')

# spatial join parcels to flu to assign plan_type_id------------------------------------------------

# read parcels file & lu type file
prcls = read_from_elmer_geo(BASE_YEAR_PRCL_LAYER, cols = ['parcel_id'])
#pin_name = "PIN" # BY 2018
pin_name = "parcel_id" # BY 2023

#prcls_pin = np.fromfile(os.path.join(CACHE, 'parcel_id.li4'), np.int32) # BY 2018
#prcls_lu = np.fromfile(os.path.join(CACHE, 'land_use_type_id.li4'), np.int32) # BY 2018
prcls_pin = np.fromfile(os.path.join(CACHE, 'parcel_id.li8'), np.int64) # BY 2023
prcls_lu = np.fromfile(os.path.join(CACHE, 'land_use_type_id.li8'), np.int64) # BY 2023
prcls_tod = np.fromfile(os.path.join(CACHE, 'tod_id.li4'), np.int32)
lu_type = pd.DataFrame({pin_name:prcls_pin, 'lu_type':prcls_lu, 'tod_id':prcls_tod}, index = prcls_pin)
#lu_type['PIN'] = lu_type['PIN'].astype(np.int64) # BY 2018

prcls[pin_name] = prcls[pin_name].astype(np.int64)
prcls = prcls.merge(lu_type, on = pin_name)

# spatial join parcels to flu shp
prcls_flu = gpd.sjoin(prcls, flu,how = 'left')
# separate out parcels that did not join to flu (i.e. those with no juris_zn match)
unmatched = prcls_flu.loc[prcls_flu['juris_zn'].isna(),['parcel_id','geometry','lu_type','tod_id']].copy()
print(f"Number of parcels with no spatial match to FLU shp: {len(unmatched)} out of {len(prcls)} total parcels. Unmatched parcels will be joined to nearest FLU polygon.")
if len(unmatched) > len(prcls) * 0.05: # if more than 5% of parcels have no match throw an error
    raise RuntimeError(f"Warning: {len(unmatched)} parcels have no spatial match to FLU shapefile, which is more than 5% of total parcels. Check data and spatial join parameters.")
# for unmatched parcels, assign plan_type_id based on nearest flu polygon
unmatched_flu = gpd.sjoin_nearest(unmatched, flu)
# remove unmatched parcels from prcls_flu
prcls_flu = prcls_flu.loc[~prcls_flu['juris_zn'].isna()].copy()
# concat the sjoin nearest results back to prcls_flu
prcls_flu = pd.concat([prcls_flu, unmatched_flu], ignore_index=True)

# flag parcels with no match in imputed flu data (plan_type_id is still null)
prcls_flu['no_flu_match'] = 0
prcls_flu.loc[prcls_flu['plan_type_id'].isna(),'no_flu_match'] = 1

# QC FLU shapefile------------------------------------------------------------------

check_multi_pins(prcls_flu, OUTPUT, pin_name = pin_name) #check code

# check for one-to-many records in flu overlay
prcls_flu[pin_name].duplicated().any()
duplicate = prcls_flu[prcls_flu.duplicated(pin_name)][[pin_name]] #221
dup_df = prcls_flu[prcls_flu[pin_name].isin(duplicate[pin_name])].sort_values(by=[pin_name]) #417 (BY 2018)

# count frequency of PINs
dup_pin_freq = dup_df.groupby([pin_name])[pin_name].count().reset_index(name='counts')
triple_pin = dup_pin_freq[dup_pin_freq['counts']>2] # handle triple count pins separately

# de-duplicate parcels with multiple matches in flu spatial join, 
# keeping non-county-level juris_zn matches where possible (i.e. prioritize city-level over county-level matches)
county_juris_zns = [
    'king_county',
    'snohomish_county',
    'pierce_county',
    'kitsap_county',
    'king',
    'pierce',
    'kitsap'
    'snoco'
    # snohomish # leave out Snohomish because it is also a muni
]

# re-assemble all parcels
unjoined = prcls[~prcls[pin_name].isin(prcls_flu[pin_name])]
x1 = prcls_flu[~prcls_flu[pin_name].isin(dup_df[pin_name])] # records with no duplicates

x2 = dup_df[~dup_df['plan_type_id'].isnull() & ~dup_df[pin_name].isin(triple_pin[pin_name])] # duplicates where plan_type_id is not null. Excludes triple_pin
x2a = dup_df[dup_df[pin_name].isin(triple_pin[pin_name]) & ~dup_df['plan_type_id'].isnull()] # triple_pin where plan_type_id is not null

# sort so county-level juris_zn rows (lowest priority) come last within each pin group
x2['_is_county'] = x2['juris_zn'].str.lower().str.startswith(tuple(county_juris_zns))
x2 = x2.sort_values(by=[pin_name, '_is_county'])
x2_kp_first = x2.drop_duplicates(subset=[pin_name], keep='first').drop(columns=['_is_county']) # remove duplicates (keep first non-county)

x2a['_is_county'] = x2a['juris_zn'].str.lower().str.startswith(tuple(county_juris_zns))
x2a = x2a.sort_values(by=[pin_name, '_is_county'])
x2a_kp_first = x2a.drop_duplicates(subset=[pin_name], keep='first').drop(columns=['_is_county']) # remove duplicates amongst triple pins (keep first non-county)

# append all tables
len(prcls) - (len(x1) + len(unjoined) + len(x2_kp_first) + len(x2a_kp_first))
all_df = pd.concat([x1, x2_kp_first, x2a_kp_first, unjoined])

# For each duplicated parcel_id, collect the sorted set of juris_zn values it intersects,
# then enumerate all juris_zn pairs and count how often each pair co-occurs on a parcel.
juris_pairs = (
    dup_df.groupby(pin_name)['juris_zn']
          .apply(lambda s: list(combinations(sorted(s.dropna().unique()), 2)))
)

pair_counts = (
    pd.Series([p for pairs in juris_pairs for p in pairs])
      .value_counts()
      .rename_axis('juris_zn_pair')
      .reset_index(name='n_duplicated_parcels')
)

pair_counts[['juris_zn_1', 'juris_zn_2']] = pd.DataFrame(
    pair_counts['juris_zn_pair'].tolist(), index=pair_counts.index
)
pair_counts = pair_counts[['juris_zn_1', 'juris_zn_2', 'n_duplicated_parcels']]
pair_counts.to_csv(os.path.join(OUTPUT,"flu_qc", 'flu_juris_zn_pair_counts_' + str(date.today()) + '.csv'), index=False)

#### create development constraints table------------------------------------------------------------------
# divide lot coverage by 100 to convert from percentage to proportion
for lc_col in ['LC_Res', 'LC_Office', 'LC_Comm', 'LC_Indust', 'LC_Mixed']:
    f[lc_col] = f[lc_col] / 100

# unroll constraints from plan_type
id_cols = ['plan_type_id', 'generic_land_use_type_id', 'constraint_type']

# --- Residential SF/MF unroll -----------------------------------------------
# Logic:
#   * If SingleFamily_Use / MultiFamily_Use flags are populated ('Y'), use them.
#   * If both flags are NA, fall back to the old density-based logic:
#       - MaxDU_Res < 11.9  -> SF
#       - MaxDU_Res >= 11.9  -> MF

res        = f['Res_Use'] == 1
flags_na   = f['SingleFamily_Use'].isna() & f['MultiFamily_Use'].isna()
sf_flagged = (f['SingleFamily_Use'] == 'Y') & (f['MultiFamily_Use'] != 'Y')  # if both flags are Y, default to MF
mf_flagged = (f['MultiFamily_Use'] == 'Y')

res_cols   = ['MinDU_Res', 'MaxDU_Res', 'LC_Res', 'MaxHt_Res']
res_rename = {'MinDU_Res': 'minimum', 'MaxDU_Res': 'maximum',
              'LC_Res': 'lc', 'MaxHt_Res': 'maxht'}

def _build_res(mask, glu_id):
    out = f.loc[mask].copy()
    out['generic_land_use_type_id'] = glu_id
    out['constraint_type'] = 'units_per_acre'
    return out[id_cols + res_cols].rename(columns=res_rename)

# --- SF ---
sf_old_mask = res & flags_na & (f['MaxDU_Res'] < 35.1)
sf_new_mask = res & sf_flagged
sf_old = _build_res(sf_old_mask, 1)
sf_new = _build_res(sf_new_mask, 1)
sf = pd.concat([sf_old, sf_new], ignore_index=True)
print(f"SF (old/density-only, flags NA & MaxDU_Res < 35.1): {len(sf_old)}")
print(f"SF (new flag, SingleFamily_Use == 'Y'):              {len(sf_new)}")
print(f"SF total rows going into devconstr:                  {len(sf)}")

# --- MF ---
mf_old_mask = res & flags_na & (f['MaxDU_Res'] > 11.9)
mf_new_mask = res & mf_flagged
mf_old = _build_res(mf_old_mask, 2)
mf_new = _build_res(mf_new_mask, 2)
mf = pd.concat([mf_old, mf_new], ignore_index=True)
print(f"MF (old/density-only, flags NA & MaxDU_Res > 11.9):  {len(mf_old)}")
print(f"MF (new flag, MultiFamily_Use == 'Y'):               {len(mf_new)}")
print(f"MF total rows going into devconstr:                  {len(mf)}")

# --- Diagnostics (optional) ---
print(f"Res_Use==1 rows captured by none of the four subsets: "
      f"{int((res & ~(sf_old_mask | mf_old_mask | sf_new_mask | mf_new_mask)).sum())}")

# --- Office ---
off = f[(f['Office_Use'] == 1)]
off['generic_land_use_type_id'] = 3
off['constraint_type'] = 'far'
off = off[id_cols + ['MinFAR_Office', 'MaxFAR_Office', 'LC_Office', 'MaxHt_Office']]
off = off.rename(columns = {'MinFAR_Office': 'minimum', 'MaxFAR_Office': 'maximum', 'LC_Office':'lc', 'MaxHt_Office':'maxht'})

# comm
comm = f[(f['Comm_Use'] == 1)]
comm['generic_land_use_type_id'] = 4
comm['constraint_type'] = 'far'
comm = comm[id_cols + ['MinFAR_Comm', 'MaxFAR_Comm', 'LC_Comm', 'MaxHt_Comm']]
comm = comm.rename(columns = {'MinFAR_Comm': 'minimum', 'MaxFAR_Comm': 'maximum', 'LC_Comm':'lc', 'MaxHt_Comm':'maxht'})

# ind
ind = f[(f['Indust_Use'] == 1)]
ind['generic_land_use_type_id'] = 5
ind['constraint_type'] = 'far'
ind = ind[id_cols + ['MinFAR_Indust', 'MaxFAR_Indust', 'LC_Indust', 'MaxHt_Indust']]
ind = ind.rename(columns = {'MinFAR_Indust': 'minimum', 'MaxFAR_Indust': 'maximum', 'LC_Indust':'lc', 'MaxHt_Indust':'maxht'})

# mixed
mixed = f[(f['Mixed_Use'] == 1)]
mixed['generic_land_use_type_id'] = 6
mixed['constraint_type'] = 'far'
mixed = mixed[id_cols + ['MinFAR_Mixed', 'MaxFAR_Mixed', 'LC_Mixed', 'MaxHt_Mixed']]
mixed = mixed.rename(columns = {'MinFAR_Mixed': 'minimum', 'MaxFAR_Mixed': 'maximum', 'LC_Mixed':'lc', 'MaxHt_Mixed':'maxht'})

# mixed du
mixed_du = f[(f['Mixed_Use'] == 1)]
mixed_du['generic_land_use_type_id'] = 6
mixed_du['constraint_type'] = 'units_per_acre'
mixed_du = mixed_du[id_cols + ['MinDU_Res', 'MaxDU_Res', 'LC_Mixed', 'MaxHt_Mixed']]
mixed_du = mixed_du.rename(columns = {'MinDU_Res': 'minimum', 'MaxDU_Res': 'maximum', 'LC_Mixed':'lc', 'MaxHt_Mixed':'maxht'})

# sf du per lot
sf_du_lot = f[(f['Res_Use'] == 1) & (f['MaxDU_lot'] > 0) & (f['MaxDU_lot'] <= 2)].copy()
sf_du_lot['generic_land_use_type_id'] = 1
sf_du_lot['constraint_type'] = 'units_per_lot'
sf_du_lot['MinDU_lot'] = sf_du_lot['MinDU_lot'].fillna(2)  # default min for SF
sf_du_lot = sf_du_lot[id_cols + ['MinDU_lot','MaxDU_lot', 'LC_Res', 'MaxHt_Res']]
sf_du_lot = sf_du_lot.rename(columns = {'MinDU_lot': 'minimum', 'MaxDU_lot': 'maximum', 'LC_Res':'lc', 'MaxHt_Res':'maxht'})

# mf du per lot
mf_du_lot = f[(f['Res_Use'] == 1) & (f['MaxDU_lot'] > 2)].copy()
mf_du_lot['generic_land_use_type_id'] = 2
mf_du_lot['constraint_type'] = 'units_per_lot'
mf_du_lot['MinDU_lot'] = mf_du_lot['MinDU_lot'].fillna(3)  # default min for MF
mf_du_lot = mf_du_lot[id_cols + ['MinDU_lot','MaxDU_lot', 'LC_Res', 'MaxHt_Res']]
mf_du_lot = mf_du_lot.rename(columns = {'MinDU_lot': 'minimum', 'MaxDU_lot': 'maximum', 'LC_Res':'lc', 'MaxHt_Res':'maxht'})

# combine together and add lockouts
lockout_id = 9999
devconstr = pd.concat([sf, mf, off, comm, ind, mixed, mixed_du, sf_du_lot, mf_du_lot], sort=False)

# clamp minimum to maximum when minimum > maximum (ignore NaNs)
_min_gt_max = devconstr['minimum'].notna() & devconstr['maximum'].notna() & (devconstr['minimum'] > devconstr['maximum'])
print(f"Rows where minimum > maximum (clamped to maximum): {int(_min_gt_max.sum())}")
devconstr.loc[_min_gt_max, 'minimum'] = devconstr.loc[_min_gt_max, 'maximum']

## consistency check (ptids)
ptid_qc_dir = os.path.join(OUTPUT, "ptid_qc")
os.makedirs(ptid_qc_dir, exist_ok = True)

common = f.merge(devconstr,on=['plan_type_id','plan_type_id'])
not_in_devconstr = f.loc[(~f.plan_type_id.isin(common.plan_type_id)), ['plan_type_id', 'FLU_master_id', 'juris_zn']]
# The reason for being in f but not devcostr is that they did not fit into any of the categories above (sf, mf, com ...).
# This could happen if instead of "Yes" in the use column, these records have some text.
# After reviewing these records we decided to lock them out.
print('WARNING: The following ptids are in object f but not devconstr:\n')
print(not_in_devconstr)
not_in_devconstr.to_csv(os.path.join(ptid_qc_dir, r'ptid_consistency_qc_notindevconstr_' + str(date.today()) + '.csv'), index=False)


max_zero_devconstr = devconstr.groupby(["plan_type_id"]).maximum.sum().reset_index()
max_zero = max_zero_devconstr[max_zero_devconstr['maximum'] == 0]
print('The following are non-9*** lockout plan types')
print(max_zero)
max_zero.to_csv(os.path.join(ptid_qc_dir, r'ptid_consistency_qc_maxzero_' + str(date.today()) + '.csv'), index=False)

# create df of plan_type_id 9999
lockout_df = pd.DataFrame({'plan_type_id': np.repeat(lockout_id, 7),
              'generic_land_use_type_id': list(np.arange(1, 7)) + [6],
              'minimum': 0,
              'maximum': 0,
              'lc': 1,
              'constraint_type': list(np.repeat("units_per_acre", 2)) + list(np.repeat("far", 4)) + ["units_per_acre"]})

devconstr = pd.concat([devconstr, lockout_df], sort=False)

# replace NA with 0, or 1 for Lot Coverage (lc)
devconstr.loc[devconstr['minimum'].isnull(), 'minimum'] = 0
devconstr.loc[devconstr['maximum'].isnull(), 'maximum'] = 0
devconstr.loc[devconstr['lc'].isnull(), 'lc'] = 1
devconstr.loc[devconstr['maxht'].isnull(), 'maxht'] = 0

# add an id column
devconstr['development_constraint_id']= np.arange(len(devconstr)) + 1

# export files ---------------------------------------------------------------------
res_constr_dir = os.path.join(OUTPUT, "dev_constraints")
os.makedirs(res_constr_dir, exist_ok = True)

res_flu_dir = os.path.join(OUTPUT, "flu")
os.makedirs(res_flu_dir, exist_ok = True)

devconstr.to_csv(os.path.join(res_constr_dir, r'devconstr_no_lockouts_' + str(date.today()) + '.csv'), index=False) 
f.to_csv(os.path.join(res_flu_dir, r'flu_imputed_ptid_' + str(date.today()) + '.csv'), index=False) # flu imputed kitchen sink file

prcls_flu_ptid = all_df[[pin_name, 'plan_type_id', 'tod_id']]
prcls_flu_ptid.to_csv(os.path.join(res_constr_dir, r'prcls_ptid_no_lockouts_' + str(date.today()) + '.csv'), index=False)
#all_df.to_file(os.path.join(OUTPUT, r'shapes\prclpt18_ptid_' + str(date.today()) + '.shp')) # Warning! Takes a long time to write!

#### post-processing lockouts ----------------------------------------------------------

# append to development constraints
lo_df = pd.DataFrame()
for x in range(9001, 9008):
    lockout_ptid_df = pd.DataFrame({'plan_type_id': np.repeat(x, 7),
                  'generic_land_use_type_id': list(np.arange(1, 7)) + [6],
                  'minimum': 0,
                  'maximum': 0,
                  'lc': 1,
                  'constraint_type': list(np.repeat("units_per_acre", 2)) + list(np.repeat("far", 4)) + ["units_per_acre"],
                  'maxht': 0})
    if lo_df.empty:
        lo_df = lockout_ptid_df
    else:
        lo_df = pd.concat([lo_df, lockout_ptid_df])

dci = devconstr['development_constraint_id'].max()
lo_df['development_constraint_id'] = list(np.arange(dci+1, dci+len(lo_df)+1))
devconstr = pd.concat([devconstr, lo_df]) 

# update plan_type_ids
all_df.loc[all_df['plan_type_id'].isnull(), 'plan_type_id'] = lockout_id
all_df.loc[all_df['plan_type_id'].isin(not_in_devconstr['plan_type_id']), 'plan_type_id'] = lockout_id  # lock plan types not found in devconstr
all_df.loc[all_df['lu_type'] == 23, 'plan_type_id'] = 9001 # Schools/universities
all_df.loc[all_df['lu_type'] == 7, 'plan_type_id'] = 9002 # Government
all_df.loc[all_df['lu_type'] == 9, 'plan_type_id'] = 9003 # Hospitals, convalescent center
all_df.loc[all_df['lu_type'] == 6, 'plan_type_id'] = 9004 # Forest, protected
all_df.loc[all_df['lu_type'] == 5, 'plan_type_id'] = 9005 # Forest, harvestable
all_df.loc[all_df['lu_type'] == 1, 'plan_type_id'] = 9006 # Agriculture
all_df.loc[all_df['lu_type'] == 27, 'plan_type_id'] = 9007 # Vacant undevelopable

# export post-processing lockouts version
prcls_flu_ptid_lockouts = all_df[[pin_name, 'plan_type_id', 'tod_id']]
prcls_flu_ptid_lockouts.to_csv(os.path.join(res_constr_dir, r'prcls_ptid_final_' + str(date.today()) + '.csv'), index=False)
devconstr.to_csv(os.path.join(res_constr_dir, r'devconstr_final_' + str(date.today()) + '.csv'), index=False)

# QC check on number of parcels with missing FLU match (plan_type_id 9999)
(all_df[all_df['plan_type_id'] == 9999]
    .groupby('juris_zn').size().reset_index(name='num_parcels')
    .sort_values(by='num_parcels', ascending=False)
    .to_csv(os.path.join(OUTPUT,'flu_qc', r'parcels_no_table_match_' + str(date.today()) + '.csv'), index=False)
)
print(f"Number of parcels with missing FLU match: {len(all_df[all_df['plan_type_id'] == 9999])} parcels will be assigned plan type id 9999")