import os
import pandas as pd
import geopandas as gpd
import numpy as np
from datetime import date

# config ----------------------------------------------------

exec(open(r'C:\Users\CLam\github\urbansim-baseyear-prep\future_land_use\unroll_constraints_functions.py').read())

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# root outdir
dir = r"J:\Staff\Christy\usim-baseyear"

# original flu shape
flu_shp_path = r"W:\gis\projects\compplan_zoning\flu19_reviewed.shp" 

# imputed data
flu_imp = os.path.join(dir, r'flu\final_flu_postprocessed_2023-01-10.csv') #old 09-22

# parcels file
base_year_prcl_path = r"J:\Projects\2018_base_year\Region\prclpt18.shp"

# read/process files ----------------------------------------------------

flu_shp = gpd.read_file(flu_shp_path)
f = pd.read_csv(flu_imp) # 1921 rows

# clean up f; remove extra/unecessary fields before join
f_col_keep = [col for col in f.columns if col not in ['Jurisdicti', 'Key', 'Zone_adj', 'Definition'] + list(f.columns[f.columns.str.endswith("src")])]
f = f[f_col_keep]
f['plan_type_id'] = np.arange(len(f)) + 1 # assign plan_type_id

# join imputed data back to FLU shapefile
flu = flu_shp.merge(f, on = ['Juris_zn'], how = 'left')

#flu.to_file(os.path.join(dir, r'shapes\flu_for_qc.shp')) # export flu for qc in arcgis

# spatial join parcels to flu to assign plan_type_id----------------------------------------------------

# read parcels file & lu type file
prcls = gpd.read_file(base_year_prcl_path)

cache = r'N:\base_year_2018_inputs\gold_standard_inputs\2018\parcels'
prcls_pin = np.fromfile(os.path.join(cache, 'parcel_id.li4'), np.int32)
prcls_lu = np.fromfile(os.path.join(cache, 'land_use_type_id.li4'), np.int32)
prcls_tod = np.fromfile(os.path.join(cache, 'tod_id.li4'), np.int32)
lu_type = pd.DataFrame({'PIN':prcls_pin, 'lu_type':prcls_lu, 'tod_id':prcls_tod}, index = prcls_pin)
lu_type['PIN'] = lu_type['PIN'].astype(np.int64)

prcls = prcls.merge(lu_type, on = 'PIN')

prcls_flu = gpd.sjoin(prcls, flu)

# QC FLU shapefile------------------------------------------------------------------

check_multi_pins(prcls_flu, dir) #check code

# check for one-to-many records in flu overlay
prcls_flu['PIN'].duplicated().any()
duplicate = prcls_flu[prcls_flu.duplicated('PIN')][['PIN']] #221
dup_df = prcls_flu[prcls_flu['PIN'].isin(duplicate['PIN'])].sort_values(by=['PIN']) #417

#dup_df.to_csv(os.path.join(dir, r'flu_qc\pins_dup_'+ str(date.today()) +'.csv'), index=False) 
#dup_df.to_file(os.path.join(dir, r'flu_qc\prcls_pins_dup_'+ str(date.today()) +'.shp'))

# count frequency of PINs
dup_pin_freq = dup_df.groupby(['PIN'])['PIN'].count().reset_index(name='counts')
triple_pin = dup_pin_freq[dup_pin_freq['counts']>2] # handle triple count pins separately

# re-assemble all parcels

unjoined = prcls[~prcls['PIN'].isin(prcls_flu['PIN'])] # 4237 recs
x1 = prcls_flu[~prcls_flu['PIN'].isin(dup_df['PIN'])] # no duplicates

x2 = dup_df[~dup_df['plan_type_id'].isnull() & ~dup_df['PIN'].isin(triple_pin['PIN'])] # duplicates where plan_type_id is not null. Excludes triple_pin
x2a = dup_df[dup_df['PIN'].isin(triple_pin['PIN']) & ~dup_df['plan_type_id'].isnull()] # triple_pin where plan_type_id is not null

x2_kp_first = x2.drop_duplicates(subset=['PIN'], keep='first') # remove duplicates (keep first)
x2a_kp_first = x2a.drop_duplicates(subset=['PIN'], keep='first') # remove duplicates amongst triple pins (keep first)

### works with old final flu post-processing-2022-09-22 --------------------------------------------------
### contained duplicates of null ptids
#t_null = dup_df[dup_df['plan_type_id'].isnull() & ~dup_df['PIN'].isin(triple_pin['PIN'])] # duplicates amongst nulls. Excludes triple_pin
#t_dup_pin_freq = t_null.groupby(['PIN'])['PIN'].count().reset_index(name='count')
#t_dup_pin_freq_dups = t_dup_pin_freq[t_dup_pin_freq['count']>1]
#null_dup = t_null[t_null['PIN'].isin(t_dup_pin_freq_dups['PIN'])] 
#x3 = null_dup.drop_duplicates(subset=['PIN'], keep='first') # remove duplicates amongst nulls (keep first)
##len(x1) + len(x2) + len(x2a) + len(x3) + len(unjoined) # 1302434 recs
## append all tables
#all_df = pd.concat([x1, x2, x2a, x3, unjoined])
### --------------------------------------------------

# append all tables
len(prcls) - (len(x1) + len(unjoined) + len(x2_kp_first) + len(x2a_kp_first))
all_df = pd.concat([x1, x2_kp_first, x2a_kp_first, unjoined])


#### create development constraints table------------------------------------------------------------------

# unroll constraints from plan_type
id_cols = ['plan_type_id', 'generic_land_use_type_id', 'constraint_type']

# sf
sf = f[(f['MaxDU_Res'] < 35.1) & (f['Res_Use'] == 'Y')]
sf['generic_land_use_type_id'] = 1
sf['constraint_type'] = 'units_per_acre'
sf = sf[id_cols + ['MinDU_Res', 'MaxDU_Res', 'LC_Res', 'MaxHt_Res']]
sf = sf.rename(columns = {'MinDU_Res': 'minimum', 'MaxDU_Res': 'maximum', 'LC_Res':'lc', 'MaxHt_Res':'maxht'})

# mf
mf = f[(f['MaxDU_Res'] > 11.9) & (f['Res_Use'] == 'Y')]
mf['generic_land_use_type_id'] = 2
mf['constraint_type'] = 'units_per_acre'
mf = mf[id_cols + ['MinDU_Res', 'MaxDU_Res', 'LC_Res', 'MaxHt_Res']]
mf = mf.rename(columns = {'MinDU_Res': 'minimum', 'MaxDU_Res': 'maximum', 'LC_Res':'lc', 'MaxHt_Res':'maxht'})

# off
off = f[(f['Office_Use'] == 'Y')]
off['generic_land_use_type_id'] = 3
off['constraint_type'] = 'far'
off = off[id_cols + ['MinFAR_Office', 'MaxFAR_Office', 'LC_Office', 'MaxHt_Office']]
off = off.rename(columns = {'MinFAR_Office': 'minimum', 'MaxFAR_Office': 'maximum', 'LC_Office':'lc', 'MaxHt_Office':'maxht'})

# comm
comm = f[(f['Comm_Use'] == 'Y')]
comm['generic_land_use_type_id'] = 4
comm['constraint_type'] = 'far'
comm = comm[id_cols + ['MinFAR_Comm', 'MaxFAR_Comm', 'LC_Comm', 'MaxHt_Comm']]
comm = comm.rename(columns = {'MinFAR_Comm': 'minimum', 'MaxFAR_Comm': 'maximum', 'LC_Comm':'lc', 'MaxHt_Comm':'maxht'})

# ind
ind = f[(f['Indust_Use'] == 'Y')]
ind['generic_land_use_type_id'] = 5
ind['constraint_type'] = 'far'
ind = ind[id_cols + ['MinFAR_Indust', 'MaxFAR_Indust', 'LC_Indust', 'MaxHt_Indust']]
ind = ind.rename(columns = {'MinFAR_Indust': 'minimum', 'MaxFAR_Indust': 'maximum', 'LC_Indust':'lc', 'MaxHt_Indust':'maxht'})

# mixed
mixed = f[(f['Mixed_Use'] == 'Y')]
mixed['generic_land_use_type_id'] = 6
mixed['constraint_type'] = 'far'
mixed = mixed[id_cols + ['MinFAR_Mixed', 'MaxFAR_Mixed', 'LC_Mixed', 'MaxHt_Mixed']]
mixed = mixed.rename(columns = {'MinFAR_Mixed': 'minimum', 'MaxFAR_Mixed': 'maximum', 'LC_Mixed':'lc', 'MaxHt_Mixed':'maxht'})

# mixed du
mixed_du = f[(f['Mixed_Use'] == 'Y')]
mixed_du['generic_land_use_type_id'] = 6
mixed_du['constraint_type'] = 'units_per_acre'
mixed_du = mixed_du[id_cols + ['MinDU_Mixed', 'MaxDU_Mixed', 'LC_Mixed', 'MaxHt_Mixed']]
mixed_du = mixed_du.rename(columns = {'MinDU_Mixed': 'minimum', 'MaxDU_Mixed': 'maximum', 'LC_Mixed':'lc', 'MaxHt_Mixed':'maxht'})

# combine together and add lockouts
lockout_id = 9999
devconstr = pd.concat([sf, mf, off, comm, ind, mixed, mixed_du], sort=False)

## consistency check (ptids)
common = f.merge(devconstr,on=['plan_type_id','plan_type_id'])
not_in_devconstr = f.loc[(~f.plan_type_id.isin(common.plan_type_id)), ['plan_type_id', 'FLU_master_ID', 'Juris_zn']]
print('WARNING: The following ptids are in object f but not devconstr:\n')
print(not_in_devconstr)
not_in_devconstr.to_csv(os.path.join(dir, r'ptid_qc\ptid_consistency_qc_notindevconstr_' + str(date.today()) + '.csv'), index=False)

max_zero_devconstr = devconstr.groupby(["plan_type_id"]).maximum.sum().reset_index()
max_zero = max_zero_devconstr[max_zero_devconstr['maximum'] == 0]
print('The following are non-9*** lockout plan types')
print(max_zero)
max_zero.to_csv(os.path.join(dir, r'ptid_qc\ptid_consistency_qc_maxzero_' + str(date.today()) + '.csv'), index=False)

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
devconstr.to_csv(os.path.join(dir, r'dev_constraints\devconstr_' + str(date.today()) + '.csv'), index=False) 
f.to_csv(os.path.join(dir, r'flu\flu_imputed_ptid_' + str(date.today()) + '.csv'), index=False) # flu imputed kitchen sink file

prcls_flu_ptid = all_df[['PIN', 'plan_type_id', 'tod_id']]
prcls_flu_ptid.to_csv(os.path.join(dir, r'dev_constraints\prcls_ptid_' + str(date.today()) + '.csv'), index=False)
#all_df.to_file(os.path.join(dir, r'shapes\prclpt18_ptid_' + str(date.today()) + '.shp')) # Warning! Takes a long time to write!

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
all_df.loc[all_df['plan_type_id'].isnull(), 'plan_type_id'] = 9999
all_df.loc[all_df['lu_type'] == 23, 'plan_type_id'] = 9001 # Schools/universities
all_df.loc[all_df['lu_type'] == 7, 'plan_type_id'] = 9002 # Government
all_df.loc[all_df['lu_type'] == 9, 'plan_type_id'] = 9003 # Hospitals, convalescent center
all_df.loc[all_df['lu_type'] == 6, 'plan_type_id'] = 9004 # Forest, protected
all_df.loc[all_df['lu_type'] == 5, 'plan_type_id'] = 9005 # Forest, harvestable
all_df.loc[all_df['lu_type'] == 1, 'plan_type_id'] = 9006 # Agriculture
all_df.loc[all_df['lu_type'] == 27, 'plan_type_id'] = 9007 # Vacant undevelopable

# export post-processing lockouts version
prcls_flu_ptid_lockouts = all_df[['PIN', 'plan_type_id', 'tod_id']]
prcls_flu_ptid_lockouts.to_csv(os.path.join(dir, r'dev_constraints\prcls_ptid_v2_' + str(date.today()) + '.csv'), index=False)
devconstr.to_csv(os.path.join(dir, r'dev_constraints\devconstr_v2_' + str(date.today()) + '.csv'), index=False)
