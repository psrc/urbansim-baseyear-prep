import os
import pandas as pd
import geopandas as gpd
import numpy as np

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# read in original flu shape
flu_shp_path = r"W:\gis\projects\compplan_zoning\FLU_19_dissolve.shp"
flu_shp = gpd.read_file(flu_shp_path) # 1894 rows

# read in imputed data 
flu_imp = r"C:\Users\clam\Desktop\urbansim-baseyear-prep\future_land_use\final_flu_imputed_2020-12-02.csv"
#flu_imp = r"J:\Staff\Christy\usim-baseyear\flu\final_flu_imputed_2020-12-02.csv"
f = pd.read_csv(flu_imp) # 1697 rows

# clean up f; remove extra/unecessary fields before join
f_col_keep = [col for col in f.columns if col not in ['Jurisdicti', 'Key', 'Zone_adj', 'Definition'] + list(f.columns[f.columns.str.endswith("src")])]
f = f[f_col_keep]

# assign plan_type_id
f['plan_type_id'] = np.arange(len(f)) + 1

# join imputed data back to FLU shapefile
flu = flu_shp.merge(f, on = ['Juris_zn'], how = 'left') # 1894 rows

# spatial join parcels to flu to assign plan_type_id----------------------------------------------------
# read parcels file (Stefan's output parcel's file or a clean file?)
base_year_prcl_path = r"J:\Projects\2018_base_year\Region\prclpt18.shp"
prcls = gpd.read_file(base_year_prcl_path)

prcls_flu = prcls.sjoin(flu)
prcls_flu.to_file(r"J:\Staff\Christy\usim-baseyear\shapes\prclpt18_ptid_"+ str(date.today()) + '.shp')

# create development constraints table------------------------------------------------------------------
# unroll constraints from plan_type
id_cols = ['plan_type_id', 'generic_land_use_type_id', 'constraint_type']

# sf
sf = f[(f['MaxDU_Res'] < 35.1) & (f['Res_Use'] == 'Y')]
sf['generic_land_use_type_id'] = 1
sf['constraint_type'] = 'units_per_acre'
sf = sf[id_cols + ['MinDU_Res', 'MaxDU_Res']]
sf = sf.rename(columns = {'MinDU_Res': 'minimum', 'MaxDU_Res': 'maximum'})

# mf
mf = f[(f['MaxDU_Res'] > 11.9) & (f['Res_Use'] == 'Y')]
mf['generic_land_use_type_id'] = 2
mf['constraint_type'] = 'units_per_acre'
mf = mf[id_cols + ['MinDU_Res', 'MaxDU_Res',]]
mf = mf.rename(columns = {'MinDU_Res': 'minimum', 'MaxDU_Res': 'maximum'})

# off
off = f[(f['Office_Use'] == 'Y')]
off['generic_land_use_type_id'] = 3
off['constraint_type'] = 'far'
off = off[id_cols + ['MinFAR_Office', 'MaxFAR_Office']]
off = off.rename(columns = {'MinFAR_Office': 'minimum', 'MaxFAR_Office': 'maximum'})

# comm
comm = f[(f['Comm_Use'] == 'Y')]
comm['generic_land_use_type_id'] = 4
comm['constraint_type'] = 'far'
comm = comm[id_cols + ['MinFAR_Comm', 'MaxFAR_Comm']]
comm = comm.rename(columns = {'MinFAR_Comm': 'minimum', 'MaxFAR_Comm': 'maximum'})

# ind
ind = f[(f['Indust_Use'] == 'Y')]
ind['generic_land_use_type_id'] = 5
ind['constraint_type'] = 'far'
ind = ind[id_cols + ['MinFAR_Indust', 'MaxFAR_Indust']]
ind = ind.rename(columns = {'MinFAR_Indust': 'minimum', 'MaxFAR_Indust': 'maximum'})

# mixed
mixed = f[(f['Mixed_Use'] == 'Y')]
mixed['generic_land_use_type_id'] = 6
mixed['constraint_type'] = 'far'
mixed = mixed[id_cols + ['MinFAR_Mixed', 'MaxFAR_Mixed']]
mixed = mixed.rename(columns = {'MinFAR_Mixed': 'minimum', 'MaxFAR_Mixed': 'maximum'})

# mixed du
mixed_du = f[(f['Mixed_Use'] == 'Y')]
mixed_du['generic_land_use_type_id'] = 6
mixed_du['constraint_type'] = 'units_per_acre'
mixed_du = mixed_du[id_cols + ['MinDU_Mixed', 'MaxDU_Mixed']]
mixed_du = mixed_du.rename(columns = {'MinDU_Mixed': 'minimum', 'MaxDU_Mixed': 'maximum'})

# combine together and add lockouts
lockout_id = 9999
devconstr = pd.concat([sf, mf, off, comm, ind, mixed, mixed_du], sort=False)

# create df of plan_type_id 9999
lockout_df = pd.DataFrame({'plan_type_id': np.repeat(lockout_id, 7),
              'generic_land_use_type_id': list(np.arange(1, 7)) + [6],
              'minimum': 0,
              'maximum': 0,
              'constraint_type': list(np.repeat("units_per_acre", 2)) + list(np.repeat("far", 4)) + ["units_per_acre"]})

devconstr = pd.concat([devconstr, lockout_df], sort=False)

# remove missing data
devconstr = devconstr[(devconstr['minimum'].notnull()) | (devconstr['maximum'].notnull())]

# replace NA with 0
devconstr.loc[devconstr['minimum'].isnull(), 'minimum'] = 0
devconstr.loc[devconstr['maximum'].isnull(), 'maximum'] = 0

# add an id column
devconstr['development_constraint_id']= np.arange(len(devconstr)) + 1
