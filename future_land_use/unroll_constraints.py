import os
import pandas as pd
import geopandas as gpd
import numpy as np

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# read in original flu shape
flu_shp_path = r"W:\gis\projects\compplan_zoning\FLU_19_dissolve.shp"
flu_shp = gpd.read_file(flu_shp_path)

# read in imputed data (as shapefile or convert to shape later) 
flu_imp = r"C:\Users\clam\Desktop\urbansim-baseyear-prep\future_land_use\final_flu_imputed_2020-11-20.csv"
f = pd.read_csv(flu_imp)

# assign plan_type_id
f['plan_type_id'] = np.arange(len(f)) + 1

# unroll constraints from plan_type
# sf
sf = f[(f['MaxDU_Res'] < 35.1) & (f['Res_Use'] == 'Y')]
sf['generic_land_use_type_id'] = 1
sf['constraint_type'] = 'units_per_acre'
sf = sf[['plan_type_id', 'generic_land_use_type_id', 'MinDU_Res', 'MaxDU_Res', 'constraint_type']]
sf = sf.rename(columns = {'MinDU_Res': 'minimum', 'MaxDU_Res': 'maximum'})

# mf
mf = f[(f['MaxDU_Res'] > 11.9) & (f['Res_Use'] == 'Y')]
mf['generic_land_use_type_id'] = 2
mf['constraint_type'] = 'units_per_acre'
mf = mf[['plan_type_id', 'generic_land_use_type_id', 'MinDU_Res', 'MaxDU_Res', 'constraint_type']]
mf = mf.rename(columns = {'MinDU_Res': 'minimum', 'MaxDU_Res': 'maximum'})

# off
off = f[(f['Office_Use'] == 'Y')]
off['generic_land_use_type_id'] = 3
off['constraint_type'] = 'far'
off = off[['plan_type_id', 'generic_land_use_type_id', 'MinFAR_Office', 'MaxFAR_Office', 'constraint_type']]
off = off.rename(columns = {'MinFAR_Office': 'minimum', 'MaxFAR_Office': 'maximum'})

# comm
comm = f[(f['Comm_Use'] == 'Y')]
comm['generic_land_use_type_id'] = 4
comm['constraint_type'] = 'far'
comm = comm[['plan_type_id', 'generic_land_use_type_id', 'MinFAR_Indust', 'MaxFAR_Indust', 'constraint_type']]
comm = comm.rename(columns = {'MinFAR_Comm': 'minimum', 'MaxFAR_Comm': 'maximum'})

# ind
ind = f[(f['Indust_Use'] == 'Y')]
ind['generic_land_use_type_id'] = 5
ind['constraint_type'] = 'far'
ind = ind[['plan_type_id', 'generic_land_use_type_id', 'MinFAR_Indust', 'MaxFAR_Indust', 'constraint_type']]
ind = ind.rename(columns = {'MinFAR_Indust': 'minimum', 'MaxFAR_Indust': 'maximum'})

# mixed
mixed = f[(f['Mixed_Use'] == 'Y')]
mixed['generic_land_use_type_id'] = 6
mixed['constraint_type'] = 'far'
mixed = mixed[['plan_type_id', 'generic_land_use_type_id', 'MinFAR_Mixed', 'MaxFAR_Mixed', 'constraint_type']]
mixed = mixed.rename(columns = {'MinFAR_Mixed': 'minimum', 'MaxFAR_Mixed': 'maximum'})

# mixed du
mixed_du = f[(f['Mixed_Use'] == 'Y')]
mixed_du['generic_land_use_type_id'] = 6
mixed_du['constraint_type'] = 'units_per_acre'
mixed_du = mixed_du[['plan_type_id', 'generic_land_use_type_id', 'MinDU_Mixed', 'MaxDU_Mixed', 'constraint_type']]
mixed_du = mixed_du.rename(columns = {'MinDU_Mixed': 'minimum', 'MaxDU_Mixed': 'maximum'})

# combine together and add lockouts
# remove missing data
# replace NA with 0
# add an id column

