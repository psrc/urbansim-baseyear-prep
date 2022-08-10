import os
import pandas as pd
import geopandas as gpd
import numpy as np
from datetime import date

def check_multi_pins(prcls_flu, out_dir):
    # prcls_flu is the spatial join of parcels to the FLU

    # count number of ptids per parcel
    pin_cnt = prcls_flu.groupby(['PIN'])['plan_type_id'].count().reset_index()
    pin_cnt = pin_cnt.rename(columns = {'plan_type_id': 'ptid_count'})

    pins_multi = pin_cnt[pin_cnt['ptid_count'] > 1]

    print(pins_multi)
    print('Number of unique parcel_ids affected by overlapping FLU polygons: ' + str(len(pins_multi)))

    if (len(pins_multi) > 0):
        # export list of parcels that overlay stacked flu polygons
        pins_multi.to_csv(os.path.join(out_dir, r'flu_qc\pins_multi_'+ str(date.today()) +'.csv'), index=False) 
        # export point shapefile of where overlapping zones occur for GIS staff to reconcile
        prcls_multi_ptid = prcls_flu[prcls_flu['PIN'].isin(pins_multi['PIN'].tolist())]
        prcls_multi_ptid = prcls_multi_ptid[['PIN', 'geometry', 'PINFIPS', 'FIPS', 'Jurisdicti', 'Juris_zn', 'Zone_adj', 'plan_type_id']]
        prcls_multi_ptid.to_file(os.path.join(out_dir, r'flu_qc\prcls_multi_ptid_'+ str(date.today()) +'.shp'))
        print('exported list of multi ptid instances as csv and shp to ' + out_dir + 'flu_qc')
