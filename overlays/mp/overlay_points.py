# this version uses a fresh parcel set for each overlay, extracts columns and ultimately joins back to parcels shp
import os, pyodbc, sqlalchemy, time
import geopandas as gpd
import pandas as pd
from shapely import wkt
from datetime import date
import multiprocessing as mp
import gp_parallel
import numpy as np

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

start = time.time()

num_procs = 2

out_file = r'T:\2020September\Stefan\overlay_test\test.csv'

parcels_name = 'parcels_urbansim_2018_pts'
 # this shape has 1,302,434 rows

connection_string = 'mssql+pyodbc://AWS-PROD-SQL\Sockeye/ElmerGeo?driver=SQL Server?Trusted_Connection=yes'

parcels_feature_id = 'parcel_id'

def read_from_sde(connection_string, feature_class_name, crs = {'init' :'epsg:2285'}):
    engine = sqlalchemy.create_engine(connection_string)
    con=engine.connect()
    feature_class_name = feature_class_name + '_evw'
    df=pd.read_sql('select *, Shape.STAsText() as geometry from %s' % (feature_class_name), con=con)
    con.close()
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf=gpd.GeoDataFrame(df, geometry='geometry')
    gdf.crs = crs
    cols = [col for col in gdf.columns if col not in ['Shape', 'GDB_GEOMATTR_DATA', 'SDE_STATE_ID']]
    return gdf[cols]

def get_feature(col_name):
    for k, v in features_dict.items():
        if col_name in v:
            return k


if __name__ == '__main__':
    
    # dict of layer name and either columns to keep or a binary column to create/keep
    features_dict = {'cities': {'col_name' : 'city_name', 'new_col_name' : 'city_name'}, 'taz2010' : {'col_name' : 'taz', 'new_col_name' : 'taz_id'}}

    # dict of buffer sizes (ft)
    buffer_dict = {#'waterbodies': 300,
               'wetlands': 110}

    # dict keys as list
    features = list(features_dict.keys())
    buffers = list(buffer_dict.keys())

    print('Reading fresh parcels file')
    start_read_prcl_time = time.time()
    
    parcels = read_from_sde(connection_string, parcels_name) # this shape has 1,302,434 rows
    #parcels = gpd.read_file(r'R:\e2projects_two\Stefan\urbansim_baseyear\urbansim-baseyear-prep\overlays\test_parcels.shp')
    
    end_read_prcl_time = time.time()
    print(str(round(((end_read_prcl_time-start_read_prcl_time)/60)/60, 2)) + " hours\n")


    results_list = []
    for feature_name, feature_col in features_dict.items():
        print('reading ' + feature_name)
        #cols_to_keep = features_dict[features[i]]
        start_loop_time = time.time()
        join_shp = read_from_sde(connection_string, feature_name) # read feature
        

        if feature_name in buffer_dict.keys(): # check if feature needs buffering
            print(feature_name + ' needs buffer; add buffer')
            join_shp['geometry'] = join_shp.geometry.buffer(buffer_dict[feature_name])

    
        print('joining parcels to ' + feature_name)

        ##### multiprocessing
        df_chunks = np.array_split(parcels, num_procs)
        pool = mp.Pool(num_procs, gp_parallel.init_pool, [join_shp])
        results = pool.map(gp_parallel.spatial_join, df_chunks)
        results = pd.concat(results)
        pool.close()

        col_name = feature_col['col_name']
        new_col_name = feature_col['new_col_name']
        results = results.groupby([parcels_feature_id], as_index = False).agg({col_name: min})
        results = results.rename(columns = {col_name : new_col_name})
        results_list.append(results)
        end_loop_time = time.time()
        print(str(round(((end_loop_time-start_loop_time)/60)/60, 2)) + " hours")

        print('overlay completed\n')

    # join main tbl back to shp
    dfs = [df.set_index(parcels_feature_id) for df in results_list]
    df = pd.concat(dfs, axis=1)
    df.to_csv(outfile)
    print('overlays completed')
    end = time.time()
    print(str(round(((end-start)/60)/60, 2)) + " hours")



