# this version uses a fresh parcel set for each overlay, extracts columns and ultimately joins back to parcels shp
import os, pyodbc, sqlalchemy, time
import geopandas as gpd
import pandas as pd
from shapely import wkt
from datetime import date
import multiprocessing as mp
import numpy as np

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

start = time.time()

###### script paramaeters:

# this script uses multiprocessing to speed up overlay process
# set to 4 when running on surface laptop
num_procs = 4

# .csv output
outfile = r'T:\2020December\Stefan\test2.csv'

# shapefile output (causes errors sometimes)
out_shape_file = r'T:\2020December\Stefan\test_shape.shp'

# ElmerGeo parcels file name 
parcels_name = 'parcels_urbansim_2018_pts'

# ElmerGeo connection string
connection_string = 'mssql+pyodbc://AWS-PROD-SQL\Sockeye/ElmerGeo?driver=SQL Server?Trusted_Connection=yes'

# parcels unique id
parcels_feature_id = 'parcel_id'

# dict of layer name and either columns to keep or a binary column to create/keep
# if is_boolean = True, a new column will be created named by the col names in col_name/new_col_name
# if is boolean = False, the spatial join will use the column name specified in col_name and then rename it
# using the new_col_name. 
features_dict = {'cities': {'col_name' : 'city_name', 'new_col_name' : 'city_name', 'is_boolean' : False}, 
                 'taz2010' : {'col_name' : 'taz', 'new_col_name' : 'taz_id', 'is_boolean' : False},
                 'tract2010' : {'col_name' : 'geoid10', 'new_col_name' : 'tract_geoid10', 'is_boolean' : False},
                 'hct_block' : {'col_name' : 'geoid10', 'new_col_name' : 'hct_block_geoid10', 'is_boolean' : False},
                 'local_parks' : {'col_name' : 'local_parks', 'new_col_name' : 'local_parks', 'is_boolean' : True},
                 'lock_parks_quartermile_buffer' : {'col_name' : 'lpqmi', 'new_col_name' : 'lpqmi', 'is_boolean' : True},
                 'tod_prcl_uga' : {'col_name' : 'tod_area5', 'new_col_name' : 'tod_area5', 'is_boolean' : False},
                 'transit_buffer' : {'col_name' : 'trans_buff', 'new_col_name' : 'trans_buff', 'is_boolean' : False},
                 'uga_quarter_buffer_no_water' : {'col_name' : 'uga_qb_nw', 'new_col_name' : 'uga_qb_nw', 'is_boolean' : True},
                 'urban_growth_area' : {'col_name' : 'uga', 'new_col_name' : 'uga', 'is_boolean' : True},
                 'zip_codes' : {'col_name' : 'zip', 'new_col_name' : 'zip', 'is_boolean' : False},
                 'regional_geographies' : {'col_name' : 'rgeo_class', 'new_col_name' : 'rgeo_class', 'is_boolean' : False},
                 'subareas' : {'col_name' : 'subarea', 'new_col_name' : 'subarea', 'is_boolean' : False},
                 'urban_centers' : {'col_name' : 'name', 'new_col_name' : 'name', 'is_boolean' : False}}


def read_from_sde(connection_string, feature_class_name,
                  crs={'init': 'epsg:2285'}):
    """
    Returns the specified feature class as a geodataframe from ElmerGeo.
    
    Parameters
    ----------
    connection_string : SQL connection string that is read by geopandas 
                        read_sql function
    
    feature_class_name: the name of the featureclass in PSRC's ElmerGeo 
                        Geodatabase
    
    cs: cordinate system
    """

    engine = sqlalchemy.create_engine(connection_string)
    con=engine.connect()
    feature_class_name = feature_class_name + '_evw'
    df=pd.read_sql('select *, Shape.STAsText() as geometry from %s' % 
                   (feature_class_name), con=con)
    con.close()
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf=gpd.GeoDataFrame(df, geometry='geometry')
    gdf.crs = crs
    cols = [col for col in gdf.columns if col not in 
            ['Shape', 'GDB_GEOMATTR_DATA', 'SDE_STATE_ID']]
    
    return gdf[cols]



def spatial_join(df):
    prcls_joined = gpd.sjoin(df, global_overlay, how='left', op='intersects') 
    return prcls_joined

def intersect_overlay(df):
    prcls_joined = gpd.overlay(df, global_overlay, how='intersection')
    prcls_joined['new_area'] = prcls_joined['geometry'].area
    prcls_joined = prcls_joined.groupby(feature_id)['new_area'].sum()
    return prcls_joined

def erase_overlay(df):
    prcls_joined = gpd.overlay(df, global_overlay, how='difference')
    #prcls_joined = prcls_joined.groupby(feature_id)['new_area'].sum()
    return prcls_joined

def init_pool(overlay):
    global global_overlay
    global_overlay = overlay


if __name__ == '__main__':
    
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
        
        # see if parcel_id is already in the fc:
        if parcels_feature_id in join_shp.columns:
            del join_shp[parcels_feature_id]

        if feature_name in buffer_dict.keys(): # check if feature needs buffering
            print(feature_name + ' needs buffer; add buffer')
            join_shp['geometry'] = join_shp.geometry.buffer(buffer_dict[feature_name])
        if feature_col['is_boolean']:
            print ('adding boolean field')
            join_shp[feature_col['new_col_name']] = 1
    
        print('joining parcels to ' + feature_name)

        ##### multiprocessing
        # split parcels layer into chunks determined by the number of processors
        df_chunks = np.array_split(parcels, num_procs)
        pool = mp.Pool(num_procs, init_pool, [join_shp])
        results = pool.map(spatial_join, df_chunks)
        # concat the results from each process
        results = pd.concat(results)
        pool.close()

        col_name = feature_col['col_name']
        new_col_name = feature_col['new_col_name']
        print (results.columns)
        results = results.groupby([parcels_feature_id], as_index = False).agg({col_name: min})
        results = results.rename(columns = {col_name : new_col_name})
        results_list.append(results)
        end_loop_time = time.time()
        print(str(round(((end_loop_time-start_loop_time)/60)/60, 2)) + " hours")

        print('overlay completed\n')

    # join main tbl back to shp
    dfs = [df.set_index(parcels_feature_id) for df in results_list]
    df = pd.concat(dfs, axis=1)
    df.fillna(0, inplace = True)
    
    # wrtie to csv
    df.to_csv(outfile)

    # join to parcels and write out
    parcels = parcels.merge(df, how = 'left', left_on = parcels_feature_id, right_on = parcels_feature_id)
    parcels.to_file(out_shape_file)
    print('overlays completed')
    end = time.time()
    print(str(round(((end-start)/60)/60, 2)) + " hours")



