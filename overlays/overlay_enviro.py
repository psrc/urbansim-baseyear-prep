# this version uses a fresh parcel set for each overlay, extracts columns and ultimately joins back to parcels shp
import os, pyodbc, sqlalchemy, time
import geopandas as gpd
import pandas as pd
from shapely import wkt
from datetime import date

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

start = time.time()

append_new_overlay_to_existing_shp = True
export_shp = True
outdir = r'J:\Staff\Christy\usim-baseyear\shapes'
existing_nm = 'prclpt18_overlay_2020-08-13b.shp'
export_nm = 'prclpt18_overlay_' + str(date.today()) + '.shp'

base_year_parcels = r'J:\Projects\2018_base_year\Region\prclpt18.shp' # this shape has 1,302,434 rows

connection_string = 'mssql+pyodbc://AWS-PROD-SQL\Sockeye/ElmerGeo?driver=SQL Server?Trusted_Connection=yes'

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

# gpd.sjoin() creates a one-to-many relationship if an environ layer has overlapping polygons or from 
# a result of buffering, creates overlapping polygons within itself.A solution is to dissolve the shp before joining.

# dict of layer name and either columns to keep or a binary column to create/keep
features_dict = {'cities': ['city_name', 'cnty_name', 'city_fips', 'cnty_fips'],
                 'waterbodies': ['in_wtrbod'], # aka waterfront
                 'floodplains': ['in_fldpln'], 
                 'Open_Space_Parks': ['in_osp'], 
                 'Farmland': ['in_farm'], 
                 'WorkingForests': ['in_wrkfor'], 
                 'Aquatic_Systems': ['in_aqsys'],
                 'rivers': ['in_river'],
                 'steep_slopes': ['grid_code'],
                 'NaturalLands': ['in_natlds'], 
                 'National_Forest_Land_Not_Harvestable': ['in_fornoth']
                 }

# dict of buffer sizes (ft)
buffer_dict = {'waterbodies': 300,
               'floodplains': 100,
               'rivers': 100}

# list of layers to not dissolve
do_not_dissolve = ['cities', 'steep_slopes']

# dict keys as list
features = list(features_dict.keys())
buffers = list(buffer_dict.keys())

print('Reading fresh parcels file')
start_read_prcl_time = time.time()
    
parcels = gpd.read_file(base_year_parcels) # this shape has 1,302,434 rows
    
end_read_prcl_time = time.time()
print(str(round(((end_read_prcl_time-start_read_prcl_time)/60)/60, 2)) + " hours\n")

all_cols_tbl = pd.DataFrame()

# if/else append is true
if append_new_overlay_to_existing_shp == True:
    print('Reading existing overlay parcels output to see what had been overlaid')
    start_read_prcl_time = time.time()

    existing_overlay_parcel_shp = gpd.read_file(os.path.join(outdir, existing_nm)) # existing overlay output on network
    
    end_read_prcl_time = time.time()
    print(str(round(((end_read_prcl_time-start_read_prcl_time)/60)/60, 2)) + " hours\n")

    print('Cross-checking for new features to overlay with')
    prcl_cols = ['PIN', 'PINFIPS', 'FIPS', 'XCOORD', 'YCOORD', 'LATITUDE', 'LONGITUDE', 'geometry']
    feat_cols_in_prcl = [i for i in list(existing_overlay_parcel_shp.columns) if i not in prcl_cols] # columns from prev overlays that exist in parcels
    features_vals_list = [col_name for list_col_names in list(features_dict.values())  for col_name in list_col_names]
    vals_not_in_prcl = [i for i in features_vals_list if i not in feat_cols_in_prcl]
    features = list(set(list(map(get_feature, vals_not_in_prcl)))) # list of features not yet overlaid to parcels
        
    print('Features to overlay with existing overlay parcels output: ' + ', '.join(features) + '\n')
else:
    pass

for i in range(len(features)):
    print('reading ' + features[i])
    cols_to_keep = features_dict[features[i]]
    start_loop_time = time.time()
    join_shp = read_from_sde(connection_string, features[i]) # read feature

    # subset/create column(s)
    if features[i] in do_not_dissolve: 
        join_shp = join_shp[cols_to_keep + ['OBJECTID', 'geometry']] # keep essential columns
    else: 
        new_col_name = cols_to_keep[0]
        print('creating new column: ' + new_col_name)
        join_shp[new_col_name] = 1 # create essential column (binary)
        join_shp = join_shp.dissolve(by=new_col_name).reset_index()
        join_shp = join_shp[[new_col_name, 'geometry', 'OBJECTID']]

    if features[i] in buffers: # check if feature needs buffering
        print(features[i] + ' needs buffer; add buffer')
        join_shp['geometry'] = join_shp.geometry.buffer(buffer_dict[features[i]])

    join_shp = join_shp.drop(['OBJECTID'], axis=1)
    
    print('joining parcels to ' + features[i])
    prcls_joined = gpd.sjoin(parcels, join_shp, how='left') # join parcels to first feature
    prcls_joined = prcls_joined.drop(['index_right'], axis=1) # remove index field
    print('dropped index column')

    tbl = prcls_joined[['PIN'] + cols_to_keep]

    if all_cols_tbl.empty:
        all_cols_tbl = tbl
    else:
        all_cols_tbl = all_cols_tbl.merge(tbl, on='PIN', how='left') 

    end_loop_time = time.time()
    print(str(round(((end_loop_time-start_loop_time)/60)/60, 2)) + " hours")

    print('overlay completed\n')

# join main tbl back to shp
if append_new_overlay_to_existing_shp == True:
    exist_parcels = gpd.read_file(os.path.join(outdir, existing_nm))
    final_parcels = exist_parcels.merge(all_cols_tbl, on='PIN', how='left')
else:
    final_parcels = parcels.merge(all_cols_tbl, on='PIN', how='left') 

# export to shp
if export_shp == True:
    print('Exporting parcels to shapefile')
    final_parcels.to_file(os.path.join(outdir, export_nm)) # write to shapefile
else:
    pass

print('overlays completed')
end = time.time()
print(str(round(((end-start)/60)/60, 2)) + " hours")



