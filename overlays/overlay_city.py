import os, pyodbc, sqlalchemy, time
import geopandas as gpd
import pandas as pd
from shapely import wkt

start = time.time()

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

#outdir = r'J:\Staff\Christy\usim-baseyear\shapes'

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

parcels = gpd.read_file(r'J:\Projects\2018_base_year\Region\prclpt18.shp') # this shape has 1,302,434 rows

# dict of layer name and columns to exclude
features_dict = {'cities': ['OBJECTID', 'acres_gis', 'feat_type'],
                 'floodplains': ['OBJECTID', 'county', 'Shape_Leng']}
buffer_dict = {'floodplains': 100}

# dict keys as list
features = list(features_dict.keys())
buffers = list(buffer_dict.keys())

for i in range(len(features)):
    print('reading ' + features[i])
    cols_to_rm = features_dict[features[i]]
    join_shp = read_from_sde(connection_string, features[i]) # read feature
    # check if feature needs buffering
    if features[i] in buffers:
        print(features[i] + ' needs buffer; add buffer')
        join_shp['geometry'] = join_shp.geometry.buffer(buffer_dict[features[i]])
    # remove unecessary columns
    join_shp = join_shp.drop(cols_to_rm, axis=1) if len(cols_to_rm) > 0 else join_shp
    if i == 0:
        print('joining parcels to ' + features[i])
        prcls_joined = gpd.sjoin(parcels, join_shp, how='left') # join parcels to first feature
    else:
        print('joining joined parcels to ' + features[i])
        prcls_joined = prcls_joined.drop(['index_right'], axis=1) # remove index field
        print('dropped index column')
        prcls_joined = gpd.sjoin(prcls_joined, join_shp, how='left') # join subsequent parcel output to feature
        print('overlays completed')

end = time.time()
print(str(((end-start)/60)/60) + " hours")


