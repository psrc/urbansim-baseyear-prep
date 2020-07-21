import os, pyodbc, sqlalchemy
import geopandas as gpd
import pandas as pd
from shapely import wkt

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

city = read_from_sde(connection_string, 'cities')
#city_2 = gpd.read_file(r'W:\geodata\political\city.shp')

parcels = gpd.read_file(r'J:\Projects\2018_base_year\Region\prclpt18.shp') # this shape has 1,302,434 rows
#parcels_gelmer = read_from_sde(connection_string, 'parcels_urbansim_2018_pts') # this shape has 1,302,412 rows and diff columns

prcl_city = gpd.sjoin(parcels, city, how='left')

# QC ---------------------------------------------------------------
prcl_city['city_name'].isnull().sum() # number of nan (uninc areas)
prcl_city['city_name'].notnull().sum() # incorp areas
