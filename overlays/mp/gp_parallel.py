import pandas as pd
import multiprocessing as mp
import numpy as np
import geopandas as gpd

def spatial_join(df):
    prcls_joined = gpd.sjoin(df, global_overlay, how='left') 
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
