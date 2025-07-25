import transit_service_analyst as tsa
from pathlib import Path
import pandas as pd
import geopandas as gpd
from dataclasses import dataclass
import configuration
import yaml


@dataclass
class transit_data_frames:
    """A class to hold transit data frames."""

    year: str
    routes: gpd.GeoDataFrame
    route_stops: gpd.GeoDataFrame
    stops: gpd.GeoDataFrame


def get_service(transit_dfs: transit_data_frames) -> gpd.GeoDataFrame:
    """
    Get the future 1491 service from the transit data frames.
    """
    # BRT
    route_list = []
    route_stop_list = []
    stop_list = []

    # street car, light rail, commuter rail
    route_types = [0, 1, 2, 5]
    routes = transit_dfs.routes[transit_dfs.routes["route_type"].isin(route_types)]
    route_stops = transit_dfs.route_stops[
        transit_dfs.route_stops["rep_trip_id"].isin(
            routes["rep_trip_id"].to_list()
            )]
    stops = transit_dfs.stops[
                transit_dfs.stops["stop_id"].isin(route_stops["stop_id"].to_list())
            ]
    #stops = add_columns_to_stops(stops, route_type, transit_dfs.year)
    route_list.append(routes)
    route_stop_list.append(route_stops)
    stop_list.append(stops)
    
    routes = gpd.GeoDataFrame(pd.concat(route_list))
    routes.crs = 4326
    routes = routes.to_crs(2285)

    route_stops = gpd.GeoDataFrame(pd.concat(route_stop_list))
    route_stops.crs = 4326
    route_stops = route_stops.to_crs(2285)

    stops = pd.concat(stop_list, ignore_index=True)
    stops.crs = 4326
    stops = stops.to_crs(2285)

    stops = pd.concat(stop_list, ignore_index=True)
    stops.crs = 4326
    stops = stops.to_crs(2285)

    return transit_data_frames(
        year=transit_dfs.year, routes=routes, route_stops=route_stops, stops=stops
    )
    

def add_columns_to_stops(
    stops: gpd.GeoDataFrame, stop_type: int, year: int
) -> pd.DataFrame:
    """
    Add a column to the stops GeoDataFrame with a constant value.
    """
    stops["stop_type"] = stop_type
    # stops['buffer_size'] = buffer_size
    stops["year"] = year
    return stops


def get_transit_gdfs(gtfs_dir: Path, date: str) -> tuple:
    """
    Get the transit GeoDataFrames for a given service definition.
    """
    tsa_instance = tsa.load_gtfs(gtfs_dir, date)
    # get the routes
    dfs = transit_data_frames(
        year=date[:4],
        routes=tsa_instance.get_lines_gdf(),
        route_stops=tsa_instance.get_line_stops_gdf(),
        stops=gpd.GeoDataFrame(
            tsa_instance.stops,
            geometry=gpd.points_from_xy(
                tsa_instance.stops["stop_lon"], tsa_instance.stops["stop_lat"]
            ),
        ),
    )
   
    # get the routes
    dfs = get_service(dfs)
    return dfs

file = Path().joinpath(configuration.args.configs_dir, "config.yaml")

config = yaml.safe_load(open(file))
store = pd.HDFStore(config["h5_cache_dir"], mode="r")
parcels = store["parcels"]
parcels.reset_index(inplace=True)
parcels_gdf = gpd.GeoDataFrame(
        parcels, geometry=gpd.points_from_xy(parcels.x_coord_sp, parcels.y_coord_sp)
    )
parcels_gdf.crs = 2285

dfs = get_transit_gdfs(
Path(config["gtfs_dir"]) / "2050", "20250408")

dfs.stops.to_file(
        Path(config["output_gdb"]), driver="OpenFileGDB", layer="stations_1110"
    )
dfs.route_stops.to_file(
        config["output_gdb"], driver="OpenFileGDB", layer="route_stops_1110"

)
dfs.routes.to_file(
        config["output_gdb"], driver="OpenFileGDB", layer="route_1110"
    )

dfs.stops["geometry"] = dfs.stops.geometry.buffer(1320)
dfs.stops = dfs.stops.to_crs(2285)
dfs.stops.to_file(
        Path(config["output_gdb"]), driver="OpenFileGDB", layer="station_buffers_1110"
    )
dfs.stops['is_stop'] = 1
test = parcels_gdf.sjoin(dfs.stops, how='left')
test['is_stop'].fillna(0, inplace=True)
test = test.groupby('parcel_id')['is_stop'].first().reset_index()
parcels_gdf = parcels_gdf.merge(test, on='parcel_id')
df = parcels_gdf[['parcel_id', 'is_stop']]
df.rename(columns={'is_stop' : 'hb_1110'}, inplace=True)
df.to_csv(Path(config["output_dir"])/'hb_1110_parcels.csv')


print("done")
