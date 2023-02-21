import math
import os
import sys

import geojson
import geopandas as gpd
import pandas as pd
from geopandas import GeoDataFrame
from halo import Halo
from progress.bar import IncrementalBar
from rtree import index
from scipy.spatial import cKDTree
from shapely.geometry import LineString, Point, Polygon, shape
from shapely.wkt import dumps, loads

from helper import user_interface_helper
from validation import validation

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))


class ParkingService:
    """
    This class provides functions to assign the bubbles to the parking spaces.
    """

    def __init__(self, csv_file: str, json_file: str) -> None:
        self.bubbles_df: pd.DataFrame = self.init_csv_df(csv_file)
        self.parking_spaces_gdf: gpd.GeoDataFrame = self.init_gdf_df(json_file)
        self.tree: cKDTree = cKDTree(self.parking_spaces_gdf.geometry.apply(lambda p: (
            self.turn_any_geometry_into_point(p).x, self.turn_any_geometry_into_point(p).y)).tolist())
        self.threshold_max_stations: int = 10
        self.ignore_list: dict = {}
        self.point_list: dict = {}
        self.bar = IncrementalBar(
            'Bubbles Processed', max=len(self.bubbles_df.index))

    def init_csv_df(self, csv_file: str) -> pd.DataFrame:
        """
        This function reads the csv file with the filtered bubbles 
        and converts the geometries from the WKT format into shapely objects. 
        Args:
            csv_file (str): The path to the csv file containing the bubbles

        Returns:
            pd.DataFrame: The processed dataframe
        """
        csv_df = pd.read_csv(csv_file)
        csv_df['hull'] = csv_df['hull'].apply(lambda x: loads(x))
        csv_df = GeoDataFrame(csv_df, geometry="hull")
        csv_df = csv_df.set_crs("EPSG:4326")
        return csv_df

    def init_gdf_df(self, json_file: str) -> gpd.GeoDataFrame:
        """
        This function reads the geojson file with the filtered parking spaces. 
        To be able to handle the coordinates of the parking spaces 
        better later on, they are converted into shapely points.

        Args:
            json_file (str): The path to the geojson file containing the parking spaces

        Returns:
            gpd.GeoDataFrame: The processed dataframe
        """
        geojson_gdf = gpd.read_file(json_file)
        geojson_gdf['geometry'] = geojson_gdf['geometry'].apply(
            lambda x: shape(x))
        geojson_gdf['geometry'] = geojson_gdf['geometry'].set_crs("EPSG:4326")
        return geojson_gdf

    def find_nearest_point_ckdtree(self, geometry: Point | LineString | Polygon) -> Point:
        """
        Given a Point, LineString, or Polygon geometry object, find the nearest point to that geometry object.
        The geometry represents a bubble while the nearest point is the nearest parking spot.
        The nearest point is only accepted if it was not found already x times. Where x is the number
        of the maximal accepted amount of chargers at a parking space.
        To make this search more efficient a cKDTree is used.

        Args:
        - geometry: A Point, LineString, or Polygon geometry object representing bubble for which to find the
        nearest point.

        Returns:
        - A Point object representing the nearest parking space
        """
        point = self.turn_any_geometry_into_point(geometry)
        start_idx = 2
        while True:
            dist, idx = self.tree.query((point.x, point.y), k=start_idx)
            nearest_point = self.parking_spaces_gdf.loc[idx,
                                                        'geometry'].iloc[start_idx-2]
            point_tuple = (nearest_point.x, nearest_point.y)
            if point_tuple not in self.ignore_list:
                break
            start_idx += 1
        self.point_list[point_tuple] = self.point_list.get(point_tuple, 0) + 1
        if self.point_list[point_tuple] == self.threshold_max_stations:
            self.ignore_list[point_tuple] = True
        self.bar.next()
        return nearest_point

    def turn_any_geometry_into_point(self, geometry: Point | LineString | Polygon) -> Point:
        """
        Given a Point, LineString, or Polygon geometry object, this method converts it into a Point object. 
        If the input is already a Point object, it is simply returned. 
        If the input is a LineString, the representative point is returned. 
        If the input is a Polygon, the centroid of the polygon is returned.

        Args:
        - geometry: A Shapely Point, LineString, or Polygon geometry object.

        Returns:
        - A Point object representing the input geometry object.
        """
        if geometry.geom_type == 'Polygon':
            return geometry.representative_point()
        elif geometry.geom_type == 'LineString':
            return geometry.interpolate(geometry.length / 2)
        elif geometry.geom_type == 'Point':
            return geometry

    def unify_charging_points(self) -> None:
        """
        The method saves a file containing all found parking spaces with 
        their respective index of how many time 
        they where chosen as the nearest point
        """
        self.bubbles_df['nearest_point'] = self.bubbles_df['nearest_point'].apply(
            lambda x: dumps(x))
        unique_values = self.bubbles_df['nearest_point'].unique()
        value_counts = self.bubbles_df['nearest_point'].value_counts(
            normalize=False)
        result = pd.Series(data=value_counts,
                           index=unique_values, name='Count')
        result.to_csv('./datasets/generated/charging_points.csv')

    def user_input_max_station(self) -> None:
        """
        Prompts the user to input a maximum number of charging stations that can be built on a parking lot
        and sets the value as the threshold_max_stations attribute of the class instance.
        If the input is invalid, the function continues to prompt the user until a valid input is received.
        """
        min_value = math.ceil(len(self.bubbles_df) /
                              len(self.parking_spaces_gdf))

        number = user_interface_helper.fancy_input_number(
            ["  Choose The Maximum Number Of Charging Stations A Parking Space Should Have.",
             "  Note: Inputting A Lower Number Will Increase The Search Time"],
            "There are not enough parking spaces to calculate this.",
            min_value)

        self.threshold_max_stations = number

    def run(self) -> None:
        """
        Runs the main algorithm to find the nearest parking space for each bubble
        and writes the results to a new CSV file. 
        """
        self.user_input_max_station()
        self.bubbles_df['nearest_point'] = self.bubbles_df['hull'].apply(
            lambda x: self.find_nearest_point_ckdtree(x))
        self.bar.finish()
        spinner = Halo("Loading")
        spinner.start(text="Saving Charging Points")
        self.unify_charging_points()
        spinner.succeed()


def filter_parking_spaces() -> None:
    """
    Filters a GeoJSON file of parking spaces based on whether they have 
    certain access properties, and writes the
    filtered results to a new GeoJSON file.
    """
    with open("./datasets/export.geojson", "r", encoding="latin_1") as f:
        data = geojson.load(f)

    property_name = "access"
    property_values = ["yes", "electric_vehicle", "customers"]

    filtered_features = []
    for feature in data["features"]:
        for property_value in property_values:
            if property_name in feature["properties"] and feature["properties"][property_name] == property_value:
                filtered_features.append(feature)
    filtered_data = geojson.FeatureCollection(filtered_features)

    with open("./datasets/generated/filtered_parking_spaces.geojson", "w") as f:
        geojson.dump(filtered_data, f)


def remove_bubbles_with_charging_stations(algorithm: int) -> None:
    """
    This function saves a CSV file containing all 
    bubbles that do not yet have a charging station in them. 
    The already existing charging stations are taken 
    from the charging station register. 

    Args:
    - algorithm (int):  Tells about if simple_split or KMeans was used.\n
                        if value is 1 -> simple_split \n
                        if value is 2 -> KMeans
    """
    if algorithm == 1:
        bubbles_df = pd.read_csv('./datasets/generated/hulls_split.csv')
    else:
        bubbles_df = pd.read_csv('./datasets/generated/hulls_batched.csv')
    bubbles_df['hull'] = bubbles_df['hull'].apply(lambda x: loads(x))

    charging_stations_df = pd.read_csv(
        './datasets/Ladesaeulenregister.csv', sep=";", skiprows=10, encoding="latin_1", usecols=['Breitengrad', "Längengrad"])

    charging_stations_df.columns = ['Latitude', 'Longitude']
    charging_stations_df['Latitude'] = charging_stations_df['Latitude'].replace(
        ',', '.', regex=True)
    charging_stations_df['Longitude'] = charging_stations_df['Longitude'].replace(
        ',', '.', regex=True)
    charging_stations_df['Latitude'] = charging_stations_df['Latitude'].replace(
        '[^0-9\.]', '', regex=True)
    charging_stations_df['Longitude'] = charging_stations_df['Longitude'].replace(
        '[^0-9\.]', '', regex=True)
    charging_stations_df = charging_stations_df.astype(float)

    idx = index.Index()
    for i, row in bubbles_df.iterrows():
        shape = row['hull']
        bbox = shape.bounds
        idx.insert(i, bbox)

    to_remove = set()
    for i, row in charging_stations_df.iterrows():
        point = Point(row['Longitude'], row['Latitude'])
        for j in idx.intersection(point.coords[0]):
            shape = bubbles_df.iloc[j]['hull']
            if shape.intersects(point):
                to_remove.add(j)

    bubbles_df = bubbles_df.drop(list(to_remove))
    bubbles_df.to_csv('./datasets/generated/filtered_bubbles.csv', index=False)


def run_parking_search(algorithm: int) -> None:
    """
    method performs all necessary steps to start the search for a suitable parking space

    Args:
        - algorithm (int):  Tells about if simple_split or KMeans was used.\n
                            if value is 1 -> simple_split \n
                            if value is 2 -> KMeans
    """

    spinner = Halo("Loading")
    spinner.start(text="Applying Filters To Parking Spaces")
    if not validation.check_generated("filtered_parking_spaces.geojson"):
        filter_parking_spaces()
    spinner.succeed()
    spinner.start(
        text="Filtering All Bubbles That Already Have A Charging Station")
    remove_bubbles_with_charging_stations(algorithm)
    spinner.succeed()
    s = ParkingService('./datasets/generated/filtered_bubbles.csv',
                       './datasets/generated/filtered_parking_spaces.geojson')
    s.run()
