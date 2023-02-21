import os
import sys

import folium
import pandas as pd
from folium.plugins import MarkerCluster
from halo import Halo
from matplotlib.colors import LinearSegmentedColormap
from shapely.wkt import loads

from validation import validation

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))


class Visualization:
    """
    A class to generate a visualization of charging points and their coverage.

    Attributes
    ----------
    MAPS_PATH : str
        The path to the directory where the map will be saved.
    map : folium.folium.Map
        A map object that will be visualized.

    Methods
    -------
    charging_points()
        Generates a marker cluster on the map object that displays the charging points and their coverage.
    bubbles(path: str)
        Generates bubble shapes on the map object based on the data file specified by `path`.
    save()
        Saves the map object as an HTML file to the specified `MAPS_PATH` directory.
    run()
        Runs the visualization process by calling the `charging_points`, `bubbles`, and `save` methods.
    """

    def __init__(self) -> None:
        self.MAPS_PATH = "./maps"
        self.map = folium.Map(
            # location of the middle of germany
            location=[51.165691, 10.451526],
            zoom_start=6,
        )

    def charging_points(self) -> None:
        df = pd.read_csv('./datasets/generated/charging_points.csv')

        df['Longitude'] = df['Unnamed: 0'].apply(lambda x: loads(x).x)
        df['Latitude'] = df['Unnamed: 0'].apply(lambda x: loads(x).y)

        marker_cluster = MarkerCluster(
            name="  Charging Points").add_to(self.map)
        colors = [(1, 0, 0), (1, 1, 0), (0, 1, 0)]  # red to yellow to green
        colormap = LinearSegmentedColormap.from_list('custom', colors, N=256)
        for index, row in df.iterrows():
            count = row['Count']
            folium.Circle(
                location=[row['Latitude'], row['Longitude']],
                radius=25,
                popup=f"# Charging_Stations: {row['Count']}",

                color=colormap(count/df['Count'].max()),
                fill=True,
                fill_color=colormap(count/df['Count'].max())).add_to(marker_cluster)

    def bubbles(self, path: str) -> None:
        df = pd.read_csv(f"./datasets/generated/{path}")
        shapes = df["hull"].apply(lambda x: loads(x))
        name = 'Bubbles Simple Split'
        if path == 'hulls_batched.csv':
            name = 'Bubbles KMeans'

        def style_function(x): return {
            'color': '#0000aa' if name == 'Bubbles KMeans' else '#00aa00'}
        shape_group = folium.FeatureGroup(
            name=name, show=False)
        for shape in shapes:
            folium.GeoJson(shape, style_function=style_function).add_to(
                shape_group)
        shape_group.add_to(self.map)

    def save(self) -> None:
        folium.LayerControl().add_to(self.map)
        self.map.save(f'{self.MAPS_PATH}/visualization.html')

    def run(self) -> None:
        spinner = Halo("Loading")
        spinner.start("Visualizing The Charging Points")
        self.charging_points()
        spinner.succeed()
        spinner.start("Visualizing The Simple Split Bubbles")
        if validation.check_generated('hulls_split.csv'):
            self.bubbles('hulls_split.csv')
            spinner.succeed()
        else:
            spinner.fail
        spinner.start("Visualizing The KMeans Bubbles")
        if validation.check_generated('hulls_batched.csv'):
            self.bubbles('hulls_batched.csv')
            spinner.succeed()
        else:
            spinner.fail()
        spinner.start(
            f"Saving The Visualization Under {self.MAPS_PATH}/visualization.html")
        self.save()
        spinner.succeed()


def run_visualization():
    """
    Runs the visualization process by creating a Visualization object and calling its `run` method.
    """
    vis = Visualization()
    vis.run()
