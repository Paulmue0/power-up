import csv

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shapely.wkt
from halo import Halo
from scipy.spatial import ConvexHull
from shapely import wkt
from shapely.geometry import LineString, Point, Polygon


def add_bubble(split: pd.DataFrame, bubbles: dict) -> dict:
    """
    This function decides whether the points in the split form a polygon, 
    a line or a single point. The resulting shape is then added to 
    the previous bubbles. 
    Finally the bubbles are returned

    Args:
        split (pd.DataFrame): A collection of Shapely Points
        bubbles (dict): All yet found bubbles

    Returns:
        dict: The bubbles incremented by the shape resulting out of the split
    """
    data = split[['y_mp_100m', 'x_mp_100m', 'EV']].values
    if len(split) > 2:
        hull = ConvexHull(data[:, :2])
        bubbles["polygons"].append(Polygon(data[hull.vertices, :2]))
    elif len(split) == 2:
        line = LineString(data)
        bubbles["lines"].append(line)
    elif len(split) == 1:
        point = Point(data[0])
        bubbles["points"].append(point)
    return bubbles


def sort(split: pd.DataFrame, sort_towards_x: bool) -> pd.DataFrame:
    """
    This function sorts the passed split either by the x or the y values of its contained points. 

    Args:
        split (pd.DataFrame): A collection of Shapely Points
        sort_towards_x (bool):  True for sorting towards x,
                                False for sorting towards y

    Returns:
        pd.DataFrame: the sorted split
    """
    if sort_towards_x:
        return split.sort_values(by='x_mp_100m')
    return split.sort_values(by='y_mp_100m')


def split_area(split: pd.DataFrame,
               #    sort_towards_x: bool = True,
               bubbles: dict = {"polygons": [], "lines": [], "points": [], }) -> dict:
    """
    This function recursively divides the given area in half until there 
    are less than 16 electric cars in two halves or the area cannot be divided further. 
    The method decides whether to divide in x or y direction 
    based on the distance of the x or y extreme points. 
    If a sub-area cannot be further divided, then it is returned as a "bubble".

    Args:
        split (pd.DataFrame): A collection of Shapely Points
        sort_towards_x (bool, optional): The direction towards 
                                         the split will be sorted. Defaults to True.
        bubbles (_type_, optional): All already found bubbles. Defaults to {"polygons": [], "lines": [], "points": [], }.

    Returns:
        dict: All found bubbles
    """
    if len(split) == 1:
        bubbles = add_bubble(split, bubbles)
    else:
        lon_diff = split['x_mp_100m'].max() - split['x_mp_100m'].min()
        lat_diff = split['y_mp_100m'].max() - split['y_mp_100m'].min()
        if lon_diff > lat_diff:
            split = sort(split, True)
        else:
            split = sort(split, False)
        first, second = np.array_split(split, 2)
        if first['EV'].sum() < 8 and second['EV'].sum() < 8:
            bubbles = add_bubble(split, bubbles)
        else:
            split_area(first, bubbles)
            split_area(second, bubbles)
    return bubbles


def save_bubbles(
        bubbles: dict = {"polygons": [], "lines": [], "points": [], }) -> None:
    """
    The method saves all found bubbles in WKT format to a CSV file
    Args:
        bubbles (_type_, optional): Bubbles to save. Defaults to {"polygons": [], "lines": [], "points": [], }.
    """
    wkt_hulls = [shapely.wkt.dumps(hull) for hull in bubbles['polygons']]
    wkt_lines = [shapely.wkt.dumps(line) for line in bubbles['lines']]
    wkt_points = [shapely.wkt.dumps(point) for point in bubbles['points']]

    with open('./datasets/generated/hulls_split.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['hull'])
        for wkt in wkt_hulls:
            writer.writerow([wkt])
        for wkt in wkt_lines:
            writer.writerow([wkt])
        for wkt in wkt_points:
            writer.writerow([wkt])


def plot_bubbles() -> None:
    """
    This method allows to plot the saved bubbles using matplotlib.
    """
    df = pd.read_csv('./datasets/generated/hulls_split.csv')
    polygon_col_name = 'hull'
    df[polygon_col_name] = df[polygon_col_name].apply(lambda x: wkt.loads(x))

    for _, geom in df.iterrows():
        if (geom[0].geom_type == 'Polygon'):
            x, y = geom[0].exterior.xy
            plt.plot(x, y)
    plt.show()


def run_splitting() -> None:
    """
    This method runs all necessary steps to perform the simple split algorithm.
    """
    spinner = Halo("Loading")
    spinner.start(text="Recursively Splitting The Area To Find Bubbles")
    df = pd.read_parquet("./datasets/generated/cleared_ev.parquet")
    bubbles = split_area(df)
    spinner.succeed()
    spinner.start("Saving The Bubbles")
    save_bubbles(bubbles)
    spinner.succeed()
