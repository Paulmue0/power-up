import csv

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shapely.wkt
from halo import Halo
from scipy.spatial import ConvexHull
from shapely.geometry import LineString, MultiPoint, Point, Polygon
from sklearn.cluster import MiniBatchKMeans


def run_kmeans_batched() -> None:
    """
    The method performs the calculation of the bubbles using the Kmeans Batched
    """
    spinner = Halo("Loading")
    spinner.start("Reading Dataset")
    df = pd.read_parquet("./datasets/generated/cleared_ev.parquet")
    data = df[['y_mp_100m', 'x_mp_100m', 'EV']].values
    spinner.succeed()

    # Set the number of clusters to the total sum of 'ev' values divided by 10
    num_clusters = int(np.sum(data[:, 2]) / 10)

    spinner.start("Running MiniBatchKmeans")
    kmeans = MiniBatchKMeans(n_clusters=num_clusters,
                             batch_size=1000, n_init='auto').fit(data[:, :2])
    spinner.succeed()
    labels = kmeans.labels_

    convex_hulls = []
    points = []
    lines = []

    spinner.start("Computing Convex Hulls")
    for i in range(num_clusters):
        cluster = data[labels == i]
        if len(cluster) < 3:
            if len(cluster) < 1:
                continue
            elif len(cluster) == 2:
                line = LineString(cluster)
                lines.append(line)
            else:
                point = Point(cluster[0])
                points.append(point)
        else:
            hull = ConvexHull(cluster[:, :2])
            convex_hulls.append(Polygon(cluster[hull.vertices, :2]))

    wkt_hulls = [shapely.wkt.dumps(hull) for hull in convex_hulls]
    wkt_lines = [shapely.wkt.dumps(line) for line in lines]
    wkt_points = [shapely.wkt.dumps(point) for point in points]
    spinner.succeed()

    spinner.start("Saving Bubbles To Disk")
    with open('./datasets/generated/hulls_batched.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['hull'])
        for wkt in wkt_hulls:
            writer.writerow([wkt])
        for wkt in wkt_lines:
            writer.writerow([wkt])
        for wkt in wkt_points:
            writer.writerow([wkt])
    spinner.succeed()


# # Compute the number of ev in each cluster
# ev_count = [np.sum(data[labels == i, 2]) for i in range(num_clusters)]

# # Normalize the ev count
# ev_count = ev_count / np.max(ev_count)
