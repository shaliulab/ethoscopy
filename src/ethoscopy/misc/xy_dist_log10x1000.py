import numpy as np

def euclidean_distance_over_time(xy):
    return np.sqrt((np.diff(xy, axis=0)**2).sum(axis=1))


def compute_xy_dist_log10x1000(data, min_distance):
    dist = euclidean_distance_over_time(data[["x", "y"]].values) + min_distance
    xy_dist_log10x1000 = np.log10(dist) * 1000
    xy_dist_log10x1000 = np.array([min_distance] + xy_dist_log10x1000.tolist())
    return xy_dist_log10x1000