from functools import wraps
import matplotlib.pyplot as plt

import numpy as np
import dask


def dask_hist(func):
    '''
    Using dask, send the partitions of dataframe to a function that defines,
    fills and returns a Hist histogram.  Then sum the resulting histograms and
    return it.
    '''
    @wraps(func)
    def send_to_dask(dataframe):
        return dask.dataframe.map_partitions(func, dataframe).compute().sum()

    return send_to_dask


def profile_plot(func):
    '''
    This is a temporary decorator.  Currently, the Hist class doesn't know how
    draw itself if it contains a .Mean() or other accumulator. This returns the
    approriate matplotlib errorbar plot.  It should go away when this
    functionality is enabled.
    '''
    @wraps(func)
    def create_plot(dataframe, **kwargs):

        dask_histogram = func(dataframe)
        centers = dask_histogram.axes.centers[0]

        results = dask_histogram.view()
        mean = results.value
        error_on_mean = np.sqrt(results.variance/results.count)

        ax = kwargs.pop('ax', None)
        if ax is None:
            ax = plt.gca()

        return ax.errorbar(x=centers, y=mean, yerr=error_on_mean, fmt='.', markersize=3, **kwargs)

    return create_plot
