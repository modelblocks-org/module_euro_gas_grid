"""Plot functions used in one or more rules."""

import geopandas as gpd
import pandas as pd
from matplotlib.axes import Axes


def get_padded_bounds(gdf: gpd.GeoDataFrame, pad_frac: float = 0.05):
    """Get a square with some padding."""
    minx, miny, maxx, maxy = gdf.total_bounds
    dx, dy = (maxx - minx), (maxy - miny)
    pad = pad_frac * max(dx, dy) if max(dx, dy) > 0 else 0.1
    return (minx - pad, maxx + pad), (miny - pad, maxy + pad)


def plot_density(ax: Axes, values: pd.Series, title: str):
    """Plot a simple density kernel of a numeric value."""
    values.plot.density(ax=ax)
    ax.set_xlim(values.min(), values.max())
    ax.tick_params(left=False, labelleft=False)
    ax.set_title(title)


def style_map_plot(ax: Axes, xlim: tuple, ylim: tuple, title: str):
    """Standardise map plots (needs projected CRS)."""
    ax.set_xlim(*xlim)
    ax.set_ylim(*ylim)
    ax.set_aspect("equal", adjustable="datalim", anchor="C")
    ax.set_title(title)

    ax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    ax.xaxis.get_offset_text().set_visible(False)
    ax.yaxis.get_offset_text().set_visible(False)
