"""Assign and aggregate existing gas storage to user-provided shapes."""

import sys
from typing import TYPE_CHECKING, Any

import _plots
import _schemas
import _utils
import cmap
import geopandas as gpd
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.colors import LogNorm
from matplotlib.lines import Line2D

if TYPE_CHECKING:
    snakemake: Any

MISSING_COLOR = "#f2f2f2"
POINT_STYLE = {
    False: ("s", "#fdd0a2", "Outside shapes"),
    True: ("s", "#e6550d", "Inside shapes"),
}


def assign_locations_to_shapes(
    locations: gpd.GeoDataFrame, shapes: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Assign each storage point to at most one intersecting shape."""
    locations = _utils.to_crs(locations, shapes.crs).reset_index(drop=True)
    points = locations.copy()
    points["_point_order"] = points.index

    polygons = shapes[["shape_id", "country_id", "shape_class", "geometry"]].copy()
    polygons["_shape_area"] = polygons.geometry.area
    joined = gpd.sjoin(points, polygons, how="left", predicate="intersects")
    joined = joined.sort_values(
        ["_point_order", "_shape_area", "shape_id"],
        kind="mergesort",
        na_position="last",
    ).drop_duplicates("_point_order", keep="first")
    joined["selected"] = joined["shape_id"].notna()
    return joined.drop(columns=["index_right", "_shape_area", "_point_order"])


def aggregate_storage(
    assigned: gpd.GeoDataFrame, shapes: gpd.GeoDataFrame
) -> pd.DataFrame:
    """Aggregate working- and cushion-gas energy capacity to every shape."""
    grouped = (
        assigned.loc[assigned["selected"]]
        .groupby("shape_id")[["storage_working_gwh", "storage_cushion_gwh"]]
        .sum(min_count=1)
    )
    output = shapes[["shape_id"]].merge(
        grouped, left_on="shape_id", right_index=True, how="left"
    )
    return _schemas.GasStorageSchema.validate(output)


# FIXME: points 'outside' should be above inside points to help users catch missed storages
def plot(
    assigned: gpd.GeoDataFrame, capacities: pd.DataFrame, shapes: gpd.GeoDataFrame
):
    """Plot locations, working gas, and working gas relative to cushion gas."""
    fig, axs = plt.subplots(1, 3, figsize=(21, 7), layout="compressed")
    xlim, ylim = _plots.get_padded_bounds(shapes, pad_frac=0.02)
    visible = assigned.cx[xlim[0] : xlim[1], ylim[0] : ylim[1]]

    shapes.plot(ax=axs[0], color=MISSING_COLOR, zorder=-2)
    shapes.boundary.plot(ax=axs[0], color="black", lw=0.5, zorder=-1)
    for selected, (marker, color, _label) in POINT_STYLE.items():
        points = visible.loc[visible["selected"].eq(selected)]
        if not points.empty:
            points.plot(
                ax=axs[0],
                color=color,
                marker=marker,
                markersize=18,
                edgecolor="black",
                linewidth=0.25,
                zorder=2 if selected else 1,
            )

    handles = [
        Line2D(
            [0],
            [0],
            marker=marker,
            color="none",
            markerfacecolor=color,
            markeredgecolor="black",
            markeredgewidth=0.25,
            label=label,
            markersize=6,
        )
        for marker, color, label in POINT_STYLE.values()
    ]
    axs[0].legend(handles=handles, loc="best")
    _plots.style_map_plot(axs[0], "Existing gas storage locations", xlim, ylim)

    clustered = shapes[["shape_id", "geometry"]].merge(
        capacities, on="shape_id", how="left"
    )
    clustered["working_to_cushion_ratio"] = (
        clustered["storage_working_gwh"] / clustered["storage_cushion_gwh"]
    )

    plot_columns = [
        ("storage_working_gwh", "Working gas ($GWh$)"),
        ("working_to_cushion_ratio", "Working / cushion gas ratio"),
    ]
    for ax, (column, title) in zip(axs[1:], plot_columns, strict=True):
        if clustered[column].notna().any():
            plot_kwargs = {}
            legend_kwds = {}
            if column == "working_to_cushion_ratio":
                values = clustered[column].dropna()
                ratio_extent = max(values.max(), 1.0 / values.min())
                ratio_extent = max(ratio_extent, 1.0 + 1e-9)
                plot_kwargs = {
                    "cmap": cmap.Colormap("colorbrewer:RdYlBu").to_mpl(),
                    "norm": LogNorm(vmin=1.0 / ratio_extent, vmax=ratio_extent),
                }
                legend_kwds = {"format": "%.1f"}
            else:
                plot_kwargs = {"cmap": cmap.Colormap("bids:fake_parula").to_mpl()}
            clustered.plot(
                ax=ax,
                column=column,
                legend=True,
                legend_kwds=legend_kwds,
                lw=0,
                missing_kwds={"color": MISSING_COLOR},
                **plot_kwargs,
            )
        else:
            clustered.plot(ax=ax, color=MISSING_COLOR, lw=0)
        clustered.boundary.plot(ax=ax, color="black", lw=0.5)
        _plots.style_map_plot(ax, title, xlim, ylim)
    return fig, axs


def main():
    """Aggregate gas storage and create its diagnostic plot."""
    projected_crs = snakemake.params.projected_crs
    _utils.check_projected_crs(projected_crs)

    shapes = _utils.to_crs(gpd.read_parquet(snakemake.input.shapes), projected_crs)
    shapes = _schemas.ShapesSchema.validate(shapes)
    locations = _utils.to_crs(
        gpd.read_parquet(snakemake.input.locations), projected_crs
    )
    assigned = assign_locations_to_shapes(locations, shapes)
    capacities = aggregate_storage(assigned, shapes)

    capacities.to_parquet(snakemake.output.capacities)
    fig, _ = plot(assigned, capacities, shapes)
    fig.savefig(snakemake.output.fig, dpi=300)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w", buffering=1)
    main()
