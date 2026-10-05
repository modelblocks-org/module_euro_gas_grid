"""Assign and aggregate existing gas storage to user-provided shapes."""

import logging
import sys
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

import _plots
import _schemas
import _utils
import cmap
import geopandas as gpd
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.lines import Line2D

if TYPE_CHECKING:
    snakemake: Any

logger = logging.getLogger(__name__)

MISSING_COLOR = "#f2f2f2"
POINT_STYLE = {
    False: ("s", "#fdd0a2", "Unassigned"),
    True: ("s", "#e6550d", "Assigned"),
}


def snap_locations_to_nearest_shapes(
    assigned: gpd.GeoDataFrame, shapes: gpd.GeoDataFrame, storage_ids: Collection[str]
) -> gpd.GeoDataFrame:
    """Assign configured, unassigned storage locations to their nearest shape."""
    storage_ids = set(storage_ids)
    unknown_ids = sorted(storage_ids.difference(assigned["storage_id"]))
    if unknown_ids:
        raise ValueError(f"Unknown storage IDs configured for snapping: {unknown_ids}")

    snap_mask = assigned["shape_id"].isna() & assigned["storage_id"].isin(storage_ids)
    if not snap_mask.any():
        return assigned

    shape_columns = ["shape_id", "country_id", "shape_class"]
    nearest = _utils.match_points_to_polygons(
        assigned.loc[snap_mask], shapes, shape_columns, predicate="nearest"
    )
    assigned.loc[snap_mask, shape_columns] = nearest[shape_columns]

    snapped = assigned.loc[snap_mask, ["storage_id"]].join(nearest[["shape_id"]])
    for storage_id, shape_id in snapped.dropna(subset=["shape_id"]).itertuples(
        index=False, name=None
    ):
        logger.info("Snapped storage_id=%s to shape_id=%s.", storage_id, shape_id)

    return assigned


def assign_locations_to_shapes(
    locations: gpd.GeoDataFrame,
    shapes: gpd.GeoDataFrame,
    snap_storage_ids: Collection[str] = (),
) -> gpd.GeoDataFrame:
    """Assign storage points to intersecting shapes, with opt-in nearest snapping."""
    locations = _utils.to_crs(locations, shapes.crs).reset_index(drop=True)
    shape_columns = ["shape_id", "country_id", "shape_class"]
    assigned = locations.join(
        _utils.match_points_to_polygons(locations, shapes, shape_columns)
    )
    assigned = snap_locations_to_nearest_shapes(assigned, shapes, snap_storage_ids)
    assigned["selected"] = assigned["shape_id"].notna()
    return assigned


def aggregate_storage(
    assigned: gpd.GeoDataFrame, shapes: gpd.GeoDataFrame
) -> pd.DataFrame:
    """Aggregate working and cushion gas energy capacity to every shape."""
    grouped = (
        assigned.loc[assigned["selected"]]
        .groupby("shape_id")[["storage_working_gwh", "storage_cushion_gwh"]]
        .sum(min_count=1)
    )
    output = shapes[["shape_id"]].merge(
        grouped, left_on="shape_id", right_index=True, how="left"
    )
    return _schemas.GasStorageSchema.validate(output)


def plot(
    assigned: gpd.GeoDataFrame, capacities: pd.DataFrame, shapes: gpd.GeoDataFrame
):
    """Plot storage locations and working gas capacity."""
    fig, axs = plt.subplots(1, 2, figsize=(10, 5), layout="compressed")
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
                zorder=1 if selected else 2,
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
    if clustered["storage_working_gwh"].notna().any():
        clustered.plot(
            ax=axs[1],
            column="storage_working_gwh",
            legend=True,
            cmap=cmap.Colormap("bids:fake_parula").to_mpl(),
            lw=0,
            missing_kwds={"color": MISSING_COLOR},
        )
    else:
        clustered.plot(ax=axs[1], color=MISSING_COLOR, lw=0)
    clustered.boundary.plot(ax=axs[1], color="black", lw=0.5)
    _plots.style_map_plot(axs[1], "Working gas ($GWh$)", xlim, ylim)
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
    assigned = assign_locations_to_shapes(
        locations, shapes, snakemake.params.snap_storage_ids
    )
    capacities = aggregate_storage(assigned, shapes)

    capacities.to_parquet(snakemake.output.capacities)
    fig, _ = plot(assigned, capacities, shapes)
    fig.savefig(snakemake.output.fig, dpi=300)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w", buffering=1)
    logging.basicConfig(level=logging.INFO)
    main()
