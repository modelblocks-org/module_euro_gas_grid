"""Gas network clustering to shapes."""

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

if TYPE_CHECKING:
    snakemake: Any


def get_area_km2(gdf: gpd.GeoDataFrame) -> pd.Series:
    """Area of geometries in km^2."""
    factor = _utils.get_crs_meter_conversion_factor(gdf.crs)
    area_native = gdf.geometry.area
    area_m2 = area_native * (factor**2)
    return area_m2 / 1e6


def salt_cavern_potential_gwh(
    caverns: gpd.GeoDataFrame,
    shapes: gpd.GeoDataFrame,
    *,
    shape_id_col: str = "shape_id",
    storage_type_col: str = "storage_type",
    capacity_per_area_col: str = "gwh_per_km2",
    min_gwh: float = 1e-6,
) -> pd.DataFrame:
    """Compute salt cavern storage potential per shape_id in GWh."""
    storage_types = pd.Index(caverns[storage_type_col].dropna().unique())

    caverns = _utils.to_crs(caverns, shapes.crs)

    cav = caverns[[capacity_per_area_col, storage_type_col, "geometry"]].copy()
    shp = shapes[[shape_id_col, "geometry"]].copy()

    cav["area_caverns_km2"] = get_area_km2(cav)
    overlay = gpd.overlay(
        shp.reset_index(drop=True), cav.reset_index(drop=True), how="intersection"
    )

    overlay["share"] = get_area_km2(overlay) / overlay["area_caverns_km2"]
    overlay["e_nom_gwh"] = (
        overlay[capacity_per_area_col] * overlay["share"] * overlay["area_caverns_km2"]
    )

    out = (
        overlay.groupby([shape_id_col, storage_type_col])["e_nom_gwh"]
        .sum()
        .unstack(storage_type_col)
        .reindex(columns=storage_types)  # ensure all storage types always exist
        .fillna(0.0)  # clears NaNs introduced by unstack
        .rename(columns=lambda c: f"{c}_gwh")
        .reset_index()
    )

    # Remove negative numbers and small values.
    gwh_cols = out.columns.drop(shape_id_col)

    out[gwh_cols] = out[gwh_cols].mask(
        (out[gwh_cols] < 0) | (out[gwh_cols].abs() < min_gwh)
    )
    out = out.dropna(subset=gwh_cols, how="all")
    out[gwh_cols] = out[gwh_cols].fillna(0.0)

    out["total_gwh"] = out[gwh_cols].sum(axis="columns")
    return out


def plot(
    shapes: gpd.GeoDataFrame,
    potential: gpd.GeoDataFrame,
    caverns: gpd.GeoDataFrame,
    *,
    shape_id_col="shape_id",
    storage_type_col="storage_type",
):
    """Plot salt cavern potential."""
    fig, axs = plt.subplots(2, 2, figsize=(12, 12), layout="compressed")
    axs = axs.ravel()

    # 1 - Raw cavern density
    caverns = gpd.overlay(caverns, shapes[[shape_id_col, "geometry"]], how="intersection")

    caverns.plot(
        ax=axs[0],
        column=storage_type_col,
        cmap=cmap.Colormap("tol:high_contrast_alt").to_mpl(),
        lw=0,
        legend=True
    )
    shapes.boundary.plot(ax=axs[0], color="black", lw=0.5)
    _plots.style_map_plot(axs[0], "Salt cavern potential density ($GWh/km^2$)")

    # 2 to 4 - Aggregated potentials by storage type
    gdf = shapes[[shape_id_col, "geometry"]].merge(potential, on=shape_id_col, how="left")

    cols = [f"{st}_gwh" for st in ["offshore", "nearshore", "onshore"]]
    all_types = pd.concat([gdf[c] for c in cols], ignore_index=True)
    all_types = all_types[all_types > 0].dropna()
    shared_norm = LogNorm(vmin=all_types.min(), vmax=all_types.max()) if len(all_types) else None

    for ax, st in zip(axs[1:], ["offshore", "nearshore", "onshore"]):
        col = f"{st}_gwh"
        vals = gdf[col].where(gdf[col] > 0)

        gdf.assign(**{col: vals}).plot(
            ax=ax,
            column=col,
            cmap=cmap.Colormap("cmocean:balance_blue_r").to_mpl(),
            norm=shared_norm,
            legend=True,
            lw=0,
            missing_kwds={"color": "lightgrey"},
        )
        gdf.boundary.plot(ax=ax, color="black", lw=0.5)
        _plots.style_map_plot(ax, f"Salt cavern H2 potential - {st} ($GWh$)")

    return fig, axs


def main():
    """Main snakemake process."""
    proj_crs = snakemake.params.projected_crs
    _utils.check_projected_crs(proj_crs)
    shapes = _utils.to_crs(gpd.read_parquet(snakemake.input.shapes), proj_crs)
    shapes = _schemas.ShapesSchema.validate(shapes)
    caverns = _utils.to_crs(gpd.read_parquet(snakemake.input.salt_caverns), proj_crs)

    min_gwh_tolerance = snakemake.params.min_gwh_tolerance
    potential = salt_cavern_potential_gwh(caverns, shapes, min_gwh=min_gwh_tolerance)
    potential = _schemas.H2Potential.validate(potential)
    potential.to_parquet(snakemake.output.salt_cavern_h2_potential)

    fig, _ = plot(shapes, potential, caverns)
    fig.savefig(snakemake.output.fig, dpi=300)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w")
    main()
