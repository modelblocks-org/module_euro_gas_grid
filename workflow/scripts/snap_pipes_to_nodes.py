"""A simple script to 'snap' pipelines to the nearest node.

SciGRID_gas does not provide IDs to link these, so we use a small buffer.
"""

import _schemas
import country_converter as coco
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Point


def snap_pipes_to_nodes(
    pipes: gpd.GeoDataFrame,
    nodes: gpd.GeoDataFrame,
    *,
    buffer_dist: float = 500.0,
    crs: str = "EPSG:3035",
):
    pipes = _schemas.PipelineSchema.validate(pipes)
    nodes = nodes.reset_index(drop=True)

    if pipes.crs != crs:
        pipes = pipes.to_crs(crs)
    if nodes.crs != crs:
        nodes = nodes.to_crs(crs)
    if not pipes.crs.is_projected:
        raise ValueError(f"Requested crs must be projected. Got {crs!r}.")

    nodes["node_id"] = np.arange(len(nodes), dtype=int)

    # endpoints (2 rows per line)
    endpoints = pd.concat(
        [
            gpd.GeoDataFrame(
                {"pipeline_id": pipes["pipeline_id"], "endpoint": "start"},
                geometry=pipes.geometry.apply(lambda ls: Point(ls.coords[0])),
                crs=pipes.crs,
            ),
            gpd.GeoDataFrame(
                {"pipeline_id": pipes["pipeline_id"], "endpoint": "end"},
                geometry=pipes.geometry.apply(lambda ls: Point(ls.coords[-1])),
                crs=pipes.crs,
            ),
        ],
        ignore_index=True,
    )

    # Nearest point within buffer
    matched = gpd.sjoin_nearest(
        endpoints,
        nodes[["node_id", "geometry"]],
        how="left",
        max_distance=buffer_dist,
        distance_col="snap_dist",
    )

    # fail fast if anything didn't match
    missing_mask = matched["node_id"].isna()
    if missing_mask.any():
        bad = matched.loc[missing_mask, ["pipeline_id", "endpoint"]]
        raise RuntimeError(
            f"Unmatched line endpoints within {buffer_dist} units:\n{bad.to_string(index=False)}"
        )

    for i in ["start", "end"]:
        ids = matched.loc[matched["endpoint"] == i].set_index("pipeline_id")["node_id"]
        pipes[f"{i}_node_id"] = pipes["pipeline_id"].map(ids).astype(int)

    # snap geometries (preserve direction: only replace first/last coord)
    point_geom = nodes.set_index("node_id").geometry

    def snap_ls(ls: LineString, s_id: int, e_id: int):
        coords = list(ls.coords)
        coords[0] = tuple(point_geom.loc[s_id].coords[0])  # preserves Z if present
        coords[-1] = tuple(point_geom.loc[e_id].coords[0])
        return LineString(coords)

    pipes["geometry"] = [
        snap_ls(ls, s_id, e_id)
        for ls, s_id, e_id in zip(
            pipes.geometry, pipes.start_node_id, pipes.end_node_id
        )
    ]

    # node coverage check (all nodes used by at least one pipeline endpoint)
    used = pd.Index(pd.concat([pipes.start_node_id, pipes.end_node_id]).unique())
    if not nodes["node_id"].isin(used).all():
        raise RuntimeError("Not all nodes were matched to pipelines.")

    # Graph metrics on nodes
    degree = pd.concat([pipes.start_node_id, pipes.end_node_id]).value_counts()

    # out_degree:
    both = pipes["is_bothDirection"].astype(bool)
    out_counts = pd.concat(
        [
            pipes.loc[~both, "start_node_id"],
            pipes.loc[both, "start_node_id"],
            pipes.loc[both, "end_node_id"],
        ]
    ).value_counts()

    # in_degree:
    in_counts = pd.concat(
        [
            pipes.loc[~both, "end_node_id"],
            pipes.loc[both, "start_node_id"],
            pipes.loc[both, "end_node_id"],
        ]
    ).value_counts()

    nodes["degree"] = nodes["node_id"].map(degree).fillna(0).astype(int)
    nodes["out_degree"] = nodes["node_id"].map(out_counts).fillna(0).astype(int)
    nodes["in_degree"] = nodes["node_id"].map(in_counts).fillna(0).astype(int)

    deg = nodes["degree"]
    in_deg = nodes["in_degree"]
    out_deg = nodes["out_degree"]

    isolated = (in_deg == 0) & (out_deg == 0)
    if isolated.any():
        raise RuntimeError(
            f"Isolated node(s): {nodes.loc[isolated, 'node_id'].tolist()}"
        )

    nodes["node_type"] = np.select(
        [
            (in_deg == 0) & (out_deg > 0),  # source
            (out_deg == 0) & (in_deg > 0),  # sink
            (deg == 1) & (in_deg == 1) & (out_deg == 1),  # terminal
            (deg == 2) & (in_deg == 1) & (out_deg == 1),  # connection (pass through)
            (in_deg > 0) & (out_deg > 0),  # junction (anything else with both)
        ],
        ["source", "sink", "terminal", "connection", "junction"],
        default="__error__",
    )

    return pipes, nodes


def fix_node_country_ids(
    nodes: gpd.GeoDataFrame,
    countries: gpd.GeoDataFrame,
    *,
    country_code_col: str = "country_code",
    country_id_col: str = "country_id",
    id_col: str = "sovereign_id",
    missing: str = "XXX",
):
    nodes = nodes.copy()
    countries = countries[[id_col, "geometry"]].copy()

    if nodes.crs != countries.crs:
        countries = countries.to_crs(nodes.crs)

    # 1) Translate using coco (trust not_found), with your explicit "XX" special-case
    uniq = nodes[country_code_col].dropna().unique()
    tr = {k: coco.convert(k, to="iso3", not_found=np.nan) for k in uniq if k != "XX"}
    tr["XX"] = missing

    nodes[country_id_col] = nodes[country_code_col].map(tr).fillna(missing)

    # 2) Attempt to spatially fill remaining 'XXX'
    m = nodes[country_id_col].eq(missing)
    if m.any():
        joined = gpd.sjoin(nodes.loc[m, ["geometry"]], countries, predicate="within", how="inner")

        # strict: no border ambiguity
        if joined.index.duplicated(keep=False).any():
            bad = joined.index[joined.index.duplicated(keep=False)].unique().tolist()
            raise RuntimeError(f"Ambiguous country match for node index(es): {bad}")

        nodes.loc[joined.index, country_id_col] = joined[id_col].astype(str)


    return nodes
