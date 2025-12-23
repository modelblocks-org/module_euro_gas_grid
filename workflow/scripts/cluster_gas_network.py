from __future__ import annotations

from collections import deque

import _utils
import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
from pyproj import CRS
from shapely.geometry import LineString


def build_exchange_capacity_graph(
    pipes: gpd.GeoDataFrame,
    nodes: gpd.GeoDataFrame,
    *,
    inside_sovereign_ids: set[str] | list[str] | pd.Index,
    capacity_col: str = "ch4_capacity_mw",
    start_col: str = "start_node_id",
    end_col: str = "end_node_id",
    bidirectional_col: str = "is_bidirectional",
    shape_col: str = "shape_id",
    sovereign_col: str = "sovereign_id",
    reduce_transmission_chains: bool = True,
    return_geodataframes: bool = False,
) -> nx.DiGraph | tuple[nx.DiGraph, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Build a reduced capacity graph between shapes, outside sovereigns, and transmission junctions."""
    _utils.check_projected_crs(nodes.crs)

    # robust CRS equality
    if not CRS(pipes.crs).equals(CRS(nodes.crs)):
        raise ValueError("pipes and nodes must share a CRS.")

    inside_sovereign_ids = set(inside_sovereign_ids)

    shape_mask = nodes[shape_col].notna()
    outside_mask = (
        nodes[shape_col].isna()
        & nodes[sovereign_col].notna()
        & ~nodes[sovereign_col].isin(inside_sovereign_ids)
    )

    model_node = np.where(
        shape_mask,
        "shape:" + nodes[shape_col].astype(str),
        np.where(
            outside_mask,
            "outside:" + nodes[sovereign_col].astype(str),
            "trans:" + nodes["node_id"].astype(str),
        ),
    )

    node_map = pd.Series(model_node, index=nodes["node_id"].to_numpy())

    # representative points (debug-only)
    xy = np.column_stack([nodes.geometry.x.to_numpy(), nodes.geometry.y.to_numpy()])
    tmp = pd.DataFrame({"model_node": model_node, "x": xy[:, 0], "y": xy[:, 1]})
    reps = tmp.groupby("model_node", sort=False)[["x", "y"]].mean()
    geom_map = {k: gpd.points_from_xy([v["x"]], [v["y"]])[0] for k, v in reps.iterrows()}

    # edges from pipes (aggregate parallel edges by summing capacities)
    df = pipes[[start_col, end_col, capacity_col, bidirectional_col]].copy()
    df["u"] = df[start_col].map(node_map)
    df["v"] = df[end_col].map(node_map)

    # drop unmapped endpoints
    df = df.dropna(subset=["u", "v"])

    # drop edges that collapse within the same model node
    df = df[df["u"] != df["v"]]

    fwd = df[["u", "v", capacity_col]].rename(columns={capacity_col: "capacity"})
    rev = (
        df.loc[df[bidirectional_col].astype(bool), ["u", "v", capacity_col]]
        .rename(columns={"u": "v", "v": "u", capacity_col: "capacity"})
    )
    edges = pd.concat([fwd, rev], ignore_index=True)
    edges = edges.groupby(["u", "v"], as_index=False)["capacity"].sum()

    # build graph
    G = nx.DiGraph()
    for mn in pd.unique(model_node):
        if mn.startswith("shape:"):
            G.add_node(
                mn,
                kind="shape",
                geometry=geom_map[mn],
                shape_id=mn.split("shape:", 1)[1],
            )
        elif mn.startswith("outside:"):
            G.add_node(
                mn,
                kind="outside",
                geometry=geom_map[mn],
                sovereign_id=mn.split("outside:", 1)[1],
            )
        else:
            G.add_node(mn, kind="transmission", geometry=geom_map[mn])

    for u, v, cap in edges.itertuples(index=False):
        cap = float(cap)
        if cap <= 0:
            continue
        if G.has_edge(u, v):
            G[u][v]["capacity"] += cap
        else:
            G.add_edge(u, v, capacity=cap)

    # series reduction on transmission degree-2 nodes
    if reduce_transmission_chains:
        q = deque([n for n, d in G.nodes(data=True) if d.get("kind") == "transmission"])

        def edge_cap(a: str, b: str) -> float:
            data = G.get_edge_data(a, b)
            return 0.0 if data is None else float(data.get("capacity", 0.0))

        while q:
            x = q.popleft()
            if x not in G or G.nodes[x].get("kind") != "transmission":
                continue

            nbrs = set(G.predecessors(x)) | set(G.successors(x))
            if len(nbrs) != 2:
                continue

            a, b = tuple(nbrs)

            cap_ab = min(edge_cap(a, x), edge_cap(x, b))
            cap_ba = min(edge_cap(b, x), edge_cap(x, a))

            G.remove_node(x)

            if cap_ab > 0:
                if G.has_edge(a, b):
                    G[a][b]["capacity"] += cap_ab
                else:
                    G.add_edge(a, b, capacity=cap_ab)

            if cap_ba > 0:
                if G.has_edge(b, a):
                    G[b][a]["capacity"] += cap_ba
                else:
                    G.add_edge(b, a, capacity=cap_ba)

            q.append(a)
            q.append(b)

    if not return_geodataframes:
        return G

    nodes_gdf = gpd.GeoDataFrame(
        [
            {
                "node": n,
                **{k: v for k, v in d.items() if k != "geometry"},
                "geometry": d.get("geometry"),
            }
            for n, d in G.nodes(data=True)
        ],
        geometry="geometry",
        crs=nodes.crs,
    )

    pair = {}
    for u, v, d in G.edges(data=True):
        a, b = (u, v) if u <= v else (v, u)
        rec = pair.setdefault((a, b), {"a": a, "b": b, "cap_ab": 0.0, "cap_ba": 0.0})
        if u == a and v == b:
            rec["cap_ab"] += float(d.get("capacity", 0.0))
        else:
            rec["cap_ba"] += float(d.get("capacity", 0.0))

    edge_rows = []
    for (a, b), rec in pair.items():
        ga = G.nodes[a].get("geometry")
        gb = G.nodes[b].get("geometry")
        edge_rows.append(
            {**rec, "capacity": max(rec["cap_ab"], rec["cap_ba"]), "geometry": LineString([ga, gb])}
        )

    edges_gdf = gpd.GeoDataFrame(edge_rows, geometry="geometry", crs=nodes.crs)
    return G, nodes_gdf, edges_gdf

