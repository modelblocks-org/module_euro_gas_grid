"""Split lines crossing boundaries."""

from collections.abc import Iterable
from dataclasses import dataclass

from shapely.geometry import LineString, Point
from shapely.geometry.base import BaseGeometry
from shapely.ops import substring, unary_union


@dataclass(frozen=True)
class Cut:
    """A cut location."""

    measure: float
    "Distance u->point in the line's CRS."
    point: Point
    "Point of this cut."


@dataclass(frozen=True)
class Segment:
    """A line segment as an exact substring of the original geometry."""

    start: float
    "Distance of the original geometry where this segment starts."
    end: float
    "Distance of the original geometry where this segment ends."
    geometry: LineString
    "Segment line geometry."


def build_boundary(shapes: Iterable[BaseGeometry]) -> BaseGeometry:
    """Build a single boundary geometry from an iterable of polygons."""
    return unary_union([g.boundary for g in shapes])


def _extract_split_points(intersection_geom: BaseGeometry) -> list[Point]:
    """Extract split points from (line ∩ boundary).

    - Points: kept
    - (Multi)LineString: an overlap, use endpoints (boundary points)
    - GeometryCollection: walk contents
    """
    pts: list[Point] = []
    stack: list[BaseGeometry] = [intersection_geom]

    while stack:
        g = stack.pop()
        if g.is_empty:
            continue

        t = g.geom_type
        if t == "Point":
            pts.append(g)
        elif t == "MultiPoint":
            pts.extend(list(g.geoms))
        elif t == "LineString":
            stack.append(g.boundary)
        elif t == "MultiLineString":
            for ls in g.geoms:
                stack.append(ls.boundary)
        elif t == "GeometryCollection":
            stack.extend(list(g.geoms))

        # else: ignore polygons etc.

    return pts


def _dedupe_measures(measures: list[float], *, tol_dist: float) -> list[float]:
    """Deduplicate measures: keep one cut per cluster within tol_m along the line."""
    if not measures:
        return []
    measures = sorted(measures)
    out = [measures[0]]
    last = measures[0]
    for m in measures[1:]:
        if (m - last) > tol_dist:
            out.append(m)
            last = m
    return out


def find_boundary_cuts(
    line: LineString,
    boundary: BaseGeometry,
    *,
    snap_tol_m: float = 1.0,
    drop_endpoints_within_m: float | None = None,
) -> list[Cut]:
    """Find boundary crossings as measures along the line.

    - boundary: a single Shapely geometry (e.g. unary_union(shapes.boundary))
    - snap_tol_m: dedupes near-identical cut positions (meters along line)
    - drop_endpoints_within_m: drops cuts too close to endpoints (defaults to snap_tol_m)
    """
    if line.geom_type != "LineString":
        raise TypeError(f"Expected LineString, got {line.geom_type}")

    if drop_endpoints_within_m is None:
        drop_endpoints_within_m = snap_tol_m

    if not line.intersects(boundary):
        return []

    inter = line.intersection(boundary)
    points = _extract_split_points(inter)
    if not points:
        return []

    line_m = float(line.length)
    cuts_m: list[float] = []

    for point in points:
        dist = float(line.project(point))
        if (
            dist <= drop_endpoints_within_m
            or (line_m - dist) <= drop_endpoints_within_m
        ):
            continue
        cuts_m.append(dist)

    cuts_m = _dedupe_measures(cuts_m, tol_dist=snap_tol_m)
    return [Cut(measure=m, point=line.interpolate(m)) for m in cuts_m]


def filter_cuts_min_segment(
    cuts: list[Cut], *, line_length_m: float, min_segment_len_m: float
) -> list[Cut]:
    """Drop cuts so that all resulting segments are at least min_segment_len_m."""
    if min_segment_len_m <= 0 or not cuts:
        return cuts

    measures = [c.measure for c in cuts]
    kept = [0.0]

    for m in measures:
        if (m - kept[-1]) >= min_segment_len_m:
            kept.append(m)

    while len(kept) > 1 and (line_length_m - kept[-1]) < min_segment_len_m:
        kept.pop()

    kept_measures = set(kept[1:])  # exclude 0.0 (start must be added separately)
    return [c for c in cuts if c.measure in kept_measures]


def cut_line_at_measures(line: LineString, measures: list[float]) -> list[Segment]:
    """Cut a LineString into exact substrings at measures along the line."""
    if line.geom_type != "LineString":
        raise TypeError(f"Expected LineString, got {line.geom_type}")

    line_m = float(line.length)
    ms = sorted(m for m in measures if 0.0 < m < line_m)

    marks = [0.0] + ms + [line_m]
    segs: list[Segment] = []
    for a, b in zip(marks[:-1], marks[1:]):
        seg = substring(line, a, b)
        if seg.geom_type != "LineString":
            raise RuntimeError(f"substring produced {seg.geom_type}")
        segs.append(Segment(start=float(a), end=float(b), geometry=seg))
    return segs


def cut_line_by_boundary(
    line: LineString,
    boundary: BaseGeometry,
    *,
    snap_tol_m: float = 1.0,
    min_segment_len_m: float = 0.0,
) -> tuple[list[Segment], list[Cut]]:
    """Split a line into segements using a boundary geometry.

    1. find boundary cuts
    2. optionally filter cuts to avoid tiny segments
    3. cut the line into segments (geometry preserved)
    """
    cuts = find_boundary_cuts(line, boundary, snap_tol_m=snap_tol_m)
    cuts = filter_cuts_min_segment(
        cuts, line_length_m=float(line.length), min_segment_len_m=min_segment_len_m
    )
    segs = cut_line_at_measures(line, [c.measure for c in cuts])
    return segs, cuts
