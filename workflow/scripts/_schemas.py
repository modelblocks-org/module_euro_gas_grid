"""Schemas for key files."""

from pandera import pandas as pa
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries
from shapely.validation import make_valid

ISO3_RE = r"^[A-Z]{3}$"


class LandSchema(pa.DataFrameModel):
    class Config:
        coerce = True
        strict = "filter"

    feature_class: Series[str] = pa.Field(eq="Land")
    "Must be 'Land'."
    geometry: GeoSeries
    "Land polygons."


class CountriesSchema(pa.DataFrameModel):
    class Config:
        coerce = True
        strict = "filter"

    sovereign_id: Series[str] = pa.Field(str_length=3)
    "ISO3-like code of sovereign body."
    sovereign_name: Series[str]
    "Name of the sovereign body."
    sovereign_type: Series[str]
    "Type of sovereign body."
    admin_name: Series[str]
    "Name of the administrative body."
    admin_id: Series[str] = pa.Field(str_matches=ISO3_RE)
    "ISO3 code of administrative body."
    geometry: GeoSeries
    "Landmass polygon of soberign body."


class PipelineSchema(pa.DataFrameModel):
    class Config:
        coerce = True
        strict = "filter"

    pipeline_id: Series[int] = pa.Field(unique=True)
    """Unique identifier."""
    name: Series[str]
    "Pipeline name."
    start_country_id: Series[str] = pa.Field(str_length=3)
    "ISO 3 code of the country in the start point."
    end_country_id: Series[str] = pa.Field(str_length=3)
    "ISO 3 code of the country in the start point."
    diameter_mm: Series[float] = pa.Field(gt=0)
    "Pipeline diameter."
    diameter_method: Series[str]
    "Diameter estimation metadata."
    max_cap_M_m3_per_d: Series[float] = pa.Field(gt=0)
    "Max capacity estimate."
    max_cap_method: Series[str]
    "Max capacity metadata (used to select formulae)."
    max_pressure_bar: Series[float] = pa.Field(gt=0)
    "Max pressure (used for sectioning)."
    is_bothDirection: Series[bool]
    "Pipeline direction."
    ch4_capacity_mw: Series[float] = pa.Field(gt=0)
    "CH4 pipeline capacity in MW (nominal)."
    ch4_capacity_method: Series[str]
    "Method used to calculate CH4 capacity."
    is_offshore: Series[bool]
    "Flag offshore pipelines (outside of country landmass)."
    geometry: GeoSeries
    "Must be lines."

    @pa.check("geometry")
    def check_geometries(cls, geom):
        """Ensure geometries are always simple lines."""
        return not {"LineString"} ^ set(geom.geom_type.unique())


class ShapesSchema(pa.DataFrameModel):
    """Schema for geographic shapes."""

    shape_id: Series[str] = pa.Field(unique=True)
    "A unique identifier for this shape."
    country_id: Series[str]
    "Country ISO alpha-3 code."
    shape_class: Series[str] = pa.Field(isin=["land", "maritime"])
    "Identifier of the shape's context."
    geometry: GeoSeries
    "Shape (multi)polygon."

    @pa.dataframe_parser
    def fix_geometries(cls, df):
        """Attempt to correct empty or malformed geometries."""
        mask = df["geometry"].apply(lambda g: (g is not None) and (not g.is_empty))
        df = df.loc[mask]
        df["geometry"] = df["geometry"].apply(
            lambda g: g if g.is_valid else make_valid(g)
        )
        return df
