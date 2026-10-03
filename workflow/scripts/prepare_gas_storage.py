"""Prepare existing gas storage locations from SciGrid_Gas."""

import sys
from typing import TYPE_CHECKING, Any

import _schemas
import geopandas as gpd
import pandas as pd

if TYPE_CHECKING:
    snakemake: Any


def prepare_gas_storage(raw_file: str, gas_kwh_per_m3_lhv: float) -> gpd.GeoDataFrame:
    """Normalize SciGRID storage points with working- and cushion-gas volumes."""
    raw = gpd.read_file(raw_file).reset_index(drop=True)
    params = pd.json_normalize(raw["param"])
    working_gas = params["max_workingGas_M_m3"]
    cushion_gas = params["max_cushionGas_M_m3"]

    source_id = raw["id"]
    storage = gpd.GeoDataFrame(
        {
            "storage_id": source_id,
            "name": raw["name"],
            "facility_type": "storage",
            # kWh/m³ and GWh/MCM have the same numerical conversion factor.
            "storage_working_gwh": working_gas * gas_kwh_per_m3_lhv,
            "storage_cushion_gwh": cushion_gas * gas_kwh_per_m3_lhv,
        },
        geometry=raw.geometry,
        crs=raw.crs,
    )
    return _schemas.GasStorageNodeSchema.validate(storage)


def main():
    """Prepare and save gas storage locations."""
    storage = prepare_gas_storage(
        snakemake.input.storage, snakemake.params.gas_kwh_per_m3_lhv
    )
    storage.to_parquet(snakemake.output.storage)


if __name__ == "__main__":
    sys.stderr = open(snakemake.log[0], "w", buffering=1)
    main()
