"""Prepare existing gas storage locations from SciGrid_Gas."""

import sys
from typing import TYPE_CHECKING, Any

import _schemas
import geopandas as gpd
import pandas as pd

if TYPE_CHECKING:
    snakemake: Any

# Assumed gas GCV/HHV: 11.36 kWh/m³ equals 11.36 GWh/MCM.
# TODO: Reconcile with this module's 10.5 kWh/m³ LHV flow basis.
MCM_TO_GWH = 11.36


def prepare_gas_storage(raw_file: str) -> gpd.GeoDataFrame:
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
            "storage_working_gwh": working_gas * MCM_TO_GWH,
            "storage_cushion_gwh": cushion_gas * MCM_TO_GWH,
        },
        geometry=raw.geometry,
        crs=raw.crs,
    )
    return _schemas.GasStorageNodeSchema.validate(storage)


def main():
    """Prepare and save gas storage locations."""
    storage = prepare_gas_storage(snakemake.input.storage)
    storage.to_parquet(snakemake.output.storage)


if __name__ == "__main__":
    # TODO: add buffering=1 to all calls.
    sys.stderr = open(snakemake.log[0], "w")
    main()
