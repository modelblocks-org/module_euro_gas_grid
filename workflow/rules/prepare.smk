"""Rules to standardise files."""


rule prepare_countries:
    input:
        raw_countries="<resources>/automatic/countries.zip",
    output:
        countries="<resources>/automatic/countries.parquet",
        fig="<resources>/automatic/countries.png",
    log:
        "<logs>/prepare_countries.log",
    conda:
        "../envs/module.yaml"
    message:
        "Preparing country data."
    script:
        "../scripts/prepare_countries.py"


rule prepare_pipelines:
    input:
        raw_pipelines="<resources>/automatic/scigrid_gas/PipeSegments.geojson",
        raw_nodes="<resources>/automatic/scigrid_gas/Nodes.geojson",
        countries=rules.prepare_countries.output.countries,
    output:
        pipelines="<resources>/automatic/scenarios/{scenario}/pipelines.parquet",
        nodes="<resources>/automatic/scenarios/{scenario}/nodes.parquet",
        fig=report(
            "<resources>/automatic/scenarios/{scenario}/pipelines.png",
            caption="../report/prepare_pipelines.rst",
            category="European Gas Grid",
        ),
    log:
        "<logs>/scenarios/{scenario}/prepare_pipelines.log",
    conda:
        "../envs/module.yaml"
    params:
        gas_kwh_per_m3_lhv=lambda wc: scenario_config(wc)["imputation"][
            "gas_kwh_per_m3_lhv"
        ],
        imputation=lambda wc: scenario_config(wc)["imputation"]["pipelines"],
        projected_crs=lambda wc: scenario_config(wc)["crs"]["projected"],
    message:
        "Harmonising SciGRID pipelines for scenario {wildcards.scenario}."
    script:
        "../scripts/prepare_pipelines.py"


rule prepare_gas_storage:
    input:
        storage="<resources>/automatic/scigrid_gas/Storages.geojson",
    output:
        storage="<resources>/automatic/scenarios/{scenario}/gas_storage.parquet",
    log:
        "<logs>/scenarios/{scenario}/prepare_gas_storage.log",
    conda:
        "../envs/module.yaml"
    params:
        gas_kwh_per_m3_lhv=lambda wc: scenario_config(wc)["imputation"][
            "gas_kwh_per_m3_lhv"
        ],
    message:
        "Preparing existing gas storage locations for scenario {wildcards.scenario}."
    script:
        "../scripts/prepare_gas_storage.py"
