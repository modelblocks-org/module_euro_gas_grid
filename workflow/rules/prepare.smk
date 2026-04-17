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
        "../envs/euro-gas-grid.yaml"
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
        pipelines="<resources>/automatic/pipelines.parquet",
        nodes="<resources>/automatic/nodes.parquet",
        fig=report(
            "<resources>/automatic/pipelines.png",
            caption="../report/prepare_pipelines.rst",
            category="Euro gas grid module",
        ),
    log:
        "<logs>/prepare_pipelines.log",
    conda:
        "../envs/euro-gas-grid.yaml"
    params:
        imputation=config.get("imputation", {}),
        projected_crs=config["crs"]["projected"],
    message:
        "Harmonising SciGRID pipelines."
    script:
        "../scripts/prepare_pipelines.py"
