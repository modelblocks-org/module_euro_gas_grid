"""Rules to used to download automatic resource files."""


wildcard_constraints:
    nat_earth="|".join(["countries"]),
    scigrid_gas="|".join(
        [
            "BorderPoints",
            "Compressors",
            "Consumers",
            "LNGs",
            "Nodes",
            "PipeSegments",
            "PowerPlants",
            "Productions",
            "Storages",
        ]
    ),


rule download_sci_grid:
    output:
        zipfile="<resources>/automatic/gas_grid.zip",
    log:
        "<logs>/automatic/download_sci_grid.log",
    localrule: True
    conda:
        "../envs/shell.yaml"
    params:
        url=internal["resources"]["automatic"]["scigrid_gas"],
    message:
        "Downloading gas infrastructure data from SciGRID_gas IGGIELGN."
    shell:
        """curl -sSLo {output} {params.url}"""


rule unzip_scigrid_dataset:
    input:
        zip_file=rules.download_sci_grid.output.zipfile,
    output:
        pipelines="<resources>/automatic/scigrid_gas/{scigrid_gas}.geojson",
    log:
        "<logs>/automatic/unzip_scigrid_dataset_{scigrid_gas}.log",
    conda:
        "../envs/euro-gas-grid.yaml"
    params:
        file=lambda wc: f"data/IGGIELGNC3_{wc.scigrid_gas}.geojson",
    message:
        "Unzipping SciGrid '{wildcards.scigrid_gas}'."
    script:
        "../scripts/unzip.py"


rule download_salt_cavern_storage:
    output:
        caverns="<resources>/automatic/salt_cavern_h2.parquet",
    log:
        "<logs>/automatic/download_salt_cavern_storage.log",
    localrule: True
    conda:
        "../envs/shell.yaml"
    params:
        url=internal["resources"]["automatic"]["salt_cavern_h2"],
    message:
        "Downloading H2 salt cavern storage dataset by Caglayan et al (2019)."
    shell:
        """curl -sSLo {output} {params.url}"""


rule download_natural_earth:
    output:
        zipfile="<resources>/automatic/{nat_earth}.zip",
    log:
        "<logs>/automatic/download_{nat_earth}.log",
    localrule: True
    conda:
        "../envs/shell.yaml"
    params:
        url=lambda wc: internal["resources"]["automatic"]["natural_earth"][wc.nat_earth],
    message:
        "Downloading '{wildcards.nat_earth}' from Natural Earth data (10m)."
    shell:
        """curl -sSLo {output} {params.url}"""
