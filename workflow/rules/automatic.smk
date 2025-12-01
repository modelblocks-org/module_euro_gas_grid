"""Rules to used to download automatic resource files."""

rule download_sci_grid:
    message:
        "Downloading gas infrastructure data from SciGRID_gas IGGIELGN."
    params:
        url = internal["resources"]["automatic"]["scigrid_gas"]
    log:
        "logs/download_sci_grid.log",
    output:
        zipfile=temp("resources/automatic/gas_grid.zip"),
    localrule: True,
    conda:
        "../envs/shell.yaml"
    shell:
        """curl -sSLo {output} {params.url}"""

rule download_landmass:
    message:
        "Downloading landmass from Natural Earth data (10m)."
    params:
        url = internal["resources"]["automatic"]["landmass"]
    log:
        "logs/download_landmass.log",
    output:
        zipfile=temp("resources/automatic/landmass.zip"),
    localrule: True,
    conda:
        "../envs/shell.yaml"
    shell:
        """curl -sSLo {output} {params.url}"""

rule unzip_pipe_segements:
    message:
        "Unzipping SciGrid '{params.file}'."
    params:
        file=f"data/IGGIELGN_PipeSegments.geojson",
    input:
        zip_file=rules.download_sci_grid.output.zipfile,
    output:
        pipelines=temp("resources/automatic/pipesegments.geojson"),
    log:
        "logs/automatic/unzip_pipe_segements.log",
    conda:
        "../envs/clustering.yaml"
    script:
        "../scripts/unzip.py"

rule unzip_landmass:
    message:
        "Unzipping landmass file."
    input:
        zip_file=rules.download_landmass.output.zipfile,
    output:
        folder=temp(directory("resources/automatic/landmass/")),
    log:
        "logs/automatic/unzip_landmass.log",
    conda:
        "../envs/clustering.yaml"
    script:
        "../scripts/unzip.py"
