"""Clustering rules."""


rule cluster_gas_network:
    input:
        pipelines=rules.prepare_pipelines.output.pipelines,
        nodes=rules.prepare_pipelines.output.nodes,
        shapes="<user_shapes>",
    output:
        hubs="<hubs>",
        pipelines="<pipelines>",
        nodes="<nodes>",
        fig=report(
            "<results>/{shapes}/pipelines.png",
            caption="../report/cluster_gas_network.rst",
            category="Euro gas grid module",
        ),
    log:
        "<logs>/{shapes}/cluster_gas_network.log",
    conda:
        "../envs/module.yaml"
    params:
        projected_crs=config["crs"]["projected"],
        replace_sovereign=config["clustering"]["pipelines"].get("replace_sovereign", {}),
    message:
        "Clustering and sectioning existing gas grid to {wildcards.shapes}."
    script:
        "../scripts/cluster_gas_network.py"


rule cluster_salt_cavern_h2_potential:
    input:
        salt_caverns=rules.download_salt_cavern_storage.output.caverns,
        shapes="<user_shapes>",
    output:
        salt_cavern_h2_potential="<salt_cavern_h2_potential>",
        fig=report(
            "<results>/{shapes}/salt_cavern_h2_potential.png",
            caption="../report/cluster_salt_cavern_h2_potential.rst",
            category="Euro gas grid module",
        ),
    log:
        "<logs>/{shapes}/cluster_salt_cavern_h2_potential.log",
    conda:
        "../envs/module.yaml"
    params:
        projected_crs=config["crs"]["projected"],
        min_gwh_tolerance=config["clustering"]["salt_caverns"]["min_gwh"],
    message:
        "Clustering of salt cavern H2 storage to {wildcards.shapes}."
    script:
        "../scripts/cluster_salt_cavern_h2_potential.py"


rule cluster_gas_storage:
    input:
        locations=rules.prepare_gas_storage.output.storage,
        shapes="<user_shapes>",
    output:
        capacities="<gas_storage>",
        fig=report(
            "<results>/{shapes}/gas_storage.png",
            caption="../report/cluster_gas_storage.rst",
            category="Euro gas grid module",
        ),
    log:
        "<logs>/{shapes}/cluster_gas_storage.log",
    conda:
        "../envs/module.yaml"
    params:
        projected_crs=config["crs"]["projected"],
        snap_storage_ids=config["clustering"]["gas_storage"]["snap_to_nearest_shape"],
    message:
        "Clustering existing gas storage to {wildcards.shapes}."
    script:
        "../scripts/cluster_gas_storage.py"
