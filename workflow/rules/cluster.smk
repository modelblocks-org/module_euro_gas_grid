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
            "<results>/{shapes}/{scenario}/pipelines.png",
            caption="../report/cluster_gas_network.rst",
            category="European Gas Grid",
        ),
    log:
        "<logs>/{shapes}/{scenario}/cluster_gas_network.log",
    conda:
        "../envs/module.yaml"
    params:
        projected_crs=lambda wc: scenario_config(wc)["crs"]["projected"],
        replace_sovereign=lambda wc: scenario_config(wc)["clustering"][
            "pipelines"
        ].get("replace_sovereign_id", {}),
    message:
        "Clustering and sectioning existing gas grid to {wildcards.shapes} for scenario {wildcards.scenario}."
    script:
        "../scripts/cluster_gas_network.py"


rule cluster_salt_cavern_h2_potential:
    input:
        salt_caverns=rules.download_salt_cavern_storage.output.caverns,
        shapes="<user_shapes>",
    output:
        salt_cavern_h2_potential="<salt_cavern_h2_potential>",
        fig=report(
            "<results>/{shapes}/{scenario}/salt_cavern_h2_potential.png",
            caption="../report/cluster_salt_cavern_h2_potential.rst",
            category="European Gas Grid",
        ),
    log:
        "<logs>/{shapes}/{scenario}/cluster_salt_cavern_h2_potential.log",
    conda:
        "../envs/module.yaml"
    params:
        projected_crs=lambda wc: scenario_config(wc)["crs"]["projected"],
        min_gwh_tolerance=lambda wc: scenario_config(wc)["clustering"]["salt_caverns"][
            "min_gwh"
        ],
    message:
        "Clustering salt cavern H2 storage to {wildcards.shapes} for scenario {wildcards.scenario}."
    script:
        "../scripts/cluster_salt_cavern_h2_potential.py"


rule cluster_gas_storage:
    input:
        locations=rules.prepare_gas_storage.output.storage,
        shapes="<user_shapes>",
    output:
        capacities="<gas_storage>",
        fig=report(
            "<results>/{shapes}/{scenario}/gas_storage.png",
            caption="../report/cluster_gas_storage.rst",
            category="European Gas Grid",
        ),
    log:
        "<logs>/{shapes}/{scenario}/cluster_gas_storage.log",
    conda:
        "../envs/module.yaml"
    params:
        projected_crs=lambda wc: scenario_config(wc)["crs"]["projected"],
        snap_storage_ids=lambda wc: scenario_config(wc)["clustering"]["gas_storage"][
            "snap_storage_id_to_nearest_shape"
        ],
    message:
        "Clustering existing gas storage to {wildcards.shapes} for scenario {wildcards.scenario}."
    script:
        "../scripts/cluster_gas_storage.py"
