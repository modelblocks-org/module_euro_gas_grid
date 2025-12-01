rule visualize_output:
    message: "generting plots for pipe clusters and salt cavern potentials {wildcards.shapes} resolution"
    input:
        regions = "resources/user/{shapes}/shapes.geojson",
        salt_cavern = rules.cluster_salt_cavern_potentials.output.clusters,
        salt_cavern_potential = rules.cluster_salt_cavern_potentials.input.salt_cavern_potentials,
        pipe_clusters = rules.cluster_gas_network.output.clusters,
    output:
        salt_cavern = "results/figs/{shapes}/salt_cavern.svg",
        pipe_clusters = "results/figs/{shapes}/pipes.svg",
    params:
        plot_configs = config["plots"]
    conda: "../envs/plots.yaml"
    script: "../scripts/plots.py"
