"""Helper functions used by workflow rules."""


def scenario_config(wildcards):
    """Return the configuration for the requested scenario."""
    return config["scenarios"][wildcards.scenario]
