# ---
# python_callable: check_dag_config_keys
# dependencies:
#   - some_task
# ---

def check_dag_config_keys(**kwargs):
    """
    Check if the DAG configuration contains all required keys.

    Args:
        **kwargs: Keyword arguments containing the DAG configuration.
    """
    import logging

    logging.info("Checking DAG configuration keys...")
    logging.info("kwargs: %s", kwargs)
    logging.info("dag_run conf: %s", kwargs['dag_run'].conf)

    dag_config = kwargs['dag_run'].conf

    required_keys = ["release_id", "release_version", "release_name", "user", "genome_uuid"]
    missing_keys = [key for key in required_keys if key not in dag_config]

    if missing_keys:
        raise ValueError("DAG configuration is missing required keys: " + ", ".join(missing_keys))

    if not len(dag_config['genome_uuid']):
        raise ValueError("genome_uuid in DAG configuration is empty. Please provide a valid genome UUID.")

    if 'species' in dag_config and not len(dag_config['species']):
        raise ValueError("species in DAG configuration is empty. Please provide a valid species.")

    return True
