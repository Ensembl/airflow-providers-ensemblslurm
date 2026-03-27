import os
from airflow import DAG
from ensemblslurm.utils.task_group_builder import build_task_group


BASE_DIR = os.path.dirname(__file__)

with (DAG(
        "MVP_PROCESSING_TEST",
        catchup=False,
        description="Ensembl MVP Processing",
        max_active_tasks=3, # number of tasks that can run concurrently
        max_active_runs=3,   # only three DAG run at a time
        schedule=None,
        tags=[
            "corestats",
            "checksum",
            "xref",
            "protein_features",
            "blast_dumps",
            "thoas_dumps",
            "homology_annotation",
            "ftp_dumps",
            "release",
            "validation",
            "resource_loader",
            "resource_dumper"
        ],
        default_args={
            "owner": "airflow",
            "depends_on_past": False,
            "email": "ensembl-production@ebi.ac.uk",
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 0,
            "trigger_rule": "none_failed_min_one_success",
            "max_active_runs": 3
        }
) as dag):
    VALIDATION = build_task_group("VALIDATION", BASE_DIR, dag)
    CORE_DB_LOAD = build_task_group("CORE_DB_LOAD", BASE_DIR, dag)
    CORE_DB_DUMPS_FOR_BLAST_REFGET = build_task_group("CORE_DB_DUMPS_FOR_BLAST_REFGET", BASE_DIR, dag)

    # Declaring TaskGroup dependencies
    VALIDATION >> CORE_DB_LOAD >> CORE_DB_DUMPS_FOR_BLAST_REFGET