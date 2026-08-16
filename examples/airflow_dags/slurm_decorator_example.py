"""
Example DAG demonstrating the @ensemblslurm_task decorator.

This DAG shows how to use the @ensemblslurm_task decorator to run Python functions
as Slurm jobs, similar to how @docker_task works for Docker containers.
"""

from datetime import datetime
from airflow.sdk import DAG
from ensemblslurm.decorators import ensemblslurm_task
from ensemblslurm.operators import EnsemblBashOperator


# Create the DAG
with DAG(
    dag_id="slurm_decorator_example",
    start_date=datetime(2024, 1, 1),
    # schedule_interval=None,
    catchup=False,
    tags=["example", "slurm", "decorator"],
    doc_md=__doc__,
) as dag:

    bash_1 = EnsemblBashOperator(
        task_id="bash_1",
        bash_command="echo 'Hello airflow v3' > ~/slurm_provider.txt",
        use_nextflow=False,
        dag=dag,
    )

    bash_1