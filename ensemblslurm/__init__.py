__version__ = "1.0.0"

# Airflow picks up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "airflow-providers-ensemblslurm",
        "name": "EnsemblSlurm",
        "description": "Submit Job Via Slurm Rest API",
        "operators": [
            {"integration-name": "EnsemblSlurmBash", "python-modules": ["ensemblslurm.operators.ensembl_bash"]},
            {"integration-name": "EnsemblSlurmHive", "python-modules": ["ensemblslurm.operators.hive"]},
            {"integration-name": "EnsemblSlurmNextFlow", "python-modules": ["ensemblslurm.operators.nextflow"]},

        ],
        "task-decorators": [
            {"class-name": "ensemblslurm.decorators.slurm.ensemblslurm_task", "name": "ensemblslurm"}
        ],
    }

