from .es_client import fetch_latest_event_record
from .ensembl_slurmdb_api.ensembl_slurm_client import EnsemblSlurmRestClient


__all__ = ['fetch_latest_event_record', 'EnsemblSlurmRestClient']