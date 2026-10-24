"""
Find and cancel SLURM "child" jobs submitted from inside an Airflow-launched SLURM job.

An Airflow task submits one head job named <job_name> (see ConfigurationParser.parse_job_name).
Work it fans out to SLURM as separate jobs - eHive workers (pipeline_name is the job name) and
inner Nextflow tasks (executor.jobName) - is named "<job_name>-...". SLURM does not cancel those
when the head job ends, so they are cleaned up explicitly by name.

The "-" separator matters: plain startswith(job_name) would treat the jobs of
..._38_10 .. ..._38_19 as children of ..._38_1.
"""

import logging
import re
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Sequence

from pyslurmutils.client.rest.api import slurm_response

ACTIVE_STATES = frozenset(
    {"PENDING", "RUNNING", "CONFIGURING", "SUSPENDED", "REQUEUED", "REQUEUE_HOLD", "RESIZING", "STOPPED"}
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SlurmJobRef:
    job_id: int
    name: str
    state: str


def is_child_job_name(name: str, parent_name: str) -> bool:
    """True if a job named `name` was submitted on behalf of the head job `parent_name`."""
    return bool(parent_name) and name.startswith(f"{parent_name}-")


def list_active_jobs(client) -> List[SlurmJobRef]:
    """Active (not yet finished) jobs of the client's SLURM user."""
    jobs = []
    for job in client.get_all_job_properties(all_users=False):
        state = slurm_response.slurm_job_state(job)
        if state in ACTIVE_STATES:
            jobs.append(SlurmJobRef(job_id=int(job.job_id), name=job.name, state=state))
    return jobs


def find_child_jobs(active_jobs: Iterable[SlurmJobRef], parent_name: str) -> List[SlurmJobRef]:
    return [job for job in active_jobs if is_child_job_name(job.name, parent_name)]


def find_orphaned_child_jobs(
    active_jobs: Sequence[SlurmJobRef],
    parent_name_patterns: Sequence[str],
    head_job_exists: Callable[[str], bool],
) -> List[SlurmJobRef]:
    """
    Active child jobs whose head job is no longer active.

    A job "<parent>-..." is an orphan only if:
      - <parent> matches one of `parent_name_patterns` (regexes for Airflow job names, e.g.
        "^mvp_"), so jobs the same SLURM user runs outside Airflow are never touched;
      - a SLURM job named exactly <parent> really existed (`head_job_exists`, e.g. a slurmdb
        lookup). Names alone cannot tell heads from children, because Airflow job names may
        contain "-": without this check the head "mvp_x_track-load_r1" would look like an
        orphaned child of a non-existent "mvp_x_track" and be cancelled;
      - no job named <parent> is still active.
    """
    patterns = [re.compile(p) for p in parent_name_patterns]
    active_names = {job.name for job in active_jobs}
    exists_cache: Dict[str, bool] = {}

    def is_real_head(name: str) -> bool:
        if name not in exists_cache:
            exists_cache[name] = head_job_exists(name)
        return exists_cache[name]

    orphans = []
    for job in active_jobs:
        # Every "-" is a candidate split point, since Airflow job names may contain "-" too.
        candidates = [job.name[:i] for i, ch in enumerate(job.name) if ch == "-"]
        airflow_parents = [p for p in candidates if any(rx.search(p) for rx in patterns)]
        if not airflow_parents or any(p in active_names for p in airflow_parents):
            continue
        if any(is_real_head(p) for p in airflow_parents):
            orphans.append(job)
    return orphans


def slurmdb_job_name_exists(client, job_name: str, lookback_days: int = 30) -> bool:
    """True if SLURM accounting has a job named exactly `job_name` started in the last `lookback_days`."""
    response = client.get(
        f"/slurmdb/{client._api_version_str}/jobs",
        request_options={"query_params": {"job_name": job_name, "start_time": f"now-{lookback_days}days"}},
        raise_on_error=True,
    )
    jobs = getattr(response, "jobs", None) or []
    return any(job.name == job_name for job in jobs)


def cancel_jobs(client, jobs: Iterable[SlurmJobRef], dry_run: bool = False) -> List[SlurmJobRef]:
    """Cancel `jobs`, continuing past individual failures. Returns the jobs actually cancelled."""
    cancelled = []
    for job in jobs:
        if dry_run:
            logger.info(f"[dry-run] Would cancel SLURM job {job.job_id} ({job.name}, {job.state})")
            continue
        try:
            client.cancel_job(job.job_id)
            logger.info(f"Cancelled SLURM job {job.job_id} ({job.name}, {job.state})")
            cancelled.append(job)
        except Exception as e:
            logger.error(f"Failed to cancel SLURM job {job.job_id} ({job.name}): {e}")
    return cancelled


def cancel_child_jobs(client, parent_name: str, active_jobs: Optional[Sequence[SlurmJobRef]] = None) -> List[SlurmJobRef]:
    """Cancel every active child job of the head job `parent_name`."""
    if active_jobs is None:
        active_jobs = list_active_jobs(client)
    return cancel_jobs(client, find_child_jobs(active_jobs, parent_name))
