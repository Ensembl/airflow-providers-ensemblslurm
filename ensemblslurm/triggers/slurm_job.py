import asyncio
import logging
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

TERMINAL_STATES = frozenset(
    {
        "FAILED",
        "COMPLETED",
        "CANCELLED",
        "TIMEOUT",
        "OUT_OF_MEMORY",
        "NODE_FAIL",
        "REVOKED",
        "SPECIAL_EXIT",
        "PREEMPTED",
    }
)


class SlurmJobTrigger(BaseTrigger):
    """
    Poll a SLURM job from the triggerer until it reaches a terminal state.

    The task defers once per SLURM job and is resumed exactly once, when the job
    finishes. This replaces re-deferring with a fresh TimeDeltaTrigger every
    check_interval, which created a new trigger row (and a full worker resume)
    on every poll.

    The SLURM JWT is deliberately not part of the serialized kwargs, so it is
    never written to the trigger table; it is read from SLURM_JWT in the
    triggerer's environment instead.
    """

    def __init__(
        self,
        job_id: int,
        job_name: str,
        poll_interval: int = 120,
        slurm_uri: Optional[str] = None,
        slurm_api_version: Optional[str] = None,
        slurm_user: Optional[str] = None,
        max_consecutive_errors: int = 5,
        max_unknown_polls: int = 15,
    ):
        super().__init__()
        self.job_id = job_id
        self.job_name = job_name
        self.poll_interval = poll_interval
        self.slurm_uri = slurm_uri
        self.slurm_api_version = slurm_api_version
        self.slurm_user = slurm_user
        self.max_consecutive_errors = max_consecutive_errors
        self.max_unknown_polls = max_unknown_polls

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "ensemblslurm.triggers.slurm_job.SlurmJobTrigger",
            {
                "job_id": self.job_id,
                "job_name": self.job_name,
                "poll_interval": self.poll_interval,
                "slurm_uri": self.slurm_uri,
                "slurm_api_version": self.slurm_api_version,
                "slurm_user": self.slurm_user,
                "max_consecutive_errors": self.max_consecutive_errors,
                "max_unknown_polls": self.max_unknown_polls,
            },
        )

    def _get_job_service(self):
        # Imported lazily: the operator module imports this trigger.
        from ensemblslurm.operators.ensembl_bash import (
            ConfigurationParser,
            SlurmClientFactory,
            SlurmConfigBuilder,
            SlurmJobService,
        )

        # Same env fallbacks as the operator (SLURM_URI / SLURM_API_VERSION / SLURM_USER / SLURM_JWT);
        # raises if SLURM_JWT is missing in the triggerer.
        config = SlurmConfigBuilder(ConfigurationParser()).build(
            slurm_uri=self.slurm_uri,
            slurm_api_version=self.slurm_api_version,
            slurm_user=self.slurm_user,
        )
        client = SlurmClientFactory.create_client(config, self.job_name)
        return SlurmJobService(client, self.poll_interval)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        job_service = self._get_job_service()
        consecutive_errors = 0
        consecutive_unknown = 0

        while True:
            try:
                # The client is blocking, so keep it off the triggerer's event loop.
                # max_retries=1 disables the client's 5-minute sleep on UNKNOWN: UNKNOWN
                # (e.g. job not yet visible in slurmdbd) is not terminal, so this loop
                # simply checks again after poll_interval without holding a thread.
                job_status = await asyncio.to_thread(
                    job_service.get_job_status_from_slurmdb, self.job_id, max_retries=1
                )
                consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                logging.warning(
                    f"Failed to fetch status for SLURM job {self.job_id} "
                    f"({consecutive_errors}/{self.max_consecutive_errors}): {e}"
                )
                if consecutive_errors >= self.max_consecutive_errors:
                    yield TriggerEvent(
                        {"job_id": self.job_id, "status": "ERROR", "message": str(e)}
                    )
                    return
                await asyncio.sleep(self.poll_interval)
                continue

            status = job_status.status
            if status in TERMINAL_STATES:
                logging.info(f"SLURM job {self.job_id} reached terminal state: {status}")
                yield TriggerEvent(
                    {
                        "job_id": self.job_id,
                        "status": status,
                        "reason": job_status.reason,
                        "exit_code": job_status.exit_code,
                    }
                )
                return

            # A job that stays UNKNOWN (e.g. purged from slurmdbd) would otherwise be polled forever.
            if status == "UNKNOWN":
                consecutive_unknown += 1
                if consecutive_unknown >= self.max_unknown_polls:
                    yield TriggerEvent(
                        {
                            "job_id": self.job_id,
                            "status": "ERROR",
                            "message": f"SLURM job {self.job_id} status UNKNOWN for {consecutive_unknown} polls "
                            f"(reason: {job_status.reason})",
                        }
                    )
                    return
            else:
                consecutive_unknown = 0

            logging.debug(f"SLURM job {self.job_id} status: {status}, polling again in {self.poll_interval}s")
            await asyncio.sleep(self.poll_interval)
