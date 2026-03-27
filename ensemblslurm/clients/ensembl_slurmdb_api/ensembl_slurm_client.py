from pyslurmutils.client import SlurmScriptRestClient
from pyslurmutils.client import utils
from pydantic import BaseModel
from typing import Optional
import time


class SlurmJobStatus(BaseModel):
    status: str
    reason: Optional[str] = None
    exit_code: Optional[int] = None


class EnsemblSlurmRestClient(SlurmScriptRestClient):
    """
    Represents a REST client interface for interacting with Slurm job properties
    and statuses via the Ensembl Slurm Database.

    This class is designed to inherit from the SlurmScriptRestClient and provides
    methods to query and manage job-specific data from the Slurm workload manager,
    primarily for purposes such as querying the database for job properties or
    to check the status of jobs based on provided job IDs.
    """

    def get_job_properties_from_slurmdb(
        self, job_id: int, raise_on_error=True, request_options: Optional[dict] = None
    ) -> Optional[BaseModel]:
        """
        Retrieves job properties from the Slurm database for a given job ID.

        Args:
            job_id (int): The ID of the job to retrieve properties for.
            raise_on_error (bool, optional): Whether to raise an exception on error. Defaults to True.
            request_options (dict, optional): Additional request options. Defaults to None.

        Returns:
            Optional[BaseModel]: The job properties if successful, otherwise None.
        """
        request_options = utils.merge_mappings(
            request_options, {"path_params": {"job_id": job_id}}
        )


        response = self.get(
            f"/slurmdb/{self._api_version_str}/job/{{job_id}}",
            request_options=request_options,
            raise_on_error=raise_on_error,
         )
        return response

    def get_job_status_from_slurmdb(
            self,
            job_id: int,
            raise_on_error: bool = True,
            request_options: Optional[dict] = None,
    ) -> Optional[SlurmJobStatus]:
        """
        Retrieves the status of a job from the Slurm database with retry logic.

        If the job status is UNKNOWN, retries up to 3 times with a 5-minute delay
        between each attempt.

        Args:
            job_id (int): The ID of the job to retrieve status for.
            raise_on_error (bool, optional): Whether to raise an exception on error. Defaults to True.
            request_options (dict, optional): Additional request options. Defaults to None.

        Returns:
            Optional[SlurmJobStatus]: The job status if successful, otherwise None.
        """
        max_retries = 3
        retry_delay = 300  # 5 minutes in seconds
        job_status = None

        for attempt in range(max_retries):
            response = self.get_job_properties_from_slurmdb(
                job_id,
                raise_on_error=raise_on_error,
                request_options=request_options,
            )

            if not response or not getattr(response, "jobs", None):
                job_status = SlurmJobStatus(
                    status="UNKNOWN",
                    reason="JOB_NOT_FOUND",
                    exit_code=1,
                )
            else:
                job = response.jobs[0]

                state = job.state.current[0] if job.state and job.state.current else None
                reason = job.state.reason if job.state else None
                exit_code = None

                if job.exit_code and job.exit_code.return_code:
                    exit_code = job.exit_code.return_code.number

                job_status = SlurmJobStatus(
                    status=state,
                    reason=reason,
                    exit_code=exit_code,
                )

            # If status is not UNKNOWN, return immediately
            if job_status.status != "UNKNOWN":
                return job_status

            # If this is not the last attempt and status is UNKNOWN, retry after delay
            if attempt < max_retries - 1:
                print(f"Job {job_id} status is UNKNOWN. Retrying in 5 minutes... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                # Last attempt, return the UNKNOWN status
                print(f"Job {job_id} status remains UNKNOWN after {max_retries} attempts.")

        return job_status

