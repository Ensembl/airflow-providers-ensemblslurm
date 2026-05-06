import logging
import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, Sequence
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger
from airflow.sdk import Variable
from airflow.sdk.bases.hook import BaseHook
from airflow.utils.context import Context
from airflow.utils.session import provide_session
from airflow.utils.state import State
from ensemblslurm.clients import EnsemblSlurmRestClient
from ensemblslurm.clients.ensembl_slurmdb_api.ensembl_slurm_client import SlurmJobStatus
from ensemblslurm.clients.es_client import fetch_latest_event_record
from ensemblslurm.hooks.ensembl_slack import EnsemblSlackNotifier


class JobStatus(str, Enum):
    """SLURM job status enumeration."""

    FAILED = "FAILED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    SKIPPED = "SKIPPED"


@dataclass
class SlurmConfig:
    """Configuration for SLURM connection and job parameters."""

    url: str = "https://codon-slurm-restd.ebi.ac.uk"
    api_version: str = "0.0.42"
    user: str = "ens2020"
    token: Optional[str] = None
    cwd: Optional[str] = None
    time_limit: int = 1440  # minutes
    memory_mb: int = 2048
    partition: Optional[str] = None
    log_directory: str = "airflow_logs"
    env: Dict[str, str] = field(default_factory=dict)


@dataclass
class NotificationConfig:
    """Configuration for notification services."""

    slack_conn_id: str = "airflow-slack-notification"
    web_log_uri: str = "https://services.airflow.ensembl-production.ebi.ac.uk/nextflow_weblog/receive"
    nf_script_path: Optional[str] = None
    required_log_conn_id: List[str] = field(default_factory=lambda: ["airflow_log", "es_log"])
    log_conn_id: List[str] = field(default_factory=lambda: ["airflow_log", "es_log"])


@dataclass
class JobInfo:
    """Information about a submitted job."""

    job_id: int
    job_name: str
    bash_command: str
    status: Optional[str] = None


class IJobSubmitter(Protocol):
    """Interface for job submission services."""

    def submit_job(self, command: str, job_name: str) -> str:
        """Submit a job and return job ID."""
        ...

    def get_job_status(self, job_id: str) -> str:
        """Get current status of a job."""
        ...

    def wait_for_job(self, job_id: str, period: int) -> str:
        """Wait for job completion and return final status."""
        ...


class INotifier(Protocol):
    """Interface for notification services."""

    def send_notification(self, message: str, context: Context, status: str = "failure") -> None:
        """Send a notification."""
        ...

    def prepare_notification_message(self, context: Context, job_info: JobInfo) -> str:
        """Prepare notification message from context and job info."""
        ...


class ICommandBuilder(Protocol):
    """Interface for command building services."""

    def build_command(self, base_command: str, job_name: str, context: Context) -> str:
        """Build the final command to execute."""
        ...


class ConfigurationParser:
    """Parses and validates configuration values."""

    @staticmethod
    def parse_memory(mem_str: str) -> int:
        """
        Convert human-readable memory string (e.g. "2GB", "512MB") to megabytes.

        Args:
            mem_str: Memory string with unit (KB, MB, GB, TB)

        Returns:
            Memory in megabytes
        """
        units = {"KB": 1 / 1024, "MB": 1, "GB": 1024, "TB": 1024 * 1024}
        mem_str = mem_str.upper().strip()
        received_unit = mem_str[-2:]

        if received_unit in units:
            for unit in units:
                if mem_str.endswith(unit):
                    try:
                        value = int(float(mem_str[:-2]) * units[unit])
                        # Zero is allowed; negative is invalid
                        if value < 0:
                            return 2048
                        return value
                    except Exception:
                        return 2048

        # Default: 2GB
        return 2048

    @staticmethod
    def parse_time(time_str: str = "1D") -> int:
        """
        Convert human-readable time string (e.g. "1D", "2H", "500M") to minutes.

        Args:
            time_str: Time string with unit (D, H, M)

        Returns:
            Time in minutes
        """
        time_str = time_str.upper().strip()

        # Validate format
        if (
            len(time_str) < 2
            or not time_str[:-1].replace(".", "", 1).isdigit()
            or time_str[-1] not in ["D", "H", "M"]
        ):
            time_str = "1D"

        number = float(time_str[:-1])
        unit = time_str[-1]

        time_units = {"D": 1440, "H": 60, "M": 1}
        minutes = number * time_units[unit]

        # Apply max limit: 7 days
        max_minutes = 7 * 1440
        if minutes > max_minutes:
            minutes = 1440  # Default to 1 day

        return int(minutes)

    @staticmethod
    def parse_job_name(context: Context, custom_name: str = "") -> str:
        """
        Generate a valid job name from context or custom name.

        Args:
            context: Airflow context
            custom_name: Optional custom job name

        Returns:
            Valid job name

        Raises:
            AirflowException: If the job name is invalid
        """
        if custom_name:
            return custom_name

        task_instance = context["ti"]
        dag_run = context["dag_run"]

        # Build job name from components
        task_id = task_instance.task_id.rsplit(".", 1)[-1]  # remove task group name from task id

        # Remove time-related special characters from run_id (ISO 8601 format)
        run_id = re.sub(r"[-:.+]", "", dag_run.run_id)

        job_name = f"{dag_run.dag_id}_{task_id}_{run_id}".lower()

        # Check length before other validations
        if len(job_name) > 80:
            raise AirflowException(f"Job name exceeds max length of 80 characters")

        # Validate job name format: must start with lowercase letter and only contain valid chars
        if not re.match("^[a-z][a-z0-9_-]*$", job_name):
            raise AirflowException(
                f"Job name {job_name} is not valid. "
                f"Must start with lowercase letter, contain only lowercase letters, digits, hyphens, "
                f"and underscores, with max length 80."
            )

        return job_name


class SlurmConfigBuilder:
    """Builds SlurmConfig from parameters and environment."""

    def __init__(self, parser: ConfigurationParser):
        self.parser = parser

    def build(
        self,
        slurm_uri: Optional[str] = None,
        slurm_api_version: Optional[str] = None,
        slurm_user: Optional[str] = None,
        slurm_jwt: Optional[str] = None,
        cwd: Optional[str] = None,
        partition: Optional[str] = None,
        time_limit: str = "7D",
        memory_per_node: str = "2GB",
        log_directory: str = "airflow_logs",
    ) -> SlurmConfig:
        """
        Build SlurmConfig from parameters with environment fallbacks.

        Args:
            slurm_uri: SLURM REST API URL
            slurm_api_version: SLURM API version
            slurm_user: SLURM username
            slurm_jwt: JWT token
            cwd: Working directory
            partition: SLURM partition
            time_limit: Job time limit
            memory_per_node: Memory per node
            log_directory: Log directory path

        Returns:
            Configured SlurmConfig instance

        Raises:
            AirflowException: If required parameters are missing
        """
        uri = slurm_uri or os.getenv("SLURM_URI", "https://codon-slurm-restd.ebi.ac.uk")
        api_version = slurm_api_version or os.getenv("SLURM_API_VERSION", "0.0.42")
        user = slurm_user or os.getenv("SLURM_USER", "ens2020")
        token = slurm_jwt or os.getenv("SLURM_JWT")

        if not all([uri, api_version, user, token]):
            raise AirflowException("Missing required SLURM configuration parameters")

        cwd_val = cwd or f"/hps/nobackup/flicek/ensembl/production/ensprod/ensembl_airflow/{user}/"
        time_limit_minutes = self.parser.parse_time(time_limit)
        memory_mb = self.parser.parse_memory(memory_per_node)

        env = {
            "HOME": f"/homes/{user}",
            "MODENV_ROOT": f"/homes/{user}/lib/ensembl-mod-env",
            "MODULEPATH_ROOT": f"/homes/{user}/modules",
            "PATH": "/bin/:/usr/bin/:/sbin/",
            "NXF_WORK": cwd_val,
            "NXF_PLUGINS_DIR": f"/homes/{user}/.nextflow/plugins/",
        }

        return SlurmConfig(
            url=uri,
            api_version=api_version,
            user=user,
            token=token,
            cwd=cwd_val,
            time_limit=time_limit_minutes,
            memory_mb=memory_mb,
            log_directory=log_directory,
            partition=partition,
            env=env,
        )


class NotificationConfigBuilder:
    """Builds NotificationConfig from parameters and environment."""

    @staticmethod
    def build(
        slack_conn_id: str = "airflow-slack-notification",
        web_log_uri: Optional[str] = None,
        nf_hive_script_path: Optional[str] = None,
        required_log_conn_id: Optional[List[str]] = None,
        log_conn_id: Optional[List[str]] = None,
    ) -> NotificationConfig:
        """
        Build NotificationConfig from parameters with environment fallbacks.

        Args:
            slack_conn_id: Slack connection ID
            web_log_uri: Web log URI
            nf_hive_script_path: Nextflow script path
            required_log_conn_id: Required log connection IDs
            log_conn_id: Log connection IDs

        Returns:
            Configured NotificationConfig instance
        """
        web_uri = web_log_uri or os.getenv(
            "NEXTFLOW_WEB_LOG_URI",
            "https://services.airflow.ensembl-production.ebi.ac.uk/nextflow_weblog/receive",
        )
        nf_path = nf_hive_script_path or os.getenv(
            "NF_HIVE_SCRIPT_PATH",
            f"/homes/{os.getenv('SLURM_USER', 'ens2020')}/dispatcher/main.nf",
        )

        return NotificationConfig(
            slack_conn_id=slack_conn_id,
            web_log_uri=web_uri,
            nf_script_path=nf_path,
            required_log_conn_id=required_log_conn_id or ["airflow_log", "es_log"],
            log_conn_id=log_conn_id or ["airflow_log", "es_log"],
        )


class SlurmJobService:
    """Service for SLURM job submission and monitoring."""

    def __init__(self, client: EnsemblSlurmRestClient, check_interval: int = 120):
        """
        Initialize SLURM job service.

        Args:
            client: SLURM REST API client
            check_interval: Interval in seconds to check job status
        """
        self.client = client
        self.check_interval = check_interval
        self.logger = logging.getLogger(self.__class__.__name__)

    def submit_job(self, command: str, job_name: str) -> int:
        """
        Submit a job to SLURM.

        Args:
            command: Bash command to execute
            job_name: Name of the job

        Returns:
            Job ID

        Raises:
            AirflowException: If submission fails
        """
        try:
            self.logger.info(f"Submitting job '{job_name}' to SLURM")
            self.client._parameters["name"] = job_name
            job_id = self.client.submit_script(command)
            self.logger.info(f"Successfully submitted job '{job_name}' with ID: {job_id}")
            return job_id
        except Exception as e:
            self.logger.error(f"Failed to submit job '{job_name}': {str(e)}")
            raise AirflowException(f"SLURM job submission failed: {str(e)}")

    def get_job_status(self, job_id: str) -> str:
        """
        Get current status of a job.

        Args:
            job_id: Job ID

        Returns:
            Job status string
        """
        try:
            status = self.client.get_status(job_id)
            self.logger.debug(f"Job {job_id} status: {status}")
            return status
        except Exception as e:
            self.logger.error(f"Failed to get status for job {job_id}: {str(e)}")
            raise AirflowException(f"Failed to get job status: {str(e)}")

    def wait_for_job(self, job_id: int, period: int = 30) -> str:
        """
        Wait for job completion.

        Args:
            job_id: Job ID
            period: Polling period in seconds

        Returns:
            Final job status
        """
        try:
            self.logger.info(f"Waiting for job {job_id} to complete (polling every {period}s)")
            self.client.wait_finished(job_id, period=period)
            status = self.client.get_status(job_id)
            self.logger.info(f"Job {job_id} completed with status: {status}")
            return status
        except Exception as e:
            self.logger.error(f"Error while waiting for job {job_id}: {str(e)}")
            raise AirflowException(f"Job wait failed: {str(e)}")

    def get_job_properties(self, job_id: str) -> Any:
        """
        Get detailed job properties from SLURM database.

        Args:
            job_id: Job ID

        Returns:
            Job properties object
        """
        try:
            response = self.client.get_job_properties_from_slurmdb(job_id)
            if response is None:
                raise AirflowException(f"Job {job_id} not found in SLURM DB")
            return response
        except Exception as e:
            self.logger.error(f"Failed to get properties for job {job_id}: {str(e)}")
            raise

    def get_job_status_from_slurmdb(self, job_id: int) -> SlurmJobStatus:
        """
        Get detailed job properties from SLURM database.

        Args:
            job_id: Job ID

        Returns:
            Job properties object
        """
        try:
            response = self.client.get_job_status_from_slurmdb(job_id)
            if response is None:
                raise AirflowException(f"Job {job_id} not found in SLURM DB")
            return response
        except Exception as e:
            self.logger.error(f"Failed to get properties for job {job_id}: {str(e)}")
            raise

    def get_job_status_and_id_by_name(self, job_name: str) -> Optional[tuple[int, str]] | None:
        """
        Get job status by job name.

        Args:
            job_name: Job name

        Returns:
            Job status if found, otherwise None
        """
        try:
            jobs = self.client.get_all_job_properties(all_users=False)
            for job in jobs:
                if job.name == job_name:
                    return job.job_id, self.get_job_status(job.job_id)  # Log current status
            self.logger.warning(f"No job found with name: {job_name}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to get job status by name {job_name}: {str(e)}")
            raise AirflowException(f"Failed to get job status by name: {str(e)}")


class NextflowCommandBuilder:
    """Builds Nextflow commands for execution."""

    @staticmethod
    def build_command(
        base_command: str,
        job_name: str,
        context: Context,
        work_dir: str,
        web_log_uri: str,
        nf_script_path: str,
        nf_plugin_version: str = "ens-nf-weblog@2.10.2",
    ) -> str:
        """
        Build a Nextflow command wrapper around base command.

        Args:
            base_command: Base bash command to wrap
            job_name: Job name
            context: Airflow context
            work_dir: Working directory
            web_log_uri: Web log URI
            nf_script_path: Path to Nextflow script
            nf_plugin_version: Nextflow plugin version

        Returns:
            Complete Nextflow command string
        """
        task_try_number = context["ti"].try_number
        bash_cmd_file = os.path.join(work_dir, f".{job_name}.sh")
        name = f"{job_name}_{task_try_number}"

        # Indent command
        indented_command = "\n".join("\t\t" + line for line in base_command.rstrip().splitlines())
        # Todo: Nextflow Notification system :
        """
        -plugins "{nf_plugin_version}" \\
        -with-weblog "{web_log_uri}" \\
        """
        job_script = f"""

    mkdir -p "{work_dir}"
    export NXF_WORK="{work_dir}"

    # Write the bash command file
    cat > "{bash_cmd_file}" <<'EOCMD'

        {indented_command}

EOCMD

    cd "{work_dir}"
    module load production/mvp114 rel_env
    nextflow run "{nf_script_path}" \\
      --run_mode bash \\
      --cmd "bash {bash_cmd_file}" \\
      -work-dir "{work_dir}" \\
      -name "{name.lower()}" \\
      -resume
    """
        return job_script


class SlackNotificationService:
    """Service for sending Slack notifications."""

    def __init__(self, slack_conn_id: str, slurm_config: SlurmConfig):
        """
        Initialize notification service.

        Args:
            slack_conn_id: Slack connection ID
            slurm_config: SLURM configuration for log paths
        """
        self.slack_conn_id = slack_conn_id
        self.slurm_config = slurm_config
        self.logger = logging.getLogger(self.__class__.__name__)

    def send_notification(self, message: str, context: Context, status: str = "failure") -> None:
        """
        Send a Slack notification.

        Args:
            message: Message to send
            context: Airflow context
            status: Status type (success/failure)
        """
        try:
            slack_notifier = EnsemblSlackNotifier(conn_id=self.slack_conn_id, context=context)
            env = Variable.get("environment", default="dev")
            block_msg = slack_notifier.format_message(context=context, error_msg=message, status=status)
            slack_notifier.post_message(message=f"{env} Ensembl Pipeline Status", block=block_msg)
            self.logger.info("Slack notification sent successfully")
        except Exception as e:
            self.logger.error(f"Failed to send Slack notification: {str(e)}")
            # Don't raise - notification failure shouldn't break pipeline

    def prepare_notification_message(self, context: Context, job_info: JobInfo) -> str:
        """
        Prepare notification message from job information.

        Args:
            context: Airflow context
            job_info: Job information

        Returns:
            Formatted message string
        """
        try:
            dag_run = context["dag_run"]
            dag_run_conf = dag_run.conf
            species_list = dag_run_conf.get("species", [])
            genome_uuids_list = dag_run_conf.get("genome_uuid", [])
            env = Variable.get("environment", default="dev")
            base_url = Variable.get("base_url", default="http://localhost:8080/")
            log_url = f"{base_url}dags/{context['ti'].dag_id}/grid?dag_run_id={context['ti'].run_id}&task_id={context['ti'].task_id}&tab=logs"

            # Check if skipped
            if context["ti"].task_id in dag_run_conf.get("skip_pipeline", []):
                return f"""
                ({env}) ✅ Pipeline/Job {self.slurm_config.user}_{job_info.job_name} Skipped
                `SpeciesList: {",".join(species_list)}`
                `GenomeUuids: {",".join(genome_uuids_list)}`
                Airflow Log: {log_url}
                """

            # Fetch ES record
            record = self._fetch_es_record(job_info.job_name, context["ti"].try_number)

            if not record:
                return f"""
                Error: No ES record found for job {job_info.job_name}
                Check codon airflow log: {os.path.join(self.slurm_config.cwd, self.slurm_config.log_directory)}
                """

            return self._format_message_from_record(
                record, job_info, species_list, genome_uuids_list, env, context
            )

        except Exception as e:
            self.logger.error(f"Error preparing notification message: {str(e)}")
            return f"Error preparing notification: {str(e)}"

    def _fetch_es_record(self, job_name: str, task_try_number: int) -> Optional[Dict]:
        """Fetch latest event record from Elasticsearch."""
        try:
            conn = BaseHook.get_connection("es_info")
            full_job_name = f"{job_name}_{task_try_number}"

            record = fetch_latest_event_record(
                conn.host,
                conn.port,
                conn.login,
                conn.password,
                "nextflow_logs",
                run_name=full_job_name,
                event_status=["completed"],
            )
            return record
        except Exception as e:
            self.logger.error(f"Failed to fetch ES record: {str(e)}")
            return None

    def _format_message_from_record(
        self,
        record: Dict,
        job_info: JobInfo,
        species_list: List[str],
        genome_uuids_list: List[str],
        env: str,
        context: Context,
    ) -> str:
        """Format message from ES record."""
        workflow = record.get("metadata", {}).get("workflow", {})
        process_list = workflow.get("stats", {}).get("processes", [])
        es_status = workflow.get("success")

        result = "\n".join(
            f"{item['name']} | {item['hash']} | Succeeded: {item['succeeded']}" for item in process_list
        )

        hive_gui = f"http://guihive.ebi.ac.uk:8080/versions/97/?driver=mysql&username=ensro&host=mysql-ens-hive-prod-1&port=4575&dbname={self.slurm_config.user}_{job_info.job_name.lower()}"

        if es_status and job_info.status == "COMPLETED":
            return f"""
            ({env}) ✅ Pipeline {self.slurm_config.user}_{job_info.job_name} completed successfully
            {hive_gui if context['ti'].task.task_type == 'HiveNextflowOperator' else ""}
            *Nextflow Details*:
            *projectDir*: `{workflow.get("projectDir")}`
            *workDir*: `{workflow.get("workDir")}`
                ```
                {result}
                ```
            *SpeciesList*: `{",".join(species_list)}`
            *GenomeUuids*: `{",".join(genome_uuids_list)}`
            """
        else:
            return f"""
            ({env}) ❌ Task `{self.slurm_config.user}_{job_info.job_name}` `{job_info.status}`:
            `workDir: {workflow.get("workDir")}`
                ```
                {result}
                ```
            *Error Msg*: {workflow.get("errorMessage")}
            *Error Report*: {workflow.get("errorReport")}
            *SpeciesList*: `{",".join(species_list)}`
            *GenomeUuids*: `{",".join(genome_uuids_list)}`
            """


class AirflowExceptionWithSlackNotification(AirflowException):
    """Raised when task fails and sends Slack notification."""

    def __init__(self, message: str, context: Context, slack_conn_id: str, status: str = "failure") -> None:
        """
        Initialize exception and send notification.

        Args:
            message: Error message
            context: Airflow context
            slack_conn_id: Slack connection ID
            status: Status type
        """
        super().__init__(message)
        self.message = message

        try:
            logging.info(f"************************8Slack Notification {message}.......................")
            ti = context.get("task_instance")
            dag_run = context.get("dag_run")
            dag_run_conf = dag_run.conf
            slack_notification_enable = dag_run_conf.get(
                "slack_notification_enable", Variable.get("slack_notification_enable", default=True)
            )
            if slack_notification_enable:
                logging.info(f"{ti}")
                # data_interval_end may not be present in test contexts; guard it
                try:
                    local_dt = context.get("data_interval_end")
                    if local_dt is not None:
                        local_dt = local_dt.astimezone()
                except Exception:
                    local_dt = None

                slack_notifier = EnsemblSlackNotifier(conn_id=slack_conn_id, context=context)
                env = Variable.get("environment", default="dev")
                block_msg = slack_notifier.format_message(context=context, error_msg=message, status=status)
                logging.info(block_msg)
                # Ensure post_message is always called if notifier is available
                try:
                    slack_notifier.post_message(message=f"{env} Ensembl Pipeline Status", block=block_msg)
                except Exception as e:
                    logging.error(f"Failed to post Slack message: {str(e)}")
            else:
                logging.info(
                    """
                To enable Slack notifications, set the global variable 'slack_notification_enable' to True.
                Alternatively, you can pass the 'slack_notification_enable' parameter in the dag_run configuration.
                """
                )
                logging.info(
                    f"""
                Task: {ti.task_id} Completed with status: {status}
                Message: {message}
                """
                )
        except Exception as e:
            logging.error(f"Failed to send Slack notification: {str(e)}")


class SlurmClientFactory:
    """Factory for creating SLURM clients - enables dependency injection."""

    @staticmethod
    def create_client(config: SlurmConfig, job_name: str) -> EnsemblSlurmRestClient:
        """
        Create a SLURM REST API client.

        Args:
            config: SLURM configuration
            job_name: Job name

        Returns:
            Configured SLURM client
        """
        return EnsemblSlurmRestClient(
            url=config.url,
            user_name=config.user,
            token=config.token,
            api_version=config.api_version,
            parameters={
                "current_working_directory": config.cwd,
                "environment": config.env,
                "partition": config.partition,
                "name": job_name,
                "time_limit": config.time_limit,
                "memory_per_node": {"set": "true", "number": config.memory_mb},
            },
            log_directory=config.log_directory,
            std_split=True,
        )


class EnsemblBashOperator(BashOperator):
    """
    SOLID-compliant Airflow operator for executing Bash commands via SLURM.

    This operator follows SOLID principles:
    - Single Responsibility: Delegates to focused service classes
    - Open/Closed: Extensible through dependency injection
    - Liskov Substitution: Properly extends BashOperator
    - Interface Segregation: Uses focused service interfaces
    - Dependency Inversion: Depends on abstractions (services)
    """

    template_fields: Sequence[str] = (
        "bash_command",
        "env",
        "cwd",
        "slurm_uri",
        "slurm_api_version",
        "slurm_user",
    )

    def __init__(
        self,
        job_name: str = "",
        slurm_uri: Optional[str] = None,
        slurm_api_version: Optional[str] = None,
        slurm_user: Optional[str] = None,
        slurm_jwt: Optional[str] = None,
        cwd: Optional[str] = None,
        env_vars: Optional[List[str]] = None,
        partition: Optional[str] = "standard",
        time_limit: str = "7D",
        memory_per_node: str = "2GB",
        check_interval: int = 120,
        log_directory: str = "airflow_logs",
        slack_conn_id: str = "airflow-slack-notification",
        run_defer: int = 1,
        required_log_conn_id: Optional[List[str]] = None,
        log_conn_id: Optional[List[str]] = None,
        web_log_uri: Optional[str] = None,
        nf_hive_script_path: Optional[str] = None,
        use_nextflow: bool = True,
        **kwargs,
    ):
        """Initialize operator with dependency injection."""
        super().__init__(**kwargs)

        # Store use_nextflow flag
        self.use_nextflow = use_nextflow

        # Initialize services (Dependency Inversion)
        self.parser = ConfigurationParser()
        self.slurm_config_builder = SlurmConfigBuilder(self.parser)
        self.notification_config_builder = NotificationConfigBuilder()

        # Build configurations
        self.slurm_config = self.slurm_config_builder.build(
            slurm_uri,
            slurm_api_version,
            slurm_user,
            slurm_jwt,
            cwd,
            partition,
            time_limit,
            memory_per_node,
            log_directory,
        )
        self.notification_config = self.notification_config_builder.build(
            slack_conn_id, web_log_uri, nf_hive_script_path, required_log_conn_id, log_conn_id
        )

        # Expose for Airflow templating
        self.slurm_uri = self.slurm_config.url
        self.slurm_api_version = self.slurm_config.api_version
        self.slurm_user = self.slurm_config.user
        self.slurm_jwt = self.slurm_config.token
        self.cwd = self.slurm_config.cwd
        self.log_directory = self.slurm_config.log_directory
        self.partition = partition

        # Job parameters
        self.job_name = job_name
        self.env_vars = env_vars or []
        self.check_interval = check_interval
        self.run_defer = run_defer

        # Runtime state
        self.job_info: Optional[JobInfo] = None
        self.ensembl_cmd: Optional[str] = None

        # Initialize services
        slurm_client = SlurmClientFactory().create_client(self.slurm_config, self.job_name)
        self.job_service = SlurmJobService(slurm_client, check_interval)
        self.notification_service = SlackNotificationService(slack_conn_id, self.slurm_config)
        self.command_builder = NextflowCommandBuilder()

        logging.info(
            f"[EnsemblBashOperator] Initialized for user '{self.slurm_config.user}' "
            f"with API version {self.slurm_config.api_version} at {self.slurm_config.url}"
        )

    def _prepare_slurm_job(self, context: Context) -> None:
        """
        Prepare the SLURM job runtime configuration.
        Updates environment, run_defer, and job_name.
        """
        # Dynamically update SLURM client environment with user env and env_vars
        current_env = self.slurm_config.env.copy()
        if hasattr(self, "env") and self.env:
            current_env.update(self.env)

        if self.env_vars:
            if isinstance(self.env_vars, dict):
                current_env.update(self.env_vars)
            elif isinstance(self.env_vars, list):
                for item in self.env_vars:
                    if "=" in item:
                        k, v = item.split("=", 1)
                        current_env[k] = v

        self.job_service.client._parameters["environment"] = current_env

        dag_run = context["dag_run"]
        dag_run_conf = dag_run.conf or {}

        # Update run_defer from config
        self.run_defer = dag_run_conf.get("run_defer", self.run_defer)

        # Parse job name
        self.job_name = self.parser.parse_job_name(context, self.job_name)

    def pre_execute(self, context: Context) -> None:
        """Prepare command before execution."""
        try:
            logging.info("Preparing command for execution")

            self._prepare_slurm_job(context)

            # Build command - skip Nextflow wrapping if use_nextflow is False
            if self.use_nextflow:
                work_dir = os.path.join(self.slurm_config.cwd, self.job_name)
                nf_plugin_version = Variable.get("nf_plugin", default="ens-nf-weblog@2.10.2")

                self.ensembl_cmd = self.command_builder.build_command(
                    base_command=self.bash_command,
                    job_name=self.job_name,
                    context=context,
                    work_dir=work_dir,
                    web_log_uri=self.notification_config.web_log_uri,
                    nf_script_path=self.notification_config.nf_script_path,
                    nf_plugin_version=nf_plugin_version,
                )
                logging.info(f"Prepared Nextflow-wrapped job '{self.job_name}' for execution")
            else:
                # Direct execution without Nextflow wrapper (used by decorator tasks)
                self.ensembl_cmd = self.bash_command
                logging.info(f"Prepared direct job '{self.job_name}' for execution (no Nextflow)")

        except Exception as e:
            logging.error(f"Error in pre_execute: {str(e)}")
            raise AirflowExceptionWithSlackNotification(
                f"Error during pre_execute: {str(e)}", context, self.notification_config.slack_conn_id
            )

    def execute(self, context: Context) -> Any:
        """Execute the job."""
        try:
            logging.info(f"Executing job '{self.job_name}'")

            task_instance = context["ti"]
            dag_run = context["dag_run"]
            dag_run_conf = dag_run.conf

            # Check for skip
            task_name = task_instance.task_id.rsplit(".", 1)[-1]
            if task_name in dag_run_conf.get("skip_pipeline", []):
                logging.info(f"Skipping task {task_instance.task_id}")
                task_instance.state = State.SKIPPED
                return

            # Submit a job if not already running
            # Check Slurm Jobs Already Running with the same name to avoid resubmission and monitor the existing one instead
            logging.info(f"Checking for existing SLURM jobs with name '{self.job_name}'")
            existing_job = self.job_service.get_job_status_and_id_by_name(self.job_name)
            if existing_job is not None and existing_job[1] not in [
                "COMPLETED",
                "FAILED",
                "CANCELLED",
                "TIMEOUT",
            ]:
                self.job_info = JobInfo(
                    job_id=existing_job[0],
                    job_name=self.job_name,
                    bash_command=self.ensembl_cmd,
                    status=existing_job[1],
                )
                logging.warning(f"Found existing SLURM jobs with name '{self.job_name}': {existing_job}")
                logging.warning(
                    f"Monitoring Existing Job ID {existing_job[0]} with status {existing_job[1]} instead of submitting a new job"
                )
                logging.info(
                    f"No existing SLURM job found with name '{self.job_name}', preparing to submit new job"
                )

            else:
                logging.info(f"Submitting a new SLURM job for '{self.job_name}'")
                job_id = self.job_service.submit_job(self.ensembl_cmd, self.job_name)
                self.job_info = JobInfo(job_id=job_id, job_name=self.job_name, bash_command=self.ensembl_cmd)

            # Push Jobs status to XCom for downstream tasks
            task_instance.xcom_push(
                key="job_info",
                value={
                    "job_id": self.job_info.job_id,
                    "task_id": task_instance.task_id,
                    "job_name": self.job_name,
                    "user": self.slurm_config.user,
                },
            )

            # Monitor job
            if self.run_defer:
                self._defer_monitoring(context, self.job_info.job_id)
            else:
                status = self.job_service.wait_for_job(self.job_info.job_id, period=30)
                self.job_info.status = status

                if status != "COMPLETED":
                    logging.error(f"**Task: {self.job_name} Failed for slurm job_id  {job_id} **")
                    # raise ValueError(f"Job {self.job_name} failed with status {status}")

        except Exception as e:
            logging.error(f"Error in execute: {str(e)}")
            raise AirflowExceptionWithSlackNotification(
                f"Failed to execute job: {str(e)}", context, self.notification_config.slack_conn_id
            )

    def _defer_monitoring(self, context: Context, job_id: int) -> None:
        """Defer job monitoring."""
        slurm_job_status = self.job_service.get_job_status_from_slurmdb(job_id)
        status = slurm_job_status.status
        self.job_info.status = status

        logging.info(f"Deferring monitoring for job {job_id} with status {status}")

        self.defer(
            trigger=TimeDeltaTrigger(timedelta(seconds=self.check_interval)),
            method_name="_monitor_job",
            kwargs={"job_id": job_id, "job_name": self.job_name, "bash_command": self.ensembl_cmd},
        )

    def _monitor_job(
        self,
        context: Context,
        event: Optional[Dict[str, Any]] = None,
        job_id: int = None,
        job_name: str = None,
        bash_command: str = None,
    ) -> Any:
        """Monitor deferred job."""
        try:
            slurm_job_status = self.job_service.get_job_status_from_slurmdb(job_id)
            status = slurm_job_status.status

            self.job_info = JobInfo(
                job_id=job_id, job_name=job_name, bash_command=bash_command, status=status
            )

            # Check if terminal state
            terminal_states = {"FAILED", "COMPLETED", "CANCELLED", "TIMEOUT"}
            if status not in terminal_states:
                logging.info(f"Job {job_id} status: {status}, continuing to monitor")
                self.defer(
                    trigger=TimeDeltaTrigger(timedelta(seconds=self.check_interval)),
                    method_name="_monitor_job",
                    kwargs={"job_id": job_id, "job_name": job_name, "bash_command": bash_command},
                )

            logging.info(f"Job {job_id} reached terminal state: {status}")

            if status != "COMPLETED":
                logging.error(f"**Task: {job_name} Failed for slurm job_id  {job_id} **")

        except Exception as e:
            raise AirflowExceptionWithSlackNotification(
                str(e), context, self.notification_config.slack_conn_id
            )

    def post_execute(self, context: Any, result: Any = None) -> None:
        """Post-execution operations."""

        msg = f"""
            Post execution....
            Task: {self.task_id} / {self.job_info.job_name}
            Job {self.job_info.job_id} completed with status {self.job_info.status}
            For provided command: 
            {self.bash_command}
            More info in logs :
            Logs: {self.cwd}/{self.log_directory}/{self.job_name}
            CWD:  {self.cwd}/{self.job_name}/
            """

        try:

            logging.info("Fetching Logs from slurm .......")
            # Todo: Copy the logs to the log directory by another slurm job
            copy_job_status = self.copy_k8s_logs()

            if copy_job_status == "COMPLETED":
                # Todo: read the log file from the k8s mounted /nfs/public
                logging.info(f"Logs copied successfully for job {self.job_info.job_name}")
                logging.info("************************Slurm Logs****************************************")
                # Open and read the log files from the k8s log directory and push to xcom for notification
                slurm_log_file = f"/opt/airflow/codon/ens_automation/k8s_logs/{self.job_info.job_name}.{self.job_info.job_id}.out"
                if os.path.exists(slurm_log_file):
                    with open(slurm_log_file, "r") as f:
                        for line in f:
                            logging.info(line.strip())
                else:
                    logging.warning(f"Log file {slurm_log_file} not found in k8s log directory")

            else:
                logging.error(f"Log Files copy job failed with status: {copy_job_status}")

            if self.job_info.status != "COMPLETED":
                logging.error(msg)
                raise ValueError(f"{msg}")
            logging.info(msg)
        except Exception as e:
            logging.info(msg)
            raise AirflowExceptionWithSlackNotification(
                str(e), context, self.notification_config.slack_conn_id
            )

    def copy_k8s_logs(self):
        """Copy logs from the job directory to the k8s log directory"""
        k8s_log_location = "/nfs/public/rw/ens_automation/k8s_logs/"
        slurm_copy_client = SlurmClientFactory.create_client(
            self.slurm_config, f"{self.job_info.job_name}_copy_logs"
        )
        slurm_copy_client._parameters["partition"] = "datamover"
        job_service = SlurmJobService(slurm_copy_client, 90)
        copy_command = f"cp -r {self.cwd}/{self.log_directory}/{self.job_info.job_name}.{self.job_info.job_id}.*  {k8s_log_location} "
        copy_job_id = job_service.submit_job(copy_command, f"{self.job_info.job_name}_copy_logs")
        logging.info(
            f"Copying logs for job {self.job_info.job_name} from {self.cwd}/{self.log_directory}/ to {k8s_log_location}"
        )
        copy_job_status = self.job_service.wait_for_job(copy_job_id, period=5)

        return copy_job_status

    def on_kill(self) -> None:
        """Handle task kill."""
        logging.warning("Task killed")
        # Note: Sending notification with empty context may fail
        # Better to handle this through Airflow's built-in notification mechanisms
