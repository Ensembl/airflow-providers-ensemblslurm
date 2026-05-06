import os
import logging
from typing import Sequence, Optional
from airflow.utils.context import Context
from airflow.models import Variable
from ensemblslurm.operators.ensembl_bash import (
    EnsemblBashOperator,
    ICommandBuilder,
    AirflowExceptionWithSlackNotification,
)

logger = logging.getLogger(__name__)


class DynamicNextflowCommandBuilder(ICommandBuilder):
    """
    Builds dynamic Nextflow commands using DAG run configuration.
    """

    def build_command(
        self,
        base_command: str,
        job_name: str,
        context: Context,
    ) -> str:
        dag_run_conf = context["dag_run"].conf or {}
        task_try_number = context["ti"].try_number

        params = context.get("params", {})
        work_dir = params.get("work_dir")
        web_log_uri = params.get("web_log_uri")

        if not work_dir or not web_log_uri:
            raise ValueError("Missing required params: work_dir or web_log_uri")

        nf_plugin_version = Variable.get(
            "nf_plugin", default_var="ens-nf-weblog@2.10.2"
        )

        dynamic_args = []

        for key in ["genome_uuid", "antispecies"]:
            value = dag_run_conf.get(key)
            if value:
                value_str = ",".join(value) if isinstance(value, list) else str(value)
                dynamic_args.append(f"--{key} {value_str}")

        """
        #Todo add plugin later 
        f"-plugins {nf_plugin_version}",
        f"-with-weblog '{web_log_uri}'",

        """
        dynamic_args.extend(
            [
                f"-work-dir {work_dir}",
                f"-name {job_name}_{task_try_number}",
                "-resume",
            ]
        )

        full_command = f"{base_command.strip()} {' '.join(dynamic_args)}"

        return f"""
        export NXF_WORK="{work_dir}"
        mkdir -p "{work_dir}"
        cd "{work_dir}"

        {full_command}
        """


class NextflowOperator(EnsemblBashOperator):
    """
    NextflowOperator for running Nextflow workflows.
    It encapsulates the orchestration logic while delegating command building to an
    ICommandBuilder interface for extensibility. This design ensures that the operator
    remains clean and focused on orchestration, adhering to the Single Responsibility Principle (SRP).
    """

    template_fields: Sequence[str] = (
        "bash_command",
        "env",
        "cwd",
        "slurm_uri",
        "slurm_api_version",
        "slurm_user",
        "job_name",
    )

    def __init__(
        self,
        command_builder: Optional[ICommandBuilder] = None,
        **kwargs,
    ):
        """
        Initialize NextflowOperator.

        Args:
            command_builder: Optional custom command builder (for extensibility)
        """
        super().__init__(**kwargs)

        self.command_builder: ICommandBuilder = (
            command_builder or DynamicNextflowCommandBuilder()
        )

    # ----------------------------------
    # Pre-execution (Orchestration only)
    # ----------------------------------
    def pre_execute(self, context: Context) -> None:
        try:
            logger.info("Preparing Nextflow execution")

            self._prepare_slurm_job(context)

            # Prepare working directory
            work_dir = os.path.join(self.cwd, self.job_name)

            # Inject runtime parameters for builder
            context["params"] = {
                "work_dir": work_dir,
                "web_log_uri": self.notification_config.web_log_uri,
            }

            # Delegate command creation (SRP ✅)
            self.ensembl_cmd = self.command_builder.build_command(
                base_command=self.bash_command,
                job_name=self.job_name,
                context=context,
            )

            logger.info(
                f"""
                Nextflow job prepared:
                - job_name: {self.job_name}
                - run_defer: {self.run_defer}
                - try_number: {context['ti'].try_number}
                - work_dir: {work_dir}
                """
            )

        except Exception as e:
            logger.error(f"Error in pre_execute: {str(e)}")

            raise AirflowExceptionWithSlackNotification(
                f"Error during pre_execute: {str(e)}",
                context,
                self.notification_config.slack_conn_id,
            )

    # ----------------------------------
    # Kill handling (optional override)
    # ----------------------------------
    def on_kill(self) -> None:
        logger.warning("Nextflow task killed")
        try:
            if hasattr(self, "subprocess_hook"):
                self.subprocess_hook.send_sigterm()
        except Exception as e:
            logger.error(f"Error during kill handling: {str(e)}")