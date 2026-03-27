import os
import logging
from airflow.models import Variable
from typing import Sequence, Optional
from ensemblslurm.operators.ensembl_bash import ICommandBuilder
from airflow.utils.context import Context
from ensemblslurm.operators.ensembl_bash import (
    EnsemblBashOperator,
    ICommandBuilder,
    AirflowExceptionWithSlackNotification,
)

logger = logging.getLogger(__name__)

class HiveCommandPreparer:
    """Parses and prepares Hive init_pipeline command."""

    def prepare(
        self,
        bash_command: str,
        dag_run_conf: dict,
        job_name: str,
        prepare_pipeline_param_by: str,
    ) -> tuple[str, str]:

        lines = bash_command.strip().replace("\\", "").split("\n")

        variable_parts = []
        command_parts = []
        init_pipeline_found = False

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if not init_pipeline_found:
                if "init_pipeline.pl" in line:
                    init_pipeline_found = True
                    parts = line.split("init_pipeline.pl", 1)

                    if parts[0].strip():
                        variable_parts.append(parts[0].strip().rstrip("&&").strip())

                    command_parts.append(f"init_pipeline.pl {parts[1].strip()}")
                else:
                    variable_parts.extend([p.strip() for p in line.split("&&")])
            else:
                command_parts.append(line)

        if not init_pipeline_found:
            raise ValueError("Missing `init_pipeline.pl` in bash_command")

        variable_command = " ; ".join(variable_parts)
        pipeline_command = " ".join(command_parts)

        # Add pipeline name
        if job_name:
            pipeline_command += f" -pipeline_name {job_name.lower()}"

        # Add dynamic params
        if dag_run_conf and prepare_pipeline_param_by in dag_run_conf:
            values = dag_run_conf[prepare_pipeline_param_by]

            if not isinstance(values, list):
                raise ValueError(f"{prepare_pipeline_param_by} must be list")

            params = " ".join(
                f"-{prepare_pipeline_param_by} {v}" for v in values
            )
            pipeline_command += f" {params}"

        # antispecies
        if dag_run_conf.get("antispecies"):
            antispecies = " ".join(
                f"-antispecies {v}" for v in dag_run_conf["antispecies"]
            )
            pipeline_command += f" {antispecies}"

        # hive_force_init
        if dag_run_conf.get("hive_force_init"):
            pipeline_command += f" -hive_force_init {dag_run_conf['hive_force_init']}"

        return variable_command, f"'{pipeline_command}'"

class HiveNextflowCommandBuilder(ICommandBuilder):
    """Builds Nextflow Hive execution command."""

    def __init__(self, nf_script_path: str, param_key: str = "genome_uuid"):
        self.nf_script_path = nf_script_path
        self.param_key = param_key
        self.preparer = HiveCommandPreparer()

    def build_command(self, base_command: str, job_name: str, context: Context) -> str:
        dag_run_conf = context["dag_run"].conf or {}
        task_try_number = context["ti"].try_number

        params = context.get("params", {})
        work_dir = params.get("work_dir")
        web_log_uri = params.get("web_log_uri")

        if not work_dir or not web_log_uri:
            raise ValueError("Missing required params")

        nf_plugin_version = Variable.get(
            "nf_plugin", default_var="ens-nf-weblog@2.10.2"
        )

        variable_cmd, hive_cmd = self.preparer.prepare(
            base_command,
            dag_run_conf,
            job_name,
            self.param_key,
        )
        """
          -plugins "{nf_plugin_version}" \
          -with-weblog "{web_log_uri}" \
        """
        return f"""
        export NXF_WORK="{work_dir}"
        mkdir -p "{work_dir}"
        cd "{work_dir}"

        {variable_cmd}

        nextflow run "{self.nf_script_path}" \
          --run_mode hive \
          --cmd {hive_cmd} \
          -work-dir "{work_dir}" \
          -name "{job_name}_{task_try_number}" \
          -resume
        """


class HiveNextflowOperator(EnsemblBashOperator):
    """
    SOLID-compliant Hive Nextflow operator.
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
        nf_hive_script_path: Optional[str] = None,
        prepare_pipeline_param_by: str = "genome_uuid",
        command_builder: Optional[ICommandBuilder] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        script_path = nf_hive_script_path or os.getenv(
            "NF_HIVE_SCRIPT_PATH",
            f"/homes/{os.getenv('SLURM_USER', 'ens2020')}/dispatcher/main.nf",
        )

        # Inject builder (DIP ✅)
        self.command_builder: ICommandBuilder = (
            command_builder
            or HiveNextflowCommandBuilder(script_path, prepare_pipeline_param_by)
        )

    def pre_execute(self, context: Context) -> None:
        try:
            logger.info("Preparing Hive Nextflow execution")

            dag_run_conf = context["dag_run"].conf or {}

            self.run_defer = dag_run_conf.get("run_defer", self.run_defer)

            # Use shared parser
            self.job_name = self.parser.parse_job_name(context, self.job_name)

            work_dir = os.path.join(self.cwd, self.job_name)

            # Inject params for builder
            context["params"] = {
                "work_dir": work_dir,
                "web_log_uri": self.notification_config.web_log_uri,
            }

            # Delegate command building
            self.ensembl_cmd = self.command_builder.build_command(
                self.bash_command,
                self.job_name,
                context,
            )

            logger.info(
                f"""
                Hive Nextflow job prepared:
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