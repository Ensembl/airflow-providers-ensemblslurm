"""
Slurm task decorator for Apache Airflow.

This module provides a @ensemblslurm_task decorator that allows you to run Python
functions as Slurm jobs, similar to how @docker_task works for Docker containers.

Example:
    @ensemblslurm_task(
        memory_per_node="4GB",
        time_limit="2H",
        slurm_user="myuser"
    )
    def process_genome(species: str, release: int) -> dict:
        # This function will be executed as a Slurm job
        return {"species": species, "release": release, "status": "completed"}

    with DAG("my_dag", start_date=datetime(2024, 1, 1)) as dag:
        result = process_genome(species="homo_sapiens", release=112)
"""

from __future__ import annotations

import inspect
import os
import textwrap
from typing import Any, Callable, Mapping, Sequence

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.utils.context import Context

from ensemblslurm.operators.ensembl_bash import EnsemblBashOperator


class _SlurmDecoratedOperator(DecoratedOperator, EnsemblBashOperator):
    """
    Decorated Slurm operator that wraps a Python callable to run on Slurm.

    This operator extends both DecoratedOperator and EnsemblBashOperator to provide
    a seamless integration between Airflow's task decorator pattern and Slurm job execution.
    """

    custom_operator_name = "@ensemblslurm_task"

    template_fields: Sequence[str] = (*DecoratedOperator.template_fields, *EnsemblBashOperator.template_fields)

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: tuple | None = None,
        op_kwargs: dict | None = None,
        job_name: str = "",
        slurm_uri: str | None = None,
        slurm_api_version: str | None = None,
        slurm_user: str | None = None,
        slurm_jwt: str | None = None,
        cwd: str | None = None,
        time_limit: str = "1D",
        memory_per_node: str = "2GB",
        log_directory: str = "airflow_logs",
        slack_conn_id: str = "airflow-slack-notification",
        web_log_uri: str | None = None,
        nf_hive_script_path: str | None = None,
        required_log_conn_id: list[str] | None = None,
        log_conn_id: list[str] | None = None,
        run_defer: int = 0,
        modules: list[str] | None = None,
        **kwargs,
    ) -> None:
        """
        Initialize the Slurm decorated operator.

        Args:
            python_callable: The Python function to execute
            op_args: Positional arguments for the callable
            op_kwargs: Keyword arguments for the callable
            job_name: Custom name for the Slurm job
            slurm_uri: Slurm REST API URL
            slurm_api_version: Slurm API version
            slurm_user: Slurm username
            slurm_jwt: Slurm JWT token
            cwd: Working directory for the job
            time_limit: Time limit (e.g., "2H", "1D")
            memory_per_node: Memory per node (e.g., "4GB", "512MB")
            log_directory: Directory for logs
            slack_conn_id: Slack connection ID for notifications
            web_log_uri: Nextflow web log URI
            nf_hive_script_path: Path to Nextflow hive script
            required_log_conn_id: Required log connection IDs
            log_conn_id: Log connection IDs
            run_defer: Whether to run in deferred mode (0 or 1)
            modules: List of Lua modules to load before execution (e.g., ["python/3.9", "git"])
            **kwargs: Additional keyword arguments
        """
        self.python_callable = python_callable
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}
        self.modules = modules or []
        self.result_file = None  # Will be set during command generation

        # Generate the bash command that will execute the Python function
        bash_command = self._generate_python_command()

        # Initialize the EnsemblBashOperator with the generated command
        # IMPORTANT: use_nextflow=False to bypass Nextflow wrapping for decorator tasks
        super().__init__(
            python_callable=self.python_callable,
            bash_command=bash_command,
            job_name=job_name,
            slurm_uri=slurm_uri,
            slurm_api_version=slurm_api_version,
            slurm_user=slurm_user,
            slurm_jwt=slurm_jwt,
            cwd=cwd,
            time_limit=time_limit,
            memory_per_node=memory_per_node,
            log_directory=log_directory,
            slack_conn_id=slack_conn_id,
            web_log_uri=web_log_uri,
            nf_hive_script_path=nf_hive_script_path,
            required_log_conn_id=required_log_conn_id,
            log_conn_id=log_conn_id,
            run_defer=run_defer,
            use_nextflow=False,  # Bypass Nextflow for decorators
            **kwargs,
        )

    def _generate_python_command(self) -> str:
        """
        Generate a bash command that will execute the Python callable with its arguments.

        If modules are specified, they will be loaded using 'module load' before execution.
        Results are written to a file that can be read back after execution.

        Returns:
            A bash command string that sets up and executes the Python function
        """
        # Generate a unique result file path
        import uuid
        result_filename = f".airflow_result_{uuid.uuid4().hex[:12]}.pkl"
        self.result_file = result_filename  # Store for later retrieval

        # Get the source code of the function
        source = inspect.getsource(self.python_callable)
        # Remove any leading indentation
        source = textwrap.dedent(source)

        # Get the function name
        func_name = self.python_callable.__name__

        # Build the arguments string
        args_str = ", ".join(repr(arg) for arg in self.op_args)
        kwargs_str = ", ".join(f"{k}={repr(v)}" for k, v in self.op_kwargs.items())

        # Combine args and kwargs
        all_args = [args_str, kwargs_str] if args_str and kwargs_str else [args_str or kwargs_str]
        call_args = ", ".join(filter(None, all_args))

        # Create a Python script that:
        # 1. Defines the function
        # 2. Calls it with the provided arguments
        # 3. Writes the result to a file
        # 4. Also prints to stdout for logging
        python_script = f'''
import sys
import json
import pickle
import base64
import os

# Define the function
{source}

# Execute the function
try:
    result = {func_name}({call_args})

    # Write result to file for Airflow to retrieve
    result_file_path = "{result_filename}"
    with open(result_file_path, "wb") as f:
        pickle.dump(result, f)
    print(f"Result written to {{result_file_path}}")

    # Also try to print human-readable version
    try:
        json_result = json.dumps(result, indent=2)
        print(f"TASK_RESULT_JSON: {{json_result}}")
    except (TypeError, ValueError):
        print(f"TASK_RESULT: {{repr(result)[:500]}}")  # Print first 500 chars

    print(f"Task {{func_name}} completed successfully")
    sys.exit(0)
except Exception as e:
    print(f"Task {{func_name}} failed with error: {{e}}", file=sys.stderr)
    import traceback
    traceback.print_exc()
    sys.exit(1)
'''

        # Build the complete bash command
        bash_commands = []

        # Add module loads if specified
        if self.modules:
            bash_commands.append("# Load required Lua modules")
            for module in self.modules:
                bash_commands.append(f"module load {module}")
            bash_commands.append("")  # Empty line for readability

        # Add the Python execution command
        bash_commands.append(f"python3 -c {repr(python_script)}")

        # Join all commands with newlines
        bash_command = "\n".join(bash_commands)

        return bash_command

    def execute(self, context: Context) -> Any:
        """
        Execute the Slurm task and return the result.

        Args:
            context: Airflow context

        Returns:
            The return value of the Python callable
        """
        import pickle
        import os
        import logging

        logger = logging.getLogger(self.__class__.__name__)

        # Execute the parent class execute method (submits to Slurm and waits)
        super().execute(context)

        # Retrieve the result from the file written by the remote job
        if self.result_file:
            # Construct the full path to the result file
            # It should be in the working directory used by Slurm
            result_file_path = os.path.join(self.slurm_config.cwd or os.getcwd(), self.result_file)

            try:
                logger.info(f"Attempting to read result from {result_file_path}")

                if os.path.exists(result_file_path):
                    with open(result_file_path, "rb") as f:
                        result = pickle.load(f)

                    logger.info(f"Successfully loaded result: {type(result)}")

                    # Clean up the result file
                    try:
                        os.remove(result_file_path)
                        logger.debug(f"Cleaned up result file: {result_file_path}")
                    except Exception as e:
                        logger.warning(f"Could not remove result file: {e}")

                    return result
                else:
                    logger.warning(f"Result file not found at {result_file_path}")
                    logger.info("Task completed but result file is missing - returning None")
                    return None

            except Exception as e:
                logger.error(f"Error reading result file: {e}")
                logger.info("Returning None due to error")
                return None
        else:
            logger.warning("No result file was configured")
            return None


def ensemblslurm_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Decorator to create Ensembl Slurm tasks from Python functions.

    This decorator wraps a Python function and executes it as a Slurm job.
    The function's code is serialized and submitted to the Slurm cluster.

    Args:
        python_callable: The Python function to decorate
        multiple_outputs: If True, the function is expected to return a dict
        **kwargs: Additional arguments passed to EnsemblBashOperator, including:
            - memory_per_node: Memory allocation (e.g., "4GB")
            - time_limit: Time limit (e.g., "2H", "1D")
            - modules: List of Lua modules to load (e.g., ["python/3.9", "git"])
            - job_name: Custom job name
            - slack_conn_id: Slack connection for notifications
            - run_defer: Deferred mode (0 or 1)

    Returns:
        A TaskDecorator that can be used to create Slurm tasks

    Example:
        @ensemblslurm_task(
            memory_per_node="4GB",
            time_limit="2H",
            modules=["python/3.9", "git"]
        )
        def process_data(species: str) -> dict:
            # Your processing logic here
            # Modules python/3.9 and git will be loaded before execution
            return {"species": species, "status": "done"}

        with DAG("example_dag", start_date=datetime(2024, 1, 1)) as dag:
            result = process_data(species="homo_sapiens")
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_SlurmDecoratedOperator,
        **kwargs,
    )
