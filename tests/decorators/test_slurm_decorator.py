"""
Test suite for the @ensemblslurm_task decorator and _SlurmDecoratedOperator.
"""

import os
import pickle
import subprocess
import tempfile
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from ensemblslurm.decorators.slurm import _SlurmDecoratedOperator, ensemblslurm_task


# ============================================================================
# Module-level callables used as python_callable across tests.
# inspect.getsource() needs a real file on disk, so these can't be defined
# inline inside test bodies as closures over locals - top-level defs work fine.
# ============================================================================

def _add(x, y=2):
    return x + y


def _no_args():
    return "no-args-result"


def _raises(x):
    raise ValueError(f"bad input: {x}")


@ensemblslurm_task(job_name="decorated_add")
def _decorated_add(x, y):
    return x + y


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_context():
    context = {
        'ti': Mock(),
        'task_instance': Mock(),
        'dag_run': Mock(),
        'task': Mock(),
    }
    context['ti'].task_id = 'test_task'
    context['ti'].dag_id = 'test_dag'
    context['ti'].run_id = 'manual__20240101t123045'
    context['ti'].try_number = 1
    context['ti'].task.task_type = '_SlurmDecoratedOperator'
    context['task_instance'] = context['ti']

    context['dag_run'].dag_id = 'test_dag'
    context['dag_run'].run_id = 'manual__20240101t123045'
    context['dag_run'].conf = {'skip_pipeline': []}

    return context


# ============================================================================
# TEST _SlurmDecoratedOperator command generation
# ============================================================================

class TestGeneratedCommand:
    """Test suite for _SlurmDecoratedOperator's bash command generation."""

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_command_defines_and_calls_function(self, mock_factory):
        """Test the generated command embeds the function source and a call to it."""
        operator = _SlurmDecoratedOperator(
            task_id="add_task",
            python_callable=_add,
            op_args=(1,),
            op_kwargs={"y": 3},
        )

        assert "def _add(x, y=2):" in operator.bash_command
        assert "result = _add(1, y=3)" in operator.bash_command
        assert "python3 -c" in operator.bash_command

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_command_with_no_args(self, mock_factory):
        """Test a function with no arguments produces a bare call."""
        operator = _SlurmDecoratedOperator(
            task_id="no_args_task",
            python_callable=_no_args,
        )

        assert "result = _no_args()" in operator.bash_command

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_command_strips_leading_decorator_line(self, mock_factory):
        """
        Regression test: inspect.getsource() on a decorated function includes the
        decorator line itself. Shipping "@ensemblslurm_task(...)" to the remote
        script would raise NameError there since the decorator isn't importable
        in that bare exec context, so it must be stripped.
        """
        operator = _SlurmDecoratedOperator(
            task_id="decorated_task",
            python_callable=_decorated_add,
            op_args=(1, 2),
        )

        assert "@ensemblslurm_task" not in operator.bash_command
        assert "def _decorated_add(x, y):" in operator.bash_command

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_command_loads_modules(self, mock_factory):
        """Test that specified modules are loaded before the python3 invocation."""
        operator = _SlurmDecoratedOperator(
            task_id="module_task",
            python_callable=_add,
            op_args=(1,),
            modules=["python/3.9", "git"],
        )

        assert "module load python/3.9" in operator.bash_command
        assert "module load git" in operator.bash_command
        # Modules must be loaded before the python3 invocation
        assert operator.bash_command.index("module load python/3.9") < operator.bash_command.index("python3 -c")

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_command_no_modules_by_default(self, mock_factory):
        """Test that no module load lines appear when modules is not provided."""
        operator = _SlurmDecoratedOperator(
            task_id="no_module_task",
            python_callable=_add,
            op_args=(1,),
        )

        assert "module load" not in operator.bash_command

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_result_file_is_unique_pkl(self, mock_factory):
        """Test that each operator instance gets a unique .pkl result filename."""
        op1 = _SlurmDecoratedOperator(task_id="t1", python_callable=_add, op_args=(1,))
        op2 = _SlurmDecoratedOperator(task_id="t2", python_callable=_add, op_args=(1,))

        assert op1.result_file != op2.result_file
        assert op1.result_file.endswith(".pkl")
        assert op2.result_file.endswith(".pkl")

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_use_nextflow_is_false(self, mock_factory):
        """Test that decorator tasks bypass the Nextflow wrapper."""
        operator = _SlurmDecoratedOperator(
            task_id="add_task",
            python_callable=_add,
            op_args=(1,),
        )

        assert operator.use_nextflow is False

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_op_args_and_op_kwargs_forwarded_to_decorated_operator(self, mock_factory):
        """
        Regression test: __init__ must forward op_args/op_kwargs to
        DecoratedOperator.__init__ (via super()), otherwise its signature-binding
        check sees no arguments and rejects any callable with required parameters.
        """
        # Previously raised TypeError("missing a required argument: 'x'")
        operator = _SlurmDecoratedOperator(
            task_id="add_task",
            python_callable=_add,
            op_args=(1,),
            op_kwargs={"y": 3},
        )

        assert operator.op_args == (1,)
        assert operator.op_kwargs == {"y": 3}


# ============================================================================
# TEST generated command actually runs (integration-style, no Airflow needed)
# ============================================================================

class TestGeneratedCommandExecution:
    """
    Executes the generated bash command in a real subprocess to verify the
    round trip: function source -> python3 -c '...' -> pickled result on disk.
    This is the level at which the shlex.quote/repr and func_name interpolation
    regressions actually surface (they don't raise at command-generation time).
    """

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_success_path_writes_pickle_result(self, mock_factory):
        operator = _SlurmDecoratedOperator(
            task_id="add_task",
            python_callable=_add,
            op_args=(1,),
            op_kwargs={"y": 3},
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                ["bash", "-c", operator.bash_command],
                cwd=tmpdir,
                capture_output=True,
                text=True,
                timeout=30,
            )

            assert result.returncode == 0, result.stderr
            assert "Task _add completed successfully" in result.stdout

            result_path = os.path.join(tmpdir, operator.result_file)
            assert os.path.exists(result_path)
            with open(result_path, "rb") as f:
                value = pickle.load(f)
            assert value == 4

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_failure_path_exits_nonzero_with_message(self, mock_factory):
        operator = _SlurmDecoratedOperator(
            task_id="fail_task",
            python_callable=_raises,
            op_args=(5,),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                ["bash", "-c", operator.bash_command],
                cwd=tmpdir,
                capture_output=True,
                text=True,
                timeout=30,
            )

            assert result.returncode == 1
            assert "bad input: 5" in result.stderr
            assert not os.path.exists(os.path.join(tmpdir, operator.result_file))


# ============================================================================
# TEST _SlurmDecoratedOperator.execute() result retrieval
# ============================================================================

class TestExecuteResultRetrieval:
    """Test suite for _SlurmDecoratedOperator.execute()'s pickle-reading logic."""

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_execute_reads_pickled_result(self, mock_factory, mock_context, tmp_path):
        operator = _SlurmDecoratedOperator(
            task_id="add_task",
            python_callable=_add,
            op_args=(1,),
            op_kwargs={"y": 3},
            cwd=str(tmp_path),
            run_defer=0,
        )
        operator.ensembl_cmd = operator.bash_command
        operator.job_name = "test_job"

        mock_client = Mock()
        mock_client._parameters = {"name": "test_job"}
        mock_client.submit_script.return_value = "12345"
        mock_client.get_all_job_properties.return_value = []
        mock_client.get_status.return_value = "COMPLETED"
        from ensemblslurm.operators.ensembl_bash import SlurmJobService
        operator.job_service = SlurmJobService(mock_client)

        # Simulate what the remote SLURM job would have written
        result_path = tmp_path / operator.result_file
        with open(result_path, "wb") as f:
            pickle.dump(4, f)

        result = operator.execute(mock_context)

        assert result == 4
        # Result file should be cleaned up after being read
        assert not result_path.exists()

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_execute_returns_none_when_result_file_missing(self, mock_factory, mock_context, tmp_path):
        operator = _SlurmDecoratedOperator(
            task_id="add_task",
            python_callable=_add,
            op_args=(1,),
            cwd=str(tmp_path),
            run_defer=0,
        )
        operator.ensembl_cmd = operator.bash_command
        operator.job_name = "test_job"

        mock_client = Mock()
        mock_client._parameters = {"name": "test_job"}
        mock_client.submit_script.return_value = "12345"
        mock_client.get_all_job_properties.return_value = []
        mock_client.get_status.return_value = "COMPLETED"
        from ensemblslurm.operators.ensembl_bash import SlurmJobService
        operator.job_service = SlurmJobService(mock_client)

        result = operator.execute(mock_context)

        assert result is None

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_execute_returns_none_when_no_result_file_configured(self, mock_factory, mock_context, tmp_path):
        operator = _SlurmDecoratedOperator(
            task_id="add_task",
            python_callable=_add,
            op_args=(1,),
            cwd=str(tmp_path),
            run_defer=0,
        )
        operator.ensembl_cmd = operator.bash_command
        operator.job_name = "test_job"
        operator.result_file = None

        mock_client = Mock()
        mock_client._parameters = {"name": "test_job"}
        mock_client.submit_script.return_value = "12345"
        mock_client.get_all_job_properties.return_value = []
        mock_client.get_status.return_value = "COMPLETED"
        from ensemblslurm.operators.ensembl_bash import SlurmJobService
        operator.job_service = SlurmJobService(mock_client)

        result = operator.execute(mock_context)

        assert result is None

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_execute_submits_generated_command_to_slurm(self, mock_factory, mock_context, tmp_path):
        """Test that execute() actually submits operator.ensembl_cmd (not the raw
        python_callable executed locally) - i.e. the job genuinely runs on Slurm."""
        operator = _SlurmDecoratedOperator(
            task_id="add_task",
            python_callable=_add,
            op_args=(1,),
            op_kwargs={"y": 3},
            cwd=str(tmp_path),
            run_defer=0,
        )
        operator.ensembl_cmd = operator.bash_command
        operator.job_name = "test_job"

        mock_client = Mock()
        mock_client._parameters = {"name": "test_job"}
        mock_client.submit_script.return_value = "12345"
        mock_client.get_all_job_properties.return_value = []
        mock_client.get_status.return_value = "COMPLETED"
        from ensemblslurm.operators.ensembl_bash import SlurmJobService
        operator.job_service = SlurmJobService(mock_client)

        operator.execute(mock_context)

        mock_client.submit_script.assert_called_once_with(operator.bash_command)


# ============================================================================
# TEST ensemblslurm_task decorator (DAG integration)
# ============================================================================

class TestEnsemblslurmTaskDecorator:
    """Test suite for the public ensemblslurm_task decorator factory."""

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_decorator_task_type_and_task_id(self, mock_factory):
        from airflow.sdk import DAG

        with DAG(dag_id="test_dag_decorator2", start_date=datetime(2024, 1, 1), schedule=None) as dag:
            @ensemblslurm_task(job_name="my_job")
            def my_task(x, y):
                return x + y

            my_task(1, 2)

        task = dag.task_dict["my_task"]
        assert isinstance(task, _SlurmDecoratedOperator)
        assert task.task_id == "my_task"

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_decorator_forwards_kwargs_to_operator(self, mock_factory):
        from airflow.sdk import DAG

        with DAG(dag_id="test_dag_decorator3", start_date=datetime(2024, 1, 1), schedule=None) as dag:
            @ensemblslurm_task(job_name="explicit_job", memory_per_node="8GB", time_limit="3H")
            def my_task(x, y):
                return x + y

            my_task(1, 2)

        task = dag.task_dict["my_task"]
        assert task.job_name == "explicit_job"
        assert task.slurm_config.memory_mb == 8192
        assert task.slurm_config.time_limit == 180

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_decorator_with_required_positional_args(self, mock_factory):
        """
        Regression test: calling a decorated function with required positional args
        used to raise TypeError("missing a required argument") because op_args
        weren't forwarded to DecoratedOperator.__init__.
        """
        from airflow.sdk import DAG

        with DAG(dag_id="test_dag_decorator4", start_date=datetime(2024, 1, 1), schedule=None) as dag:
            @ensemblslurm_task()
            def requires_args(a, b, c=3):
                return a + b + c

            requires_args(10, 20)

        task = dag.task_dict["requires_args"]
        assert task.op_args == (10, 20)
        assert "result = requires_args(10, 20)" in task.bash_command


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
