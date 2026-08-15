"""
Test suite for HiveCommandPreparer, HiveNextflowCommandBuilder and HiveNextflowOperator.
"""

import os
import pytest
from unittest.mock import Mock, patch

from ensemblslurm.operators.hive import (
    HiveCommandPreparer,
    HiveNextflowCommandBuilder,
    HiveNextflowOperator,
)
from ensemblslurm.operators.ensembl_bash import AirflowExceptionWithSlackNotification


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_context():
    """Create a mock Airflow context (plain dict, mirroring runtime Context)."""
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
    context['ti'].task = Mock()
    context['ti'].task.task_type = 'HiveNextflowOperator'
    context['task_instance'] = context['ti']

    context['dag_run'].dag_id = 'test_dag'
    context['dag_run'].run_id = 'manual__20240101t123045'
    context['dag_run'].conf = {
        'genome_uuid': ['uuid-123'],
        'skip_pipeline': [],
    }

    return context


# ============================================================================
# TEST HiveCommandPreparer
# ============================================================================

class TestHiveCommandPreparer:
    """Test suite for HiveCommandPreparer class."""

    def test_prepare_basic_command(self):
        """Test preparing a simple init_pipeline.pl command."""
        preparer = HiveCommandPreparer()

        variable_cmd, pipeline_cmd = preparer.prepare(
            bash_command="init_pipeline.pl MyPipeline::Conf",
            dag_run_conf={},
            job_name="test_job",
            prepare_pipeline_param_by="genome_uuid",
        )

        assert variable_cmd == ""
        assert pipeline_cmd == "'init_pipeline.pl MyPipeline::Conf -pipeline_name test_job'"

    def test_prepare_extracts_variables_before_init(self):
        """Test that variable assignments before init_pipeline.pl are captured separately."""
        preparer = HiveCommandPreparer()

        variable_cmd, pipeline_cmd = preparer.prepare(
            bash_command="export FOO=bar && init_pipeline.pl MyPipeline::Conf",
            dag_run_conf={},
            job_name="test_job",
            prepare_pipeline_param_by="genome_uuid",
        )

        assert variable_cmd == "export FOO=bar"
        assert "init_pipeline.pl MyPipeline::Conf" in pipeline_cmd
        assert "-pipeline_name test_job" in pipeline_cmd

    def test_prepare_pipeline_name_lowercased(self):
        """Test that job_name is lowercased when added as -pipeline_name."""
        preparer = HiveCommandPreparer()

        _, pipeline_cmd = preparer.prepare(
            bash_command="init_pipeline.pl MyPipeline::Conf",
            dag_run_conf={},
            job_name="Test_JOB",
            prepare_pipeline_param_by="genome_uuid",
        )

        assert "-pipeline_name test_job" in pipeline_cmd

    def test_prepare_pipeline_name_within_limit_ok(self):
        """Test a job_name at exactly the 80-char limit is accepted."""
        preparer = HiveCommandPreparer()
        job_name = "a" * 80

        _, pipeline_cmd = preparer.prepare(
            bash_command="init_pipeline.pl MyPipeline::Conf",
            dag_run_conf={},
            job_name=job_name,
            prepare_pipeline_param_by="genome_uuid",
        )

        assert f"-pipeline_name {job_name}" in pipeline_cmd

    def test_prepare_pipeline_name_exceeds_limit_raises(self):
        """
        Test that a job_name over 80 chars raises ValueError: eHive's
        -pipeline_name becomes the pipeline database name, which has a length
        limit. This is enforced only here (Hive-specific), not in the shared
        ConfigurationParser.parse_job_name used by EnsemblBashOperator /
        NextflowOperator (see test_ensembl_bash_operator.py).
        """
        preparer = HiveCommandPreparer()
        job_name = "a" * 81

        with pytest.raises(ValueError, match="exceeds max length of 80"):
            preparer.prepare(
                bash_command="init_pipeline.pl MyPipeline::Conf",
                dag_run_conf={},
                job_name=job_name,
                prepare_pipeline_param_by="genome_uuid",
            )

    def test_prepare_missing_init_pipeline_raises(self):
        """Test that missing init_pipeline.pl raises ValueError."""
        preparer = HiveCommandPreparer()

        with pytest.raises(ValueError, match="Missing `init_pipeline.pl`"):
            preparer.prepare(
                bash_command="echo 'no pipeline here'",
                dag_run_conf={},
                job_name="test_job",
                prepare_pipeline_param_by="genome_uuid",
            )

    def test_prepare_adds_dynamic_params(self):
        """Test that dynamic params (e.g. genome_uuid) are appended as repeated flags."""
        preparer = HiveCommandPreparer()

        _, pipeline_cmd = preparer.prepare(
            bash_command="init_pipeline.pl MyPipeline::Conf",
            dag_run_conf={"genome_uuid": ["uuid-1", "uuid-2"]},
            job_name="test_job",
            prepare_pipeline_param_by="genome_uuid",
        )

        assert "-genome_uuid uuid-1" in pipeline_cmd
        assert "-genome_uuid uuid-2" in pipeline_cmd

    def test_prepare_dynamic_param_not_list_raises(self):
        """Test that a non-list value for the dynamic param raises ValueError."""
        preparer = HiveCommandPreparer()

        with pytest.raises(ValueError, match="must be list"):
            preparer.prepare(
                bash_command="init_pipeline.pl MyPipeline::Conf",
                dag_run_conf={"genome_uuid": "not-a-list"},
                job_name="test_job",
                prepare_pipeline_param_by="genome_uuid",
            )

    def test_prepare_adds_antispecies(self):
        """Test that antispecies values are appended as repeated flags."""
        preparer = HiveCommandPreparer()

        _, pipeline_cmd = preparer.prepare(
            bash_command="init_pipeline.pl MyPipeline::Conf",
            dag_run_conf={"antispecies": ["sp1", "sp2"]},
            job_name="test_job",
            prepare_pipeline_param_by="genome_uuid",
        )

        assert "-antispecies sp1" in pipeline_cmd
        assert "-antispecies sp2" in pipeline_cmd

    def test_prepare_adds_hive_force_init(self):
        """Test that hive_force_init is appended when present."""
        preparer = HiveCommandPreparer()

        _, pipeline_cmd = preparer.prepare(
            bash_command="init_pipeline.pl MyPipeline::Conf",
            dag_run_conf={"hive_force_init": 1},
            job_name="test_job",
            prepare_pipeline_param_by="genome_uuid",
        )

        assert "-hive_force_init 1" in pipeline_cmd

    def test_prepare_no_job_name_skips_pipeline_name_flag(self):
        """Test that -pipeline_name is omitted when job_name is empty."""
        preparer = HiveCommandPreparer()

        _, pipeline_cmd = preparer.prepare(
            bash_command="init_pipeline.pl MyPipeline::Conf",
            dag_run_conf={},
            job_name="",
            prepare_pipeline_param_by="genome_uuid",
        )

        assert "-pipeline_name" not in pipeline_cmd

    def test_prepare_multiline_variable_commands(self):
        """Test that multiple pre-init_pipeline lines are joined with ' ; '."""
        preparer = HiveCommandPreparer()

        variable_cmd, _ = preparer.prepare(
            bash_command="export FOO=bar\nexport BAZ=qux\ninit_pipeline.pl MyPipeline::Conf",
            dag_run_conf={},
            job_name="test_job",
            prepare_pipeline_param_by="genome_uuid",
        )

        assert variable_cmd == "export FOO=bar ; export BAZ=qux"


# ============================================================================
# TEST HiveNextflowCommandBuilder
# ============================================================================

class TestHiveNextflowCommandBuilder:
    """Test suite for HiveNextflowCommandBuilder class."""

    @patch('ensemblslurm.operators.hive.Variable')
    def test_build_command_basic(self, mock_variable, mock_context):
        """Test building a basic hive nextflow command."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['params'] = {
            'work_dir': '/test/work',
            'web_log_uri': 'https://log.ebi.ac.uk',
        }

        builder = HiveNextflowCommandBuilder(nf_script_path="/test/script.nf")
        command = builder.build_command(
            base_command="init_pipeline.pl MyPipeline::Conf",
            job_name="test_job",
            context=mock_context,
        )

        assert "nextflow run" in command
        assert "/test/script.nf" in command
        assert "--run_mode hive" in command
        assert "/test/work" in command
        assert "-name \"test_job_1\"" in command
        assert "-resume" in command
        assert "init_pipeline.pl MyPipeline::Conf -pipeline_name test_job" in command

    @patch('ensemblslurm.operators.hive.Variable')
    def test_build_command_missing_work_dir_raises(self, mock_variable, mock_context):
        """Test that missing work_dir raises ValueError."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['params'] = {'web_log_uri': 'https://log.ebi.ac.uk'}

        builder = HiveNextflowCommandBuilder(nf_script_path="/test/script.nf")

        with pytest.raises(ValueError, match="Missing required params"):
            builder.build_command(
                base_command="init_pipeline.pl MyPipeline::Conf",
                job_name="test_job",
                context=mock_context,
            )

    @patch('ensemblslurm.operators.hive.Variable')
    def test_build_command_missing_web_log_uri_raises(self, mock_variable, mock_context):
        """Test that missing web_log_uri raises ValueError."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['params'] = {'work_dir': '/test/work'}

        builder = HiveNextflowCommandBuilder(nf_script_path="/test/script.nf")

        with pytest.raises(ValueError, match="Missing required params"):
            builder.build_command(
                base_command="init_pipeline.pl MyPipeline::Conf",
                job_name="test_job",
                context=mock_context,
            )

    @patch('ensemblslurm.operators.hive.Variable')
    def test_build_command_uses_custom_param_key(self, mock_variable, mock_context):
        """Test that a custom param_key is used to look up dag_run_conf values."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['params'] = {
            'work_dir': '/test/work',
            'web_log_uri': 'https://log.ebi.ac.uk',
        }
        mock_context['dag_run'].conf = {"release_id": [1, 2]}

        builder = HiveNextflowCommandBuilder(nf_script_path="/test/script.nf", param_key="release_id")
        command = builder.build_command(
            base_command="init_pipeline.pl MyPipeline::Conf",
            job_name="test_job",
            context=mock_context,
        )

        assert "-release_id 1" in command
        assert "-release_id 2" in command

    @patch('ensemblslurm.operators.hive.Variable')
    def test_build_command_includes_variable_cmd(self, mock_variable, mock_context):
        """Test that variable assignments preceding init_pipeline.pl are included."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['params'] = {
            'work_dir': '/test/work',
            'web_log_uri': 'https://log.ebi.ac.uk',
        }

        builder = HiveNextflowCommandBuilder(nf_script_path="/test/script.nf")
        command = builder.build_command(
            base_command="export FOO=bar && init_pipeline.pl MyPipeline::Conf",
            job_name="test_job",
            context=mock_context,
        )

        assert "export FOO=bar" in command


# ============================================================================
# TEST HiveNextflowOperator
# ============================================================================

class TestHiveNextflowOperator:
    """Test suite for HiveNextflowOperator class."""

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_default_script_path_from_env(self, mock_factory):
        """Test default nf_hive_script_path uses env vars when not provided."""
        with patch.dict(os.environ, {'SLURM_USER': 'myuser'}, clear=False):
            operator = HiveNextflowOperator(
                task_id="hive_task",
                bash_command="init_pipeline.pl MyPipeline::Conf",
            )

        assert operator.command_builder.nf_script_path == "/homes/myuser/dispatcher/main.nf"

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_custom_script_path(self, mock_factory):
        """Test explicit nf_hive_script_path overrides env/default."""
        operator = HiveNextflowOperator(
            task_id="hive_task",
            bash_command="init_pipeline.pl MyPipeline::Conf",
            nf_hive_script_path="/custom/main.nf",
        )

        assert operator.command_builder.nf_script_path == "/custom/main.nf"

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_custom_command_builder_injected(self, mock_factory):
        """Test that an injected command_builder is used instead of the default."""
        custom_builder = Mock()

        operator = HiveNextflowOperator(
            task_id="hive_task",
            bash_command="init_pipeline.pl MyPipeline::Conf",
            command_builder=custom_builder,
        )

        assert operator.command_builder is custom_builder

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_prepare_pipeline_param_by_passed_to_builder(self, mock_factory):
        """Test that prepare_pipeline_param_by is forwarded to the default command builder."""
        operator = HiveNextflowOperator(
            task_id="hive_task",
            bash_command="init_pipeline.pl MyPipeline::Conf",
            prepare_pipeline_param_by="release_id",
        )

        assert operator.command_builder.param_key == "release_id"

    @patch('ensemblslurm.operators.hive.Variable')
    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_pre_execute_builds_command(self, mock_factory, mock_variable, mock_context):
        """Test pre_execute prepares the hive nextflow command and sets ensembl_cmd."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"

        operator = HiveNextflowOperator(
            task_id="hive_task",
            bash_command="init_pipeline.pl MyPipeline::Conf",
        )

        operator.pre_execute(mock_context)

        assert operator.ensembl_cmd is not None
        assert "nextflow run" in operator.ensembl_cmd
        assert "--run_mode hive" in operator.ensembl_cmd
        assert operator.job_name is not None
        assert mock_context['params']['work_dir'] == os.path.join(operator.cwd, operator.job_name)

    @patch('ensemblslurm.operators.hive.Variable')
    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_pre_execute_uses_custom_job_name(self, mock_factory, mock_variable, mock_context):
        """Test pre_execute honors an explicitly provided job_name."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"

        operator = HiveNextflowOperator(
            task_id="hive_task",
            bash_command="init_pipeline.pl MyPipeline::Conf",
            job_name="my_hive_job",
        )

        operator.pre_execute(mock_context)

        assert operator.job_name == "my_hive_job"
        assert "-pipeline_name my_hive_job" in operator.ensembl_cmd

    @patch('ensemblslurm.operators.hive.Variable')
    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_pre_execute_job_name_over_80_chars_raises_with_slack_notification(
        self, mock_factory, mock_variable, mock_context
    ):
        """
        End-to-end regression test: a job_name over the 80-char Hive
        pipeline_name limit surfaces as AirflowExceptionWithSlackNotification
        from pre_execute (not silently truncated or ignored).
        """
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"

        operator = HiveNextflowOperator(
            task_id="hive_task",
            bash_command="init_pipeline.pl MyPipeline::Conf",
            job_name="a" * 81,
        )

        with patch('ensemblslurm.operators.ensembl_bash.EnsemblSlackNotifier'), \
             patch('ensemblslurm.operators.ensembl_bash.Variable') as mock_bash_var:
            mock_bash_var.get.return_value = False
            with pytest.raises(AirflowExceptionWithSlackNotification, match="exceeds max length of 80"):
                operator.pre_execute(mock_context)

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_pre_execute_error_raises_with_slack_notification(self, mock_factory, mock_context):
        """Test that a failure during pre_execute is wrapped in AirflowExceptionWithSlackNotification."""
        failing_builder = Mock()
        failing_builder.build_command.side_effect = ValueError("boom")

        operator = HiveNextflowOperator(
            task_id="hive_task",
            bash_command="init_pipeline.pl MyPipeline::Conf",
            command_builder=failing_builder,
        )

        with patch('ensemblslurm.operators.ensembl_bash.EnsemblSlackNotifier'), \
             patch('ensemblslurm.operators.ensembl_bash.Variable') as mock_var:
            mock_var.get.return_value = False
            with pytest.raises(AirflowExceptionWithSlackNotification):
                operator.pre_execute(mock_context)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
