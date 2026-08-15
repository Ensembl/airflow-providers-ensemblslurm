"""
Test suite for DynamicNextflowCommandBuilder and NextflowOperator.
"""

import os
import pytest
from unittest.mock import Mock, patch

from ensemblslurm.operators.nextflow import (
    DynamicNextflowCommandBuilder,
    NextflowOperator,
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
    context['ti'].task.task_type = 'NextflowOperator'
    context['task_instance'] = context['ti']

    context['dag_run'].dag_id = 'test_dag'
    context['dag_run'].run_id = 'manual__20240101t123045'
    context['dag_run'].conf = {
        'genome_uuid': ['uuid-123'],
        'skip_pipeline': [],
    }

    return context


# ============================================================================
# TEST DynamicNextflowCommandBuilder
# ============================================================================

class TestDynamicNextflowCommandBuilder:
    """Test suite for DynamicNextflowCommandBuilder class."""

    @patch('ensemblslurm.operators.nextflow.Variable')
    def test_build_command_basic(self, mock_variable, mock_context):
        """Test building a basic Nextflow command."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['params'] = {
            'work_dir': '/test/work',
            'web_log_uri': 'https://log.ebi.ac.uk',
        }

        builder = DynamicNextflowCommandBuilder()
        command = builder.build_command(
            base_command="nextflow run main.nf",
            job_name="test_job",
            context=mock_context,
        )

        assert "nextflow run main.nf" in command
        assert "-work-dir /test/work" in command
        assert "-name test_job_1" in command
        assert "-resume" in command
        assert "export NXF_WORK=" in command
        assert "mkdir -p" in command

    @patch('ensemblslurm.operators.nextflow.Variable')
    def test_build_command_missing_work_dir_raises(self, mock_variable, mock_context):
        """Test that missing work_dir raises ValueError."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['params'] = {'web_log_uri': 'https://log.ebi.ac.uk'}

        builder = DynamicNextflowCommandBuilder()

        with pytest.raises(ValueError, match="Missing required params"):
            builder.build_command(
                base_command="nextflow run main.nf",
                job_name="test_job",
                context=mock_context,
            )

    @patch('ensemblslurm.operators.nextflow.Variable')
    def test_build_command_missing_web_log_uri_raises(self, mock_variable, mock_context):
        """Test that missing web_log_uri raises ValueError."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['params'] = {'work_dir': '/test/work'}

        builder = DynamicNextflowCommandBuilder()

        with pytest.raises(ValueError, match="Missing required params"):
            builder.build_command(
                base_command="nextflow run main.nf",
                job_name="test_job",
                context=mock_context,
            )

    @patch('ensemblslurm.operators.nextflow.Variable')
    def test_build_command_includes_genome_uuid(self, mock_variable, mock_context):
        """Test that genome_uuid from dag_run conf is passed as a comma-separated flag."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['params'] = {
            'work_dir': '/test/work',
            'web_log_uri': 'https://log.ebi.ac.uk',
        }
        mock_context['dag_run'].conf = {'genome_uuid': ['uuid-1', 'uuid-2']}

        builder = DynamicNextflowCommandBuilder()
        command = builder.build_command(
            base_command="nextflow run main.nf",
            job_name="test_job",
            context=mock_context,
        )

        assert "--genome_uuid uuid-1,uuid-2" in command

    @patch('ensemblslurm.operators.nextflow.Variable')
    def test_build_command_includes_antispecies(self, mock_variable, mock_context):
        """Test that antispecies from dag_run conf is passed as a comma-separated flag."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['params'] = {
            'work_dir': '/test/work',
            'web_log_uri': 'https://log.ebi.ac.uk',
        }
        mock_context['dag_run'].conf = {'antispecies': ['sp1', 'sp2']}

        builder = DynamicNextflowCommandBuilder()
        command = builder.build_command(
            base_command="nextflow run main.nf",
            job_name="test_job",
            context=mock_context,
        )

        assert "--antispecies sp1,sp2" in command

    @patch('ensemblslurm.operators.nextflow.Variable')
    def test_build_command_scalar_dynamic_value(self, mock_variable, mock_context):
        """Test that a scalar (non-list) dynamic value is stringified rather than joined."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['params'] = {
            'work_dir': '/test/work',
            'web_log_uri': 'https://log.ebi.ac.uk',
        }
        mock_context['dag_run'].conf = {'genome_uuid': 'single-uuid'}

        builder = DynamicNextflowCommandBuilder()
        command = builder.build_command(
            base_command="nextflow run main.nf",
            job_name="test_job",
            context=mock_context,
        )

        assert "--genome_uuid single-uuid" in command

    @patch('ensemblslurm.operators.nextflow.Variable')
    def test_build_command_no_dynamic_args_when_absent(self, mock_variable, mock_context):
        """Test that no --genome_uuid/--antispecies flags appear when absent from conf."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['params'] = {
            'work_dir': '/test/work',
            'web_log_uri': 'https://log.ebi.ac.uk',
        }
        mock_context['dag_run'].conf = {}

        builder = DynamicNextflowCommandBuilder()
        command = builder.build_command(
            base_command="nextflow run main.nf",
            job_name="test_job",
            context=mock_context,
        )

        assert "--genome_uuid" not in command
        assert "--antispecies" not in command

    @patch('ensemblslurm.operators.nextflow.Variable')
    def test_build_command_includes_report_and_trace(self, mock_variable, mock_context):
        """Test that -with-report and -with-trace paths are included."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['params'] = {
            'work_dir': '/test/work',
            'web_log_uri': 'https://log.ebi.ac.uk',
        }

        builder = DynamicNextflowCommandBuilder()
        command = builder.build_command(
            base_command="nextflow run main.nf",
            job_name="test_job",
            context=mock_context,
        )

        assert "-with-report /test/work/test_job_1_report.html" in command
        assert "-with-trace /test/work/test_job_1_trace.txt" in command

    @patch('ensemblslurm.operators.nextflow.Variable')
    def test_build_command_try_number_in_name(self, mock_variable, mock_context):
        """Test that retry number is reflected in the -name flag."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"
        mock_context['ti'].try_number = 5
        mock_context['params'] = {
            'work_dir': '/test/work',
            'web_log_uri': 'https://log.ebi.ac.uk',
        }

        builder = DynamicNextflowCommandBuilder()
        command = builder.build_command(
            base_command="nextflow run main.nf",
            job_name="test_job",
            context=mock_context,
        )

        assert "-name test_job_5" in command


# ============================================================================
# TEST NextflowOperator
# ============================================================================

class TestNextflowOperator:
    """Test suite for NextflowOperator class."""

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_default_command_builder(self, mock_factory):
        """Test that a DynamicNextflowCommandBuilder is used by default."""
        operator = NextflowOperator(
            task_id="nf_task",
            bash_command="nextflow run main.nf",
        )

        assert isinstance(operator.command_builder, DynamicNextflowCommandBuilder)

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_custom_command_builder_injected(self, mock_factory):
        """Test that an injected command_builder is used instead of the default."""
        custom_builder = Mock()

        operator = NextflowOperator(
            task_id="nf_task",
            bash_command="nextflow run main.nf",
            command_builder=custom_builder,
        )

        assert operator.command_builder is custom_builder

    @patch('ensemblslurm.operators.nextflow.Variable')
    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_pre_execute_builds_command(self, mock_factory, mock_variable, mock_context):
        """Test pre_execute prepares the Nextflow command and sets ensembl_cmd."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"

        operator = NextflowOperator(
            task_id="nf_task",
            bash_command="nextflow run main.nf",
        )

        operator.pre_execute(mock_context)

        assert operator.ensembl_cmd is not None
        assert "nextflow run main.nf" in operator.ensembl_cmd
        assert operator.job_name is not None
        assert mock_context['params']['work_dir'] == os.path.join(operator.cwd, operator.job_name)

    @patch('ensemblslurm.operators.nextflow.Variable')
    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_pre_execute_uses_custom_job_name(self, mock_factory, mock_variable, mock_context):
        """Test pre_execute honors an explicitly provided job_name."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"

        operator = NextflowOperator(
            task_id="nf_task",
            bash_command="nextflow run main.nf",
            job_name="my_nf_job",
        )

        operator.pre_execute(mock_context)

        assert operator.job_name == "my_nf_job"
        assert "-name my_nf_job_1" in operator.ensembl_cmd

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_pre_execute_error_raises_with_slack_notification(self, mock_factory, mock_context):
        """Test that a failure during pre_execute is wrapped in AirflowExceptionWithSlackNotification."""
        failing_builder = Mock()
        failing_builder.build_command.side_effect = ValueError("boom")

        operator = NextflowOperator(
            task_id="nf_task",
            bash_command="nextflow run main.nf",
            command_builder=failing_builder,
        )

        with patch('ensemblslurm.operators.ensembl_bash.EnsemblSlackNotifier'), \
             patch('ensemblslurm.operators.ensembl_bash.Variable') as mock_var:
            mock_var.get.return_value = False
            with pytest.raises(AirflowExceptionWithSlackNotification):
                operator.pre_execute(mock_context)

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_on_kill_no_subprocess_hook(self, mock_factory):
        """Test on_kill does not raise when no subprocess_hook is set."""
        operator = NextflowOperator(
            task_id="nf_task",
            bash_command="nextflow run main.nf",
        )

        # Should not raise
        operator.on_kill()

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'}, clear=False)
    def test_on_kill_sends_sigterm_when_subprocess_hook_present(self, mock_factory):
        """Test on_kill delegates to subprocess_hook.send_sigterm when present."""
        operator = NextflowOperator(
            task_id="nf_task",
            bash_command="nextflow run main.nf",
        )
        operator.subprocess_hook = Mock()

        operator.on_kill()

        operator.subprocess_hook.send_sigterm.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
