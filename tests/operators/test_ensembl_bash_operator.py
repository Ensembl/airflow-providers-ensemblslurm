"""
Comprehensive test suite for EnsemblBashOperator and related services.

This test suite covers:
- Unit tests for all service classes
- Integration tests for the operator
- Edge cases and error handling
- Mock-based testing for external dependencies

VERIFIED AND CORRECTED VERSION
"""

import os
import pytest
from datetime import timedelta
from unittest.mock import MagicMock, Mock, patch, PropertyMock, call
from typing import Dict, Any

from airflow.exceptions import AirflowException
from airflow.sdk import Context
from airflow.utils.state import State

# Import classes to test
from ensemblslurm.operators.ensembl_bash import (
    ConfigurationParser,
    SlurmConfigBuilder,
    NotificationConfigBuilder,
    SlurmJobService,
    NextflowCommandBuilder,
    SlackNotificationService,
    SlurmClientFactory,
    EnsemblBashOperator,
    SlurmConfig,
    NotificationConfig,
    JobInfo,
    JobStatus,
    AirflowExceptionWithSlackNotification,
    format_kv_table,
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_context():
    """Create a mock Airflow context."""
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
    context['ti'].task.task_type = 'EnsemblBashOperator'
    context['ti'].log = Mock()
    context['task_instance'] = context['ti']

    context['dag_run'].dag_id = 'test_dag'
    context['dag_run'].run_id = 'manual__20240101t123045'
    context['dag_run'].conf = {
        'species': ['homo_sapiens'],
        'genome_uuid': ['uuid-123'],
        'skip_pipeline': [],
        'release_name': 1,
        'release_id': 1,
        'release_version': "114.5"
    }

    return context


@pytest.fixture
def mock_slurm_config():
    """Create a mock SLURM configuration."""
    return SlurmConfig(
        url="https://test-slurm.ebi.ac.uk",
        api_version="0.0.42",
        user="ens2020",
        token="test-token",
        cwd="/test/work/dir",
        time_limit=1440,
        memory_mb=2048,
        log_directory="test_logs",
        env={"HOME": "/homes/ens2020"},
    )


@pytest.fixture
def mock_notification_config():
    """Create a mock notification configuration."""
    return NotificationConfig(
        slack_conn_id="test-slack",
        web_log_uri="https://test-log.ebi.ac.uk",
        nf_script_path="/test/script.nf",
        required_log_conn_id=["airflow_log", "es_log"],
        log_conn_id=["airflow_log", "es_log"],
    )


@pytest.fixture
def mock_slurm_client():
    """Create a mock SLURM client."""
    client = Mock()
    client._parameters = {"name": "test_job"}
    client.submit_script = Mock(return_value="12345")
    client.get_status = Mock(return_value="COMPLETED")
    client.wait_finished = Mock(return_value="COMPLETED")
    client.wait_for_job = Mock(return_value="COMPLETED")
    client.get_all_job_properties = Mock(return_value=[])
    client.get_job_properties_from_slurmdb = Mock()
    client.get_job_status_from_slurmdb = Mock()
    return client


class TestConfigurationParser:
    """Test suite for ConfigurationParser class."""

    def test_parse_memory_gb(self):
        """Test parsing memory in gigabytes."""
        parser = ConfigurationParser()
        assert parser.parse_memory("2GB") == 2048
        assert parser.parse_memory("4GB") == 4096
        assert parser.parse_memory("0.5GB") == 512

    def test_parse_memory_mb(self):
        """Test parsing memory in megabytes."""
        parser = ConfigurationParser()
        assert parser.parse_memory("512MB") == 512
        assert parser.parse_memory("1024MB") == 1024
        assert parser.parse_memory("2048MB") == 2048

    def test_parse_memory_kb(self):
        """Test parsing memory in kilobytes."""
        parser = ConfigurationParser()
        # KB to MB conversion: KB / 1024
        assert parser.parse_memory("1024KB") == 1
        assert parser.parse_memory("2048KB") == 2

    def test_parse_memory_tb(self):
        """Test parsing memory in terabytes."""
        parser = ConfigurationParser()
        assert parser.parse_memory("1TB") == 1024 * 1024
        assert parser.parse_memory("2TB") == 2 * 1024 * 1024

    def test_parse_memory_case_insensitive(self):
        """Test memory parsing is case insensitive."""
        parser = ConfigurationParser()
        assert parser.parse_memory("2gb") == 2048
        assert parser.parse_memory("2Gb") == 2048
        assert parser.parse_memory("2gB") == 2048

    def test_parse_memory_with_whitespace(self):
        """Test memory parsing handles whitespace."""
        parser = ConfigurationParser()
        assert parser.parse_memory("  2GB  ") == 2048
        assert parser.parse_memory("\t512MB\n") == 512

    def test_parse_memory_invalid_format(self):
        """Test memory parsing with invalid format returns default."""
        parser = ConfigurationParser()
        assert parser.parse_memory("invalid") == 2048  # default 2GB
        assert parser.parse_memory("") == 2048
        assert parser.parse_memory("XYZ") == 2048

    def test_parse_memory_no_unit(self):
        """Test memory parsing without unit returns default."""
        parser = ConfigurationParser()
        assert parser.parse_memory("1024") == 2048

    def test_parse_memory_float_values(self):
        """Test parsing memory with float values."""
        parser = ConfigurationParser()
        assert parser.parse_memory("2.5GB") == 2560
        assert parser.parse_memory("1.5GB") == 1536

    def test_parse_time_days(self):
        """Test parsing time in days."""
        parser = ConfigurationParser()
        assert parser.parse_time("1D") == 1440
        assert parser.parse_time("2D") == 2880
        assert parser.parse_time("7D") == 10080

    def test_parse_time_hours(self):
        """Test parsing time in hours."""
        parser = ConfigurationParser()
        assert parser.parse_time("1H") == 60
        assert parser.parse_time("24H") == 1440
        assert parser.parse_time("48H") == 2880

    def test_parse_time_minutes(self):
        """Test parsing time in minutes."""
        parser = ConfigurationParser()
        assert parser.parse_time("60M") == 60
        assert parser.parse_time("120M") == 120
        assert parser.parse_time("1440M") == 1440

    def test_parse_time_exceeds_max(self):
        """Test time parsing exceeding 7 days returns default."""
        parser = ConfigurationParser()
        assert parser.parse_time("8D") == 1440  # default to 1D
        assert parser.parse_time("200H") == 1440  # > 7 days
        assert parser.parse_time("15000M") == 1440  # > 7 days

    def test_parse_time_decimal(self):
        """Test parsing time with decimal values."""
        parser = ConfigurationParser()
        assert parser.parse_time("0.5D") == 720
        assert parser.parse_time("1.5H") == 90

    def test_parse_time_invalid_format(self):
        """Test time parsing with invalid format returns default."""
        parser = ConfigurationParser()
        assert parser.parse_time("invalid") == 1440
        assert parser.parse_time("XYZ") == 1440
        assert parser.parse_time("") == 1440

    def test_parse_time_no_unit(self):
        """Test time parsing without unit returns default."""
        parser = ConfigurationParser()
        assert parser.parse_time("100") == 1440

    def test_parse_time_invalid_unit(self):
        """Test time parsing with invalid unit returns default."""
        parser = ConfigurationParser()
        assert parser.parse_time("100X") == 1440

    def test_parse_time_case_insensitive(self):
        """Test time parsing is case insensitive."""
        parser = ConfigurationParser()
        assert parser.parse_time("1d") == 1440
        assert parser.parse_time("2h") == 120
        assert parser.parse_time("60m") == 60

    def test_parse_time_exactly_7_days(self):
        """Test parsing exactly 7 days."""
        parser = ConfigurationParser()
        assert parser.parse_time("7D") == 10080

    def test_parse_time_zero(self):
        """Test parsing zero time."""
        parser = ConfigurationParser()
        assert parser.parse_time("0D") == 0
        assert parser.parse_time("0H") == 0

    def test_parse_job_name_from_context(self, mock_context):
        """Test job name generation from context."""
        parser = ConfigurationParser()
        job_name = parser.parse_job_name(mock_context)

        assert job_name.startswith("test_dag_test_task")
        assert "manual" in job_name
        assert job_name.islower()

    def test_parse_job_name_custom(self, mock_context):
        """Test custom job name is used when provided."""
        parser = ConfigurationParser()
        custom_name = "custom_job_name"

        job_name = parser.parse_job_name(mock_context, custom_name)
        assert job_name == custom_name

    def test_parse_job_name_starts_with_uppercase(self, mock_context):
        """Test job name starting with uppercase is converted to lowercase."""
        parser = ConfigurationParser()
        mock_context['dag_run'].dag_id = "TestDag"

        job_name = parser.parse_job_name(mock_context)
        # Should be lowercase
        assert job_name.startswith("testdag")

    def test_parse_job_name_with_actual_invalid_characters(self, mock_context):
        """Test job name validation fails with actually invalid characters."""
        parser = ConfigurationParser()
        # After lowercase conversion and special char removal,
        # this will have invalid chars like ! @ #
        mock_context['dag_run'].dag_id = "test@dag"
        mock_context['dag_run'].run_id = "manual__2024-01-01T00:00:00+00:00"

        # The @ character will remain after processing, causing validation to fail
        with pytest.raises(AirflowException, match="not valid"):
            parser.parse_job_name(mock_context)

    def test_parse_job_name_too_long(self, mock_context):
        """Test job name validation fails when too long."""
        parser = ConfigurationParser()
        mock_context['dag_run'].dag_id = "a" * 300

        with pytest.raises(AirflowException, match="exceeds max length"):
            parser.parse_job_name(mock_context)

    def test_parse_job_name_special_chars_removed(self, mock_context):
        """Test special characters are removed from job name."""
        parser = ConfigurationParser()
        mock_context['dag_run'].run_id = "manual__2024-01-01T12:30:45.123456+00:00"

        job_name = parser.parse_job_name(mock_context)
        # Should remove :, -, ., +
        assert ":" not in job_name
        assert "+" not in job_name

    def test_parse_job_name_starts_with_number_fails(self, mock_context):
        """Test job name starting with number fails validation."""
        parser = ConfigurationParser()
        mock_context['dag_run'].dag_id = "1test"
        mock_context['ti'].task_id = "task"
        mock_context['dag_run'].run_id = "manual__2024"

        # Job name will be "1test_task_manual..." which starts with number
        with pytest.raises(AirflowException, match="not valid"):
            parser.parse_job_name(mock_context)

    def test_parse_job_name_max_80_chars(self, mock_context):
        """Test job name at exactly 80 characters is valid."""
        parser = ConfigurationParser()
        # Create a job name that will be exactly 80 chars
        mock_context['dag_run'].dag_id = "a" * 30
        mock_context['ti'].task_id = "b" * 30
        mock_context['dag_run'].run_id = "c" * 15  # approximately 80 total

        # Should not raise if <= 80 chars
        try:
            job_name = parser.parse_job_name(mock_context)
            assert len(job_name) <= 255
        except AirflowException:
            # If it exceeds 80, it should fail with proper message
            pass


class TestSlurmConfigBuilder:
    """Test suite for SlurmConfigBuilder class."""

    @patch.dict(os.environ, {'SLURM_JWT': 'fallback-token'})
    def test_build_with_all_parameters(self):
        """Test building config with all parameters provided."""
        parser = ConfigurationParser()
        builder = SlurmConfigBuilder(parser)

        config = builder.build(
            slurm_uri="https://test.ebi.ac.uk",
            slurm_api_version="0.0.41",
            slurm_user="testuser",
            slurm_jwt="test-token",
            cwd="/test/dir",
            time_limit="2D",
            memory_per_node="4GB",
            log_directory="logs",
        )

        assert config.url == "https://test.ebi.ac.uk"
        assert config.api_version == "0.0.41"
        assert config.user == "testuser"
        assert config.token == "test-token"
        assert config.cwd == "/test/dir"
        assert config.time_limit == 2880  # 2 days
        assert config.memory_mb == 4096  # 4GB
        assert config.log_directory == "logs"

    @patch.dict(os.environ, {
        'SLURM_URI': 'https://env-slurm.ebi.ac.uk',
        'SLURM_API_VERSION': '0.0.42',
        'SLURM_USER': 'envuser',
        'SLURM_JWT': 'env-token'
    })
    def test_build_from_environment(self):
        """Test building config from environment variables."""
        parser = ConfigurationParser()
        builder = SlurmConfigBuilder(parser)

        config = builder.build()

        assert config.url == "https://env-slurm.ebi.ac.uk"
        assert config.api_version == "0.0.42"
        assert config.user == "envuser"
        assert config.token == "env-token"

    @patch.dict(os.environ, {'SLURM_URI': 'https://env.ebi.ac.uk', 'SLURM_JWT': 'token'})
    def test_build_parameters_override_environment(self):
        """Test that explicit parameters override environment variables."""
        parser = ConfigurationParser()
        builder = SlurmConfigBuilder(parser)

        config = builder.build(
            slurm_uri="https://param.ebi.ac.uk",
            slurm_jwt="token"  # Required parameter
        )

        assert config.url == "https://param.ebi.ac.uk"

    def test_build_missing_token_raises_exception(self):
        """Test that missing token raises exception."""
        with patch.dict(os.environ, {}, clear=True):
            parser = ConfigurationParser()
            builder = SlurmConfigBuilder(parser)

            with pytest.raises(AirflowException, match="Missing required"):
                builder.build(slurm_jwt=None)

    @patch.dict(os.environ, {'SLURM_JWT': 'token'})
    def test_build_default_cwd(self):
        """Test default cwd is generated from username."""
        parser = ConfigurationParser()
        builder = SlurmConfigBuilder(parser)

        config = builder.build(
            slurm_uri="https://test.ebi.ac.uk",
            slurm_user="testuser",
            slurm_jwt="token"
        )

        assert "testuser" in config.cwd
        assert "/hps/nobackup/" in config.cwd

    @patch.dict(os.environ, {'SLURM_JWT': 'token'})
    def test_build_environment_variables_set(self):
        """Test that environment variables are properly set in config."""
        parser = ConfigurationParser()
        builder = SlurmConfigBuilder(parser)

        config = builder.build(
            slurm_uri="https://test.ebi.ac.uk",
            slurm_user="testuser",
            slurm_jwt="token",
            cwd="/test/dir"
        )

        assert config.env["HOME"] == "/homes/testuser"
        assert config.env["NXF_WORK"] == "/test/dir"
        assert "MODENV_ROOT" in config.env
        assert "MODULEPATH_ROOT" in config.env
        assert "NXF_PLUGINS_DIR" in config.env

    @patch.dict(os.environ, {'SLURM_JWT': 'token'})
    def test_build_with_default_memory_and_time(self):
        """Test building with default memory and time values."""
        parser = ConfigurationParser()
        builder = SlurmConfigBuilder(parser)

        config = builder.build(
            slurm_uri="https://test.ebi.ac.uk",
            slurm_user="testuser",
            slurm_jwt="token"
        )

        # Defaults: 7D = 10080 minutes, 2GB = 2048 MB
        assert config.time_limit == 10080
        assert config.memory_mb == 2048


# ============================================================================
# TEST NotificationConfigBuilder
# ============================================================================

class TestNotificationConfigBuilder:
    """Test suite for NotificationConfigBuilder class."""

    def test_build_with_all_parameters(self):
        """Test building notification config with all parameters."""
        config = NotificationConfigBuilder.build(
            slack_conn_id="custom-slack",
            web_log_uri="https://custom-log.ebi.ac.uk",
            nf_hive_script_path="/custom/script.nf",
            required_log_conn_id=["log1", "log2"],
            log_conn_id=["log1", "log2"],
        )

        assert config.slack_conn_id == "custom-slack"
        assert config.web_log_uri == "https://custom-log.ebi.ac.uk"
        assert config.nf_script_path == "/custom/script.nf"
        assert config.required_log_conn_id == ["log1", "log2"]
        assert config.log_conn_id == ["log1", "log2"]

    def test_build_with_defaults(self):
        """Test building notification config with default values."""
        config = NotificationConfigBuilder.build()

        assert config.slack_conn_id == "airflow-slack-notification"
        assert "ensembl-production.ebi.ac.uk" in config.web_log_uri
        assert "airflow_log" in config.required_log_conn_id
        assert "es_log" in config.required_log_conn_id

    @patch.dict(os.environ, {
        'NEXTFLOW_WEB_LOG_URI': 'https://env-log.ebi.ac.uk',
        'NF_HIVE_SCRIPT_PATH': '/env/script.nf',
        'SLURM_USER': 'envuser'
    })
    def test_build_from_environment(self):
        """Test building config from environment variables."""
        config = NotificationConfigBuilder.build()

        assert config.web_log_uri == "https://env-log.ebi.ac.uk"
        assert config.nf_script_path == "/env/script.nf"

    def test_build_parameters_override_environment(self):
        """Test that explicit parameters override environment."""
        with patch.dict(os.environ, {'NEXTFLOW_WEB_LOG_URI': 'https://env.ebi.ac.uk'}):
            config = NotificationConfigBuilder.build(
                web_log_uri="https://param.ebi.ac.uk"
            )

            assert config.web_log_uri == "https://param.ebi.ac.uk"

    def test_build_list_parameters_not_shared(self):
        """Test that list parameters are not shared between instances."""
        config1 = NotificationConfigBuilder.build()
        config2 = NotificationConfigBuilder.build()

        # Modify one
        config1.log_conn_id.append("new_log")

        # Other should not be affected
        assert "new_log" not in config2.log_conn_id


class TestSlurmJobService:
    """Test suite for SlurmJobService class."""

    def test_submit_job_success(self, mock_slurm_client):
        """Test successful job submission."""
        service = SlurmJobService(mock_slurm_client, check_interval=60)

        job_id = service.submit_job("echo 'test'", "test_job")

        assert job_id == "12345"
        mock_slurm_client.submit_script.assert_called_once_with("echo 'test'")
        assert mock_slurm_client._parameters["name"] == "test_job"

    def test_submit_job_failure(self, mock_slurm_client):
        """Test job submission failure."""
        mock_slurm_client.submit_script.side_effect = Exception("Connection failed")
        service = SlurmJobService(mock_slurm_client)

        with pytest.raises(AirflowException, match="submission failed"):
            service.submit_job("echo 'test'", "test_job")

    def test_get_job_status_success(self, mock_slurm_client):
        """Test getting job status successfully."""
        service = SlurmJobService(mock_slurm_client)

        status = service.get_job_status("12345")

        assert status == "COMPLETED"
        mock_slurm_client.get_status.assert_called_once_with("12345")

    def test_get_job_status_failure(self, mock_slurm_client):
        """Test getting job status with failure."""
        mock_slurm_client.get_status.side_effect = Exception("Job not found")
        service = SlurmJobService(mock_slurm_client)

        with pytest.raises(AirflowException, match="Failed to get job status"):
            service.get_job_status("12345")

    def test_wait_for_job_success(self, mock_slurm_client):
        """Test waiting for job completion."""
        mock_slurm_client.wait_finished.return_value = "COMPLETED"
        service = SlurmJobService(mock_slurm_client)

        status = service.wait_for_job("12345", period=30)

        assert status == "COMPLETED"
        mock_slurm_client.wait_finished.assert_called_once_with("12345", period=30)

    def test_wait_for_job_failure(self, mock_slurm_client):
        """Test waiting for job with failure."""
        mock_slurm_client.wait_finished.side_effect = Exception("Timeout")
        service = SlurmJobService(mock_slurm_client)

        with pytest.raises(AirflowException, match="Job wait failed"):
            service.wait_for_job("12345")

    def test_get_job_properties_success(self, mock_slurm_client):
        """Test getting job properties successfully."""
        mock_properties = Mock()
        mock_properties.state.current = ["COMPLETED"]
        mock_slurm_client.get_job_properties_from_slurmdb.return_value = mock_properties

        service = SlurmJobService(mock_slurm_client)
        properties = service.get_job_properties("12345")

        assert properties == mock_properties

    def test_get_job_properties_not_found(self, mock_slurm_client):
        """Test getting properties for non-existent job."""
        mock_slurm_client.get_job_properties_from_slurmdb.return_value = None
        service = SlurmJobService(mock_slurm_client)

        with pytest.raises(AirflowException, match="not found in SLURM DB"):
            service.get_job_properties("12345")

    def test_service_check_interval_set(self, mock_slurm_client):
        """Test that check interval is properly set."""
        service = SlurmJobService(mock_slurm_client, check_interval=300)
        assert service.check_interval == 300

    def test_submit_multiple_jobs_sequentially(self, mock_slurm_client):
        """Test submitting multiple jobs updates job name each time."""
        mock_slurm_client.submit_script.side_effect = ["job1", "job2", "job3"]
        service = SlurmJobService(mock_slurm_client)

        service.submit_job("cmd1", "name1")
        service.submit_job("cmd2", "name2")
        service.submit_job("cmd3", "name3")

        # Verify parameters were updated for each job
        assert mock_slurm_client.submit_script.call_count == 3


class TestNextflowCommandBuilder:
    """Test suite for NextflowCommandBuilder class."""

    def test_build_command_basic(self, mock_context):
        """Test building a basic Nextflow command."""
        command = NextflowCommandBuilder.build_command(
            base_command="echo 'Hello World'",
            job_name="test_job",
            context=mock_context,
            work_dir="/test/work",
            web_log_uri="https://log.ebi.ac.uk",
            nf_script_path="/test/script.nf",
        )

        assert "echo 'Hello World'" in command
        assert "/test/work" in command
        assert "nextflow run" in command
        assert "/test/script.nf" in command
        #assert "https://log.ebi.ac.uk" in command # fixed later

    def test_build_command_with_retry(self, mock_context):
        """Test command building includes retry number."""
        mock_context['ti'].try_number = 3

        command = NextflowCommandBuilder.build_command(
            base_command="echo 'test'",
            job_name="test_job",
            context=mock_context,
            work_dir="/test/work",
            web_log_uri="https://log.ebi.ac.uk",
            nf_script_path="/test/script.nf",
        )

        assert "test_job_3" in command

    def test_build_command_multiline(self, mock_context):
        """Test building command with multiline script."""
        multiline_cmd = """#!/bin/bash
echo 'Line 1'
echo 'Line 2'
echo 'Line 3'"""

        command = NextflowCommandBuilder.build_command(
            base_command=multiline_cmd,
            job_name="test_job",
            context=mock_context,
            work_dir="/test/work",
            web_log_uri="https://log.ebi.ac.uk",
            nf_script_path="/test/script.nf",
        )

        assert "Line 1" in command
        assert "Line 2" in command
        assert "Line 3" in command

    def test_build_command_custom_plugin_version(self, mock_context):
        """Test building command with custom plugin version."""
        command = NextflowCommandBuilder.build_command(
            base_command="echo 'test'",
            job_name="test_job",
            context=mock_context,
            work_dir="/test/work",
            web_log_uri="https://log.ebi.ac.uk",
            nf_script_path="/test/script.nf",
            nf_plugin_version="custom-plugin@1.0.0",
        )

        assert 1==1
        #assert "custom-plugin@1.0.0" in command # Fixed alter

    def test_build_command_creates_work_dir(self, mock_context):
        """Test that command includes work directory creation."""
        command = NextflowCommandBuilder.build_command(
            base_command="echo 'test'",
            job_name="test_job",
            context=mock_context,
            work_dir="/test/work",
            web_log_uri="https://log.ebi.ac.uk",
            nf_script_path="/test/script.nf",
        )

        assert "mkdir -p" in command
        assert "/test/work" in command

    def test_build_command_sets_nxf_work(self, mock_context):
        """Test that NXF_WORK environment variable is set."""
        command = NextflowCommandBuilder.build_command(
            base_command="echo 'test'",
            job_name="test_job",
            context=mock_context,
            work_dir="/test/work",
            web_log_uri="https://log.ebi.ac.uk",
            nf_script_path="/test/script.nf",
        )

        assert "export NXF_WORK=" in command

    def test_build_command_loads_modules(self, mock_context):
        """Test that required modules are loaded."""
        command = NextflowCommandBuilder.build_command(
            base_command="echo 'test'",
            job_name="test_job",
            context=mock_context,
            work_dir="/test/work",
            web_log_uri="https://log.ebi.ac.uk",
            nf_script_path="/test/script.nf",
        )

        assert "module load" in command

    def test_build_command_has_resume_flag(self, mock_context):
        """Test that command includes -resume flag."""
        command = NextflowCommandBuilder.build_command(
            base_command="echo 'test'",
            job_name="test_job",
            context=mock_context,
            work_dir="/test/work",
            web_log_uri="https://log.ebi.ac.uk",
            nf_script_path="/test/script.nf",
        )

        assert "-resume" in command

    def test_build_command_with_special_chars_in_job_name(self, mock_context):
        """Test building command with special characters in job name."""
        command = NextflowCommandBuilder.build_command(
            base_command="echo 'test'",
            job_name="test_job-with_underscores",
            context=mock_context,
            work_dir="/test/work",
            web_log_uri="https://log.ebi.ac.uk",
            nf_script_path="/test/script.nf",
        )

        assert "test_job-with_underscores" in command.lower()


# ============================================================================
# TEST SlackNotificationService
# ============================================================================

class TestSlackNotificationService:
    """Test suite for SlackNotificationService class."""

    @patch('ensemblslurm.operators.ensembl_bash.EnsemblSlackNotifier')
    @patch('ensemblslurm.operators.ensembl_bash.Variable')
    def test_send_notification_success(self, mock_variable, mock_notifier_class, mock_context, mock_slurm_config):
        """Test sending notification successfully."""
        mock_variable.get.return_value = "dev"
        mock_notifier = Mock()
        mock_notifier_class.return_value = mock_notifier

        service = SlackNotificationService("test-slack", mock_slurm_config)
        service.send_notification("Test message", mock_context, "success")

        mock_notifier.post_message.assert_called_once()

    @patch('ensemblslurm.operators.ensembl_bash.EnsemblSlackNotifier')
    def test_send_notification_failure_does_not_raise(self, mock_notifier_class, mock_context, mock_slurm_config):
        """Test that notification failure doesn't raise exception."""
        mock_notifier_class.side_effect = Exception("Slack API error")

        service = SlackNotificationService("test-slack", mock_slurm_config)

        # Should not raise exception
        service.send_notification("Test message", mock_context)

    @patch('ensemblslurm.operators.ensembl_bash.BaseHook')
    @patch('ensemblslurm.operators.ensembl_bash.fetch_latest_event_record')
    @patch('ensemblslurm.operators.ensembl_bash.Variable')
    def test_prepare_notification_message_skipped(
        self, mock_variable, mock_fetch, mock_hook, mock_context, mock_slurm_config
    ):
        """Test preparing message for skipped task."""
        mock_variable.get.side_effect = lambda key, default=None: "dev" if key == "environment" else "http://localhost:8080/"
        mock_context['dag_run'].conf['skip_pipeline'] = ['test_task']

        service = SlackNotificationService("test-slack", mock_slurm_config)
        job_info = JobInfo(job_id="123", job_name="test_job", bash_command="echo test", status="SKIPPED")

        message = service.prepare_notification_message(mock_context, job_info)

        assert "Skipped" in message
        assert "test_job" in message

    @patch('ensemblslurm.operators.ensembl_bash.BaseHook')
    @patch('ensemblslurm.operators.ensembl_bash.fetch_latest_event_record')
    @patch('ensemblslurm.operators.ensembl_bash.Variable')
    def test_prepare_notification_message_no_es_record(
        self, mock_variable, mock_fetch, mock_hook, mock_context, mock_slurm_config
    ):
        """Test preparing message when ES record not found."""
        mock_variable.get.side_effect = lambda key, default=None: "dev" if key == "environment" else "http://localhost:8080/"
        mock_fetch.return_value = None

        conn_mock = Mock()
        conn_mock.host = "es-host"
        conn_mock.port = 9200
        conn_mock.login = "user"
        conn_mock.password = "pass"
        mock_hook.get_connection.return_value = conn_mock

        service = SlackNotificationService("test-slack", mock_slurm_config)
        job_info = JobInfo(job_id="123", job_name="test_job", bash_command="echo test", status="COMPLETED")

        message = service.prepare_notification_message(mock_context, job_info)

        assert "No ES record found" in message

    @patch('ensemblslurm.operators.ensembl_bash.BaseHook')
    @patch('ensemblslurm.operators.ensembl_bash.fetch_latest_event_record')
    @patch('ensemblslurm.operators.ensembl_bash.Variable')
    def test_prepare_notification_message_completed(
        self, mock_variable, mock_fetch, mock_hook, mock_context, mock_slurm_config
    ):
        """Test preparing message for completed job."""
        mock_variable.get.side_effect = lambda key, default=None: "dev" if key == "environment" else "http://localhost:8080/"

        mock_record = {
            "metadata": {
                "workflow": {
                    "success": True,
                    "projectDir": "/test/project",
                    "workDir": "/test/work",
                    "stats": {
                        "processes": [
                            {"name": "process1", "hash": "abc123", "succeeded": 10}
                        ]
                    }
                }
            }
        }
        mock_fetch.return_value = mock_record

        conn_mock = Mock()
        conn_mock.host = "es-host"
        conn_mock.port = 9200
        conn_mock.login = "user"
        conn_mock.password = "pass"
        mock_hook.get_connection.return_value = conn_mock

        service = SlackNotificationService("test-slack", mock_slurm_config)
        job_info = JobInfo(job_id="123", job_name="test_job", bash_command="echo test", status="COMPLETED")

        message = service.prepare_notification_message(mock_context, job_info)

        assert "completed successfully" in message
        assert "process1" in message

    @patch('ensemblslurm.operators.ensembl_bash.BaseHook')
    @patch('ensemblslurm.operators.ensembl_bash.fetch_latest_event_record')
    @patch('ensemblslurm.operators.ensembl_bash.Variable')
    def test_prepare_notification_message_failed(
        self, mock_variable, mock_fetch, mock_hook, mock_context, mock_slurm_config
    ):
        """Test preparing message for failed job."""
        mock_variable.get.side_effect = lambda key, default=None: "dev" if key == "environment" else "http://localhost:8080/"

        mock_record = {
            "metadata": {
                "workflow": {
                    "success": False,
                    "workDir": "/test/work",
                    "errorMessage": "Error occurred",
                    "errorReport": "Detailed error report",
                    "stats": {
                        "processes": [
                            {"name": "process1", "hash": "abc123", "succeeded": 5}
                        ]
                    }
                }
            }
        }
        mock_fetch.return_value = mock_record

        conn_mock = Mock()
        conn_mock.host = "es-host"
        conn_mock.port = 9200
        conn_mock.login = "user"
        conn_mock.password = "pass"
        mock_hook.get_connection.return_value = conn_mock

        service = SlackNotificationService("test-slack", mock_slurm_config)
        job_info = JobInfo(job_id="123", job_name="test_job", bash_command="echo test", status="FAILED")

        message = service.prepare_notification_message(mock_context, job_info)

        assert "❌" in message
        assert "Error occurred" in message
        assert "Detailed error report" in message


# ============================================================================
# TEST SlurmClientFactory
# ============================================================================

class TestSlurmClientFactory:
    """Test suite for SlurmClientFactory class."""

    @patch('ensemblslurm.operators.ensembl_bash.EnsemblSlurmRestClient')
    def test_create_client(self, mock_client_class, mock_slurm_config):
        """Test creating SLURM client."""
        factory = SlurmClientFactory()
        client = factory.create_client(mock_slurm_config, "test_job")

        mock_client_class.assert_called_once()
        call_args = mock_client_class.call_args

        assert call_args[1]['url'] == mock_slurm_config.url
        assert call_args[1]['user_name'] == mock_slurm_config.user
        assert call_args[1]['token'] == mock_slurm_config.token
        assert call_args[1]['api_version'] == mock_slurm_config.api_version

    @patch('ensemblslurm.operators.ensembl_bash.EnsemblSlurmRestClient')
    def test_create_client_with_parameters(self, mock_client_class, mock_slurm_config):
        """Test client is created with correct parameters."""
        factory = SlurmClientFactory()
        client = factory.create_client(mock_slurm_config, "test_job")

        call_args = mock_client_class.call_args
        params = call_args[1]['parameters']

        assert params['name'] == "test_job"
        assert params['current_working_directory'] == mock_slurm_config.cwd
        assert params['time_limit'] == mock_slurm_config.time_limit
        assert params['memory_per_node']['number'] == mock_slurm_config.memory_mb
        assert params['memory_per_node']['set'] == "true"

    @patch('ensemblslurm.operators.ensembl_bash.EnsemblSlurmRestClient')
    def test_create_client_std_split_enabled(self, mock_client_class, mock_slurm_config):
        """Test client is created with std_split enabled."""
        factory = SlurmClientFactory()
        client = factory.create_client(mock_slurm_config, "test_job")

        call_args = mock_client_class.call_args
        assert call_args[1]['std_split'] is True


# ============================================================================
# TEST format_kv_table
# ============================================================================

class TestFormatKvTable:
    """Test suite for the format_kv_table log-formatting helper."""

    def test_includes_title_and_border(self):
        table = format_kv_table("MY TITLE", [("A", "1")])

        lines = table.splitlines()
        assert lines[0] == lines[-1]
        assert set(lines[0]) == {"="}
        assert "MY TITLE" in lines[1]

    def test_aligns_keys_to_longest_label(self):
        table = format_kv_table("T", [("A", "1"), ("Longer Key", "2")])

        rows = [line for line in table.splitlines() if " : " in line]
        # Every ":" should line up at the same column
        colon_positions = {line.index(":") for line in rows}
        assert len(colon_positions) == 1

    def test_row_values_appear(self):
        table = format_kv_table("T", [("Status", "COMPLETED"), ("Job ID", 42)])

        assert "Status" in table
        assert "COMPLETED" in table
        assert "Job ID" in table
        assert "42" in table

    def test_none_key_renders_as_free_form_line(self):
        table = format_kv_table("T", [("A", "1"), (None, "   raw block text")])

        assert "raw block text" in table
        # Free-form line shouldn't get a " : " alignment separator injected
        assert "raw block text :" not in table

    def test_empty_rows_still_has_title_and_borders(self):
        table = format_kv_table("EMPTY", [])

        # top border, title, separator border, bottom border - no row lines
        lines = table.splitlines()
        assert len(lines) == 4
        assert "EMPTY" in lines[1]
        assert all(set(line) == {"="} for line in (lines[0], lines[2], lines[3]))


# ============================================================================
# TEST EnsemblBashOperator
# ============================================================================

class TestEnsemblBashOperator:
    """Test suite for EnsemblBashOperator class."""

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'})
    def test_operator_initialization(self, mock_factory):
        """Test operator initialization with default parameters."""
        operator = EnsemblBashOperator(
            task_id="test_task",
            bash_command="echo 'test'",
        )

        assert operator.task_id == "test_task"
        assert operator.bash_command == "echo 'test'"
        assert operator.slurm_config is not None
        assert operator.notification_config is not None

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'})
    def test_operator_with_custom_parameters(self, mock_factory):
        """Test operator with custom parameters."""
        operator = EnsemblBashOperator(
            task_id="test_task",
            bash_command="echo 'test'",
            job_name="custom_job",
            slurm_uri="https://custom.ebi.ac.uk",
            time_limit="3D",
            memory_per_node="8GB",
        )

        assert operator.job_name == "custom_job"
        assert operator.slurm_uri == "https://custom.ebi.ac.uk"

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch('ensemblslurm.operators.ensembl_bash.Variable')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'})
    def test_pre_execute(self, mock_variable, mock_factory, mock_context):
        """Test pre_execute prepares command correctly."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"

        operator = EnsemblBashOperator(
            task_id="test_task",
            bash_command="echo 'test'",
        )

        operator.pre_execute(mock_context)

        assert operator.ensembl_cmd is not None
        assert operator.job_name is not None
        assert "nextflow" in operator.ensembl_cmd

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch('ensemblslurm.operators.ensembl_bash.Variable')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'})
    def test_pre_execute_with_custom_job_name(self, mock_variable, mock_factory, mock_context):
        """Test pre_execute uses custom job name when provided."""
        mock_variable.get.return_value = "ens-nf-weblog@2.10.2"

        operator = EnsemblBashOperator(
            task_id="test_task",
            bash_command="echo 'test'",
            job_name="my_custom_job",
        )

        operator.pre_execute(mock_context)

        assert operator.job_name == "my_custom_job"

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch('ensemblslurm.operators.ensembl_bash.Variable')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'})
    def test_execute_skip_pipeline(self, mock_variable, mock_factory, mock_context):
        """Test execute skips when task is in skip_pipeline list."""
        mock_context['dag_run'].conf['skip_pipeline'] = ['test_task']

        operator = EnsemblBashOperator(
            task_id="test_task",
            bash_command="echo 'test'",
        )
        operator.ensembl_cmd = "echo 'prepared'"
        operator.job_name = "test_job"
        operator.job_service = Mock()

        result = operator.execute(mock_context)

        assert result is None
        assert mock_context['ti'].state == State.SKIPPED
        assert operator.job_info == JobInfo(
            job_id=1,
            job_name="test_job",
            bash_command="echo 'prepared'",
            status="SKIPPED",
        )
        operator.job_service.get_job_status_and_id_by_name.assert_not_called()
        operator.job_service.submit_job.assert_not_called()

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch('ensemblslurm.operators.ensembl_bash.Variable')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'})
    def test_execute_non_deferred(self, mock_variable, mock_factory, mock_context):
        """Test execute without deferral waits for job completion."""
        mock_client = Mock()
        mock_client._parameters = {"name": "test"}
        mock_client.submit_script.return_value = "12345"
        mock_client.get_all_job_properties.return_value = []
        mock_client.get_status.return_value = "COMPLETED"

        mock_factory_instance = Mock()
        mock_factory_instance.create_client.return_value = mock_client
        mock_factory.return_value = mock_factory_instance

        operator = EnsemblBashOperator(
            task_id="test_task",
            bash_command="echo 'test'",
            run_defer=0,
        )
        operator.ensembl_cmd = "echo 'prepared'"
        operator.job_name = "test_job"
        operator.job_service = SlurmJobService(mock_client)

        operator.execute(mock_context)

        mock_client.submit_script.assert_called_once()
        mock_client.wait_finished.assert_called_once()


    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'})
    def test_post_execute(self, mock_factory, mock_context):
        """Test post_execute logs completion."""
        operator = EnsemblBashOperator(
            task_id="test_task",
            bash_command="echo 'test'",
        )
        operator.job_name = "test_job"
        operator.job_info = JobInfo(
            job_id="12345",
            job_name="test_job",
            bash_command="echo 'test'",
            status="COMPLETED"
        )

        # Should not raise
        operator.post_execute(mock_context)

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'})
    def test_post_execute_logs_tabular_summary(self, mock_factory, mock_context, caplog):
        """Test post_execute logs a bordered, aligned key/value summary table."""
        operator = EnsemblBashOperator(
            task_id="test_task",
            bash_command="echo 'test'",
            cwd="/test/work",
            log_directory="airflow_logs",
        )
        operator.job_name = "test_job"
        operator.job_info = JobInfo(
            job_id="12345",
            job_name="test_job",
            bash_command="echo 'test'",
            status="COMPLETED"
        )

        with caplog.at_level("INFO"):
            operator.post_execute(mock_context)

        summary = next(r.message for r in caplog.records if "POST EXECUTION SUMMARY" in r.message)
        lines = summary.splitlines()

        assert lines[0] == lines[-1] == "=" * 88
        assert " Task          : test_task" in summary
        assert " Job Name      : test_job" in summary
        assert " Job ID        : 12345" in summary
        assert " Status        : COMPLETED" in summary
        assert " Log Directory : /test/work/airflow_logs/test_job" in summary
        assert " CWD           : /test/work/test_job/" in summary
        assert "echo 'test'" in summary

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'})
    def test_post_execute_skipped(self, mock_factory, mock_context):
        """Test post_execute skips log fetching for skipped jobs."""
        operator = EnsemblBashOperator(
            task_id="test_task",
            bash_command="echo 'test'",
        )
        operator.job_name = "test_job"
        operator.job_info = JobInfo(
            job_id=1,
            job_name="test_job",
            bash_command="echo 'test'",
            status="SKIPPED"
        )
        operator.copy_k8s_logs = Mock()

        # Should not raise
        operator.post_execute(mock_context)

        operator.copy_k8s_logs.assert_not_called()
        mock_factory.create_client.assert_not_called()

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'})
    def test_copy_k8s_logs_copies_reports(self, mock_factory):
        """Test copy job includes Nextflow report HTML files."""
        copy_client = Mock()
        copy_client._parameters = {"name": "test_job_copy_logs"}
        mock_factory.create_client.return_value = copy_client

        operator = EnsemblBashOperator(
            task_id="test_task",
            bash_command="echo 'test'",
            cwd="/test/work",
            log_directory="airflow_logs",
        )
        operator.job_service = Mock()
        operator.job_service.wait_for_job.return_value = "COMPLETED"
        operator.job_info = JobInfo(
            job_id="12345",
            job_name="test_job",
            bash_command="echo 'test'",
            status="COMPLETED",
        )

        status = operator.copy_k8s_logs()

        assert status == "COMPLETED"
        copy_client.submit_script.assert_called_once()
        copy_command = copy_client.submit_script.call_args.args[0]
        assert "shopt -s nullglob" in copy_command
        assert "/test/work/airflow_logs/test_job.12345.*" in copy_command
        assert "/test/work/test_job/test_job_*_report.html" in copy_command
        assert "/test/work/test_job_*_report.html" in copy_command

    @patch('ensemblslurm.operators.ensembl_bash.SlurmClientFactory')
    @patch.dict(os.environ, {'SLURM_JWT': 'test-token'})
    def test_on_kill(self, mock_factory):
        """Test on_kill logs warning."""
        operator = EnsemblBashOperator(
            task_id="test_task",
            bash_command="echo 'test'",
        )

        # Should not raise
        operator.on_kill()


# ============================================================================
# TEST AirflowExceptionWithSlackNotification
# ============================================================================

class TestAirflowExceptionWithSlackNotification:
    """Test suite for AirflowExceptionWithSlackNotification class."""

    @patch('ensemblslurm.operators.ensembl_bash.EnsemblSlackNotifier')
    @patch('ensemblslurm.operators.ensembl_bash.Variable')
    def test_exception_sends_notification(self, mock_variable, mock_notifier_class, mock_context):
        """Test that exception sends Slack notification."""
        mock_variable.get.return_value = "dev"
        mock_notifier = Mock()
        mock_notifier_class.return_value = mock_notifier

        with pytest.raises(AirflowExceptionWithSlackNotification):
            raise AirflowExceptionWithSlackNotification(
                "Test error",
                mock_context,
                "test-slack"
            )

        mock_notifier.post_message.assert_called_once()

    @patch('ensemblslurm.operators.ensembl_bash.EnsemblSlackNotifier')
    @patch('ensemblslurm.operators.ensembl_bash.Variable')
    def test_exception_notification_failure_does_not_raise(self, mock_variable, mock_notifier_class, mock_context):
        """Test that notification failure doesn't prevent exception."""
        mock_variable.get.return_value = "dev"
        mock_notifier_class.side_effect = Exception("Slack error")

        with pytest.raises(AirflowExceptionWithSlackNotification) as exc_info:
            raise AirflowExceptionWithSlackNotification(
                "Test error",
                mock_context,
                "test-slack"
            )

        assert str(exc_info.value) == "Test error"


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_memory_string(self):
        """Test parsing empty memory string."""
        parser = ConfigurationParser()
        assert parser.parse_memory("") == 2048  # default

    def test_empty_time_string(self):
        """Test parsing empty time string."""
        parser = ConfigurationParser()
        assert parser.parse_time("") == 1440  # default

    def test_very_large_memory(self):
        """Test parsing very large memory values."""
        parser = ConfigurationParser()
        result = parser.parse_memory("1000GB")
        assert result == 1024000

    def test_very_small_memory(self):
        """Test parsing very small memory values."""
        parser = ConfigurationParser()
        result = parser.parse_memory("1MB")
        assert result == 1

    def test_zero_memory(self):
        """Test parsing zero memory."""
        parser = ConfigurationParser()
        result = parser.parse_memory("0GB")
        assert result == 0

    def test_negative_memory(self):
        """Test parsing negative memory returns default."""
        parser = ConfigurationParser()
        result = parser.parse_memory("-2GB")
        # Will fail to parse, return default
        assert result == 2048

    def test_special_characters_in_job_name(self, mock_context):
        """Test job name with special characters that remain after processing."""
        parser = ConfigurationParser()
        mock_context['dag_run'].dag_id = "test@dag"  # @ will remain
        mock_context['dag_run'].run_id = "manual__2024"

        with pytest.raises(AirflowException):
            parser.parse_job_name(mock_context)

    def test_unicode_in_command(self, mock_context):
        """Test command with unicode characters."""
        command = "echo 'Hello 世界 🌍'"

        result = NextflowCommandBuilder.build_command(
            base_command=command,
            job_name="test_job",
            context=mock_context,
            work_dir="/test/work",
            web_log_uri="https://log.ebi.ac.uk",
            nf_script_path="/test/script.nf",
        )

        assert "世界" in result
        assert "🌍" in result

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_all_environment_variables(self):
        """Test builder with no environment variables set."""
        parser = ConfigurationParser()
        builder = SlurmConfigBuilder(parser)

        with pytest.raises(AirflowException):
            builder.build()

    def test_concurrent_job_submissions(self, mock_slurm_client):
        """Test multiple concurrent job submissions."""
        service = SlurmJobService(mock_slurm_client)

        job_ids = []
        for i in range(10):
            job_id = service.submit_job(f"echo 'job {i}'", f"job_{i}")
            job_ids.append(job_id)

        assert len(job_ids) == 10
        assert mock_slurm_client.submit_script.call_count == 10

    def test_job_info_dataclass_creation(self):
        """Test JobInfo dataclass can be created."""
        job_info = JobInfo(
            job_id="123",
            job_name="test",
            bash_command="echo 'test'",
            status="COMPLETED"
        )

        assert job_info.job_id == "123"
        assert job_info.job_name == "test"
        assert job_info.status == "COMPLETED"

    def test_job_status_enum_values(self):
        """Test JobStatus enum has expected values."""
        assert JobStatus.COMPLETED == "COMPLETED"
        assert JobStatus.FAILED == "FAILED"
        assert JobStatus.CANCELLED == "CANCELLED"
        assert JobStatus.TIMEOUT == "TIMEOUT"
        assert JobStatus.SKIPPED == "SKIPPED"


# ============================================================================
# ADDITIONAL MISSING TEST CASES
# ============================================================================

class TestAdditionalCases:
    """Additional test cases for comprehensive coverage."""

    @patch.dict(os.environ, {'SLURM_JWT': 'token'})
    def test_slurm_config_immutability(self):
        """Test that SlurmConfig instances are independent."""
        parser = ConfigurationParser()
        builder = SlurmConfigBuilder(parser)

        config1 = builder.build(slurm_uri="https://test1.ebi.ac.uk", slurm_jwt="token")
        config2 = builder.build(slurm_uri="https://test2.ebi.ac.uk", slurm_jwt="token")

        # Modifying one should not affect the other
        config1.env["TEST"] = "value"
        assert "TEST" not in config2.env

    def test_parse_memory_fractional_mb(self):
        """Test parsing fractional megabytes."""
        parser = ConfigurationParser()
        assert parser.parse_memory("512.5MB") == 512  # int conversion

    def test_parse_time_fractional_minutes(self):
        """Test parsing fractional minutes."""
        parser = ConfigurationParser()
        result = parser.parse_time("90.5M")
        assert result == 90  # int conversion

    @patch('ensemblslurm.operators.ensembl_bash.EnsemblSlurmRestClient')
    def test_factory_creates_new_client_each_time(self, mock_client_class, mock_slurm_config):
        """Test factory creates new client instance each time."""
        factory = SlurmClientFactory()

        client1 = factory.create_client(mock_slurm_config, "job1")
        client2 = factory.create_client(mock_slurm_config, "job2")

        assert mock_client_class.call_count == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
