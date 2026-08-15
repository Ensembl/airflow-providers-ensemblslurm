"""
Test suite for EnsemblSlackNotifier.
"""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest

from ensemblslurm.hooks.ensembl_slack import EnsemblSlackNotifier


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_context():
    ti = Mock()
    ti.task_id = "test_task"
    ti.dag_id = "test_dag"
    ti.state = "success"
    ti.start_date = datetime(2024, 1, 1, 10, 0, 0)
    ti.end_date = datetime(2024, 1, 1, 10, 5, 30)

    dag_run = Mock()
    dag_run.run_id = "manual__20240101t100000"
    dag_run.state = "running"

    return {
        "task_instance": ti,
        "dag_run": dag_run,
        "data_interval_end": datetime(2024, 1, 1, 10, 0, 0),
    }


# ============================================================================
# TEST __init__ / context resolution
# ============================================================================

class TestInit:
    """Test suite for EnsemblSlackNotifier.__init__."""

    def test_uses_provided_context(self, mock_context):
        notifier = EnsemblSlackNotifier(conn_id="test-conn", context=mock_context)

        assert notifier.conn_id == "test-conn"
        assert notifier.context is mock_context

    @patch("ensemblslurm.hooks.ensembl_slack.get_current_context")
    def test_falls_back_to_current_context(self, mock_get_current_context, mock_context):
        mock_get_current_context.return_value = mock_context

        notifier = EnsemblSlackNotifier(conn_id="test-conn")

        assert notifier.context is mock_context
        mock_get_current_context.assert_called_once()


# ============================================================================
# TEST _get_icon
# ============================================================================

class TestGetIcon:
    """Test suite for EnsemblSlackNotifier._get_icon."""

    @pytest.mark.parametrize(
        "status,expected",
        [
            ("success", ":large_green_circle:"),
            ("failure", ":red_circle:"),
            ("warning", ":large_orange_circle:"),
            ("info", ":blue_circle:"),
            ("unknown_status", ":blue_circle:"),
        ],
    )
    def test_icon_for_status(self, status, expected):
        assert EnsemblSlackNotifier._get_icon(status) == expected


# ============================================================================
# TEST _get_duration
# ============================================================================

class TestGetDuration:
    """Test suite for EnsemblSlackNotifier._get_duration."""

    def test_duration_with_both_dates(self, mock_context):
        notifier = EnsemblSlackNotifier(conn_id="test-conn", context=mock_context)
        ti = Mock()
        ti.start_date = datetime(2024, 1, 1, 10, 0, 0)
        ti.end_date = datetime(2024, 1, 1, 10, 0, 5, 500000)

        assert notifier._get_duration(ti) == 5.5

    def test_duration_missing_end_date(self, mock_context):
        notifier = EnsemblSlackNotifier(conn_id="test-conn", context=mock_context)
        ti = Mock()
        ti.start_date = datetime(2024, 1, 1, 10, 0, 0)
        ti.end_date = None

        assert notifier._get_duration(ti) == "N/A"

    def test_duration_missing_start_date(self, mock_context):
        notifier = EnsemblSlackNotifier(conn_id="test-conn", context=mock_context)
        ti = Mock()
        ti.start_date = None
        ti.end_date = datetime(2024, 1, 1, 10, 0, 0)

        assert notifier._get_duration(ti) == "N/A"


# ============================================================================
# TEST format_message
# ============================================================================

class TestFormatMessage:
    """Test suite for EnsemblSlackNotifier.format_message."""

    @patch("ensemblslurm.hooks.ensembl_slack.Variable")
    def test_format_message_basic_structure(self, mock_variable, mock_context):
        mock_variable.get.side_effect = lambda key, default=None: {
            "base_url": "airflow.example.com",
            "environment": "prod",
        }.get(key, default)

        notifier = EnsemblSlackNotifier(conn_id="test-conn", context=mock_context)
        block = notifier.format_message(mock_context, error_msg="", status="success")

        assert isinstance(block, list)
        assert block[0]["type"] == "section"
        assert "test_dag" in block[0]["text"]["text"]
        assert "manual__20240101t100000" in block[0]["text"]["text"]

    @patch("ensemblslurm.hooks.ensembl_slack.Variable")
    def test_format_message_includes_task_status_and_duration(self, mock_variable, mock_context):
        mock_variable.get.return_value = "dev"

        notifier = EnsemblSlackNotifier(conn_id="test-conn", context=mock_context)
        block = notifier.format_message(mock_context, error_msg="", status="success")

        # block layout: [header section, divider, task section, divider, ...]
        task_section = block[2]
        fields = {f["text"] for f in task_section["fields"]}
        assert any("test_task" in f for f in fields)
        assert any("success" in f for f in fields)
        assert any("330.0" in f for f in fields)  # 5 min 30 sec = 330 seconds

    @patch("ensemblslurm.hooks.ensembl_slack.Variable")
    def test_format_message_failure_includes_rerun_button(self, mock_variable, mock_context):
        mock_variable.get.return_value = "dev"

        notifier = EnsemblSlackNotifier(conn_id="test-conn", context=mock_context)
        block = notifier.format_message(mock_context, error_msg="Something broke", status="failure")

        block_types = [b.get("type") for b in block]
        assert "actions" in block_types
        error_texts = [
            b["text"]["text"] for b in block if b.get("type") == "section" and "text" in b
        ]
        assert any("Something broke" in t for t in error_texts)

    @patch("ensemblslurm.hooks.ensembl_slack.Variable")
    def test_format_message_success_includes_dag_message(self, mock_variable, mock_context):
        mock_variable.get.return_value = "dev"

        notifier = EnsemblSlackNotifier(conn_id="test-conn", context=mock_context)
        block = notifier.format_message(mock_context, error_msg="All good", status="success")

        success_texts = [
            b["text"]["text"] for b in block if b.get("type") == "section" and "text" in b
        ]
        assert any("All good" in t for t in success_texts)

    @patch("ensemblslurm.hooks.ensembl_slack.Variable")
    def test_format_message_neutral_status_no_extra_block(self, mock_variable, mock_context):
        mock_variable.get.return_value = "dev"

        notifier = EnsemblSlackNotifier(conn_id="test-conn", context=mock_context)
        block = notifier.format_message(mock_context, error_msg="", status="warning")

        block_types = [b.get("type") for b in block]
        assert "actions" not in block_types

    @patch("ensemblslurm.hooks.ensembl_slack.Variable")
    def test_format_message_raises_value_error_on_bad_context(self, mock_variable):
        mock_variable.get.return_value = "dev"

        # A non-empty dict so `context or get_current_context()` in __init__
        # doesn't fall through to the ambient-context lookup.
        notifier = EnsemblSlackNotifier(conn_id="test-conn", context={"placeholder": True})
        bad_context = {}  # missing task_instance/dag_run/data_interval_end

        with pytest.raises(ValueError, match="Error formatting Slack message"):
            notifier.format_message(bad_context, error_msg="", status="failure")


# ============================================================================
# TEST post_message
# ============================================================================

class TestPostMessage:
    """Test suite for EnsemblSlackNotifier.post_message."""

    @patch("ensemblslurm.hooks.ensembl_slack.SlackWebhookOperator")
    def test_post_message_success(self, mock_operator_class, mock_context):
        mock_operator = Mock()
        mock_operator_class.return_value = mock_operator

        notifier = EnsemblSlackNotifier(conn_id="test-conn", context=mock_context)
        notifier.post_message(message="hello", block=[{"type": "section"}])

        mock_operator_class.assert_called_once_with(
            task_id="slack_webhook_send_blocks",
            slack_webhook_conn_id="test-conn",
            blocks=[{"type": "section"}],
            message="hello",
        )
        mock_operator.execute.assert_called_once_with(context=mock_context)

    @patch("ensemblslurm.hooks.ensembl_slack.SlackWebhookOperator")
    def test_post_message_defaults_block_to_empty_list(self, mock_operator_class, mock_context):
        mock_operator = Mock()
        mock_operator_class.return_value = mock_operator

        notifier = EnsemblSlackNotifier(conn_id="test-conn", context=mock_context)
        notifier.post_message(message="hello")

        call_kwargs = mock_operator_class.call_args[1]
        assert call_kwargs["blocks"] == []

    @patch("ensemblslurm.hooks.ensembl_slack.SlackWebhookOperator")
    def test_post_message_wraps_errors_in_runtime_error(self, mock_operator_class, mock_context):
        mock_operator_class.side_effect = Exception("Slack API down")

        notifier = EnsemblSlackNotifier(conn_id="test-conn", context=mock_context)

        with pytest.raises(RuntimeError, match="Failed to send Slack message"):
            notifier.post_message(message="hello")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
