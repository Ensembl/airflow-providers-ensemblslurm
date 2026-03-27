import logging
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
from airflow.sdk import get_current_context

# Set up a logger for SlackBot operations
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class EnsemblSlackNotifier:
    """
    A class for sending Slack notifications based on the status of Airflow tasks.
    Compatible with Airflow 3.x (no direct ORM usage).
    """

    def __init__(self, conn_id, context=None):
        self.conn_id = conn_id
        self.context = context or get_current_context()

    @staticmethod
    def _get_icon(status: str) -> str:
        status_icons = {
            "success": ":large_green_circle:",
            "failure": ":red_circle:",
            "warning": ":large_orange_circle:",
            "info": ":blue_circle:",
        }
        return status_icons.get(status, ":blue_circle:")

    def _get_duration(self, ti):

        if ti.start_date and ti.end_date:
            return round((ti.end_date - ti.start_date).total_seconds(), 2)

        return "N/A"

    def format_message(self, context, error_msg: str, status: str, **kwargs) -> list:
        try:
            icon = self._get_icon(status)

            ti = context.get("task_instance")
            dag_run = context.get("dag_run")

            dag_name = ti.dag_id
            run_id = dag_run.run_id

            local_dt = context.get("data_interval_end").astimezone()

            base_url = Variable.get("base_url", default_var="localhost:8080")
            env = Variable.get("environment", default_var="dev")

            log_url = (
                f"https://{base_url}/dags/{dag_name}/grid"
                f"?dag_run_id={run_id}&task_id={ti.task_id}&tab=logs"
            )

            logger.info("get Task instances............")
            #TODO: Fetch all the task in dag run
            task_instances = [ti]

            task_instances.sort(key=lambda x: x.task_id)

            tasks = []
            for task in task_instances:
                tasks.append(
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*Task:* {task.task_id}",
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Status:* `{task.state}`",
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Duration:* {self._get_duration(task)}",
                            },
                        ],
                    }
                )
                tasks.append({"type": "divider"})

            header = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"{icon} *DAG {dag_name} : {run_id} : `{status}`*",
                    },
                },
                {"type": "divider"},
            ]

            block = header + tasks

            # Footer info
            block += [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"({env}) {icon} *Task {ti.task_id}* `{dag_run.state}`",
                    },
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Execution Date:*\n{local_dt.strftime('%Y-%m-%d %H:%M:%S')}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Log URL:*\n<{log_url}|Click here to view logs>",
                        },
                    ],
                },
                {"type": "divider"},
            ]

            # Failure block
            if status == "failure":
                block += [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Failed With Error Message:*\n{error_msg}",
                        },
                    },
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "emoji": True,
                                    "text": f"Rerun {ti.task_id}",
                                },
                                "style": "danger",
                                "value": f"{dag_name}#{run_id}#{ti.task_id}",
                            }
                        ],
                    },
                    {"type": "divider"},
                ]

            # Success block
            if status == "success":
                block += [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Dag Message:*\n{error_msg}",
                        },
                    },
                    {"type": "divider"},
                ]

            return block

        except Exception as e:
            logger.error(f"Error formatting Slack message: {e}")
            raise ValueError("Error formatting Slack message.")

    def post_message(self, message: str, block: list = None):
        try:
            if block is None:
                block = []

            slack_alert = SlackWebhookOperator(
                task_id="slack_webhook_send_blocks",
                slack_webhook_conn_id=self.conn_id,
                blocks=block,
                message=message,
            )

            return slack_alert.execute(context=self.context)

        except Exception as e:
            logger.error(f"Error sending message to Slack: {e}")
            raise RuntimeError("Failed to send Slack message.")