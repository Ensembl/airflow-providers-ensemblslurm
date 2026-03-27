import logging
from datetime import datetime
#
# from ensembl.production.airflow.hooks.ensembl_slack import EnsemblSlackNotifier
# from airflow.sdk import Variable
#
# env = Variable.get("environment", default="dev")
#
#
# def slack_notifier(context: dict, exclude_operators=('HiveNextflowOperator', 'NextflowOperator', 'EnsemblBashOperator'),
#                    status="success"
#                    ) -> EnsemblSlackNotifier:
#     """
#     Get the Slack notifier instance from the context.
#
#     Args:
#         context (dict): Airflow context dictionary.
#
#     Returns:
#         EnsemblSlackNotifier: An instance of EnsemblSlackNotifier.
#     """
#     slack_conn_id = 'airflow-slack-notification'
#     slack_notifier_obj = EnsemblSlackNotifier(conn_id=slack_conn_id, context=context)
#     if context['ti'].task.task_type not in exclude_operators:
#         error_msg = f"{context['task_instance'].task_id} succeeded" if status == "success" else context.get('exception',
#                                                                                                             'Unknown Error occurred')
#         block_msg = slack_notifier_obj.format_message(context=context, error_msg=error_msg, status=status)
#         slack_notifier_obj.post_message(message=f"{env} Ensembl DAG Status Task Callback", block=block_msg)
#     return slack_notifier_obj
#
#
# def task_success_callback(context):
#     logging.info(f"✅ Task {context['task_instance'].task_id} succeeded from callback")
#     # slack_notifier(context, status="success")
#
#
# def task_failure_callback(context):
#     logging.error(f"❌ Task {context['task_instance'].task_id} failed ****")
#     slack_notifier(context, status="failure")
#
#
# def task_on_retry_callback(context):
#     logging.error(f"❌ Task {context['task_instance'].task_id} task_on_retry_callback")
#
#
# def dag_failure_callback(context):
#     logging.error(f"💥 DAG {context['dag_run'].dag_id} failed")
#
#
# def dag_success_callback(context):
#     logging.info(f"💥 DAG {context['dag_run'].dag_id} success")
