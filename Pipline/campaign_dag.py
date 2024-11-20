from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='Campaign_Effectiveness',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    with TaskGroup('campaign_denormalization_group') as campaign_denormalization_group:
        campaign_denormalization = BigQueryInsertJobOperator(
            task_id='campaign_denormalization',
            configuration={
                "query": {
                    "query": "{% include 'campaign_denormalization.sql' %}",
                    "useLegacySql": False,
                }
            }
        )

    with TaskGroup('clickthrough_rate_metrics_group') as clickthrough_rate_metrics_group:
        clickthrough_rate_metrics = BigQueryInsertJobOperator(
            task_id='clickthrough_rate_metrics',
            configuration={
                "query": {
                    "query": "{% include 'clickthrough_rate_metrics.sql' %}",
                    "useLegacySql": False,
                }
            }
        )

    with TaskGroup('conversion_rate_metrics_group') as conversion_rate_metrics_group:
        conversion_rate_metrics = BigQueryInsertJobOperator(
            task_id='conversion_rate_metrics',
            configuration={
                "query": {
                    "query": "{% include 'conversion_rate_metrics.sql' %}",
                    "useLegacySql": False,
                }
            }
        )

    campaign_denormalization_group >> clickthrough_rate_metrics_group >> conversion_rate_metrics_group