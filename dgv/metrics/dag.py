from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.dgv.metrics.task import (
    create_metrics_tables,
    get_new_logs,
    download_catalog,
    download_log,
    process_log,
    aggregate_log,
    remove_existing_metrics_from_postgres,
    save_metrics_to_postgres,
    copy_logs_to_processed_folder,
    refresh_materialized_views,
    process_matomo,
    save_matomo_to_postgres,
    TMP_FOLDER,
)


default_args = {
    "email": [],
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dgv_metrics",
    schedule_interval="15 6 * * *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60 * 8),
    tags=["dgv", "metrics"],
    default_args=default_args,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    create_metrics_tables = PythonOperator(
        task_id="create_metrics_tables",
        python_callable=create_metrics_tables,
    )

    get_new_logs = ShortCircuitOperator(
        task_id="get_new_logs",
        python_callable=get_new_logs,
    )

    download_catalog = PythonOperator(
        task_id="download_catalog",
        python_callable=download_catalog,
    )

    download_log = PythonOperator(
        task_id="download_log",
        python_callable=download_log,
    )

    process_log = PythonOperator(
        task_id="process_log",
        python_callable=process_log,
    )

    aggregate_log = PythonOperator(
        task_id="aggregate_log",
        python_callable=aggregate_log,
    )

    remove_existing_metrics_from_postgres = PythonOperator(
        task_id="remove_existing_metrics_from_postgres",
        python_callable=remove_existing_metrics_from_postgres,
    )

    save_metrics_to_postgres = PythonOperator(
        task_id="save_metrics_to_postgres",
        python_callable=save_metrics_to_postgres,
    )

    process_matomo = PythonOperator(
        task_id="process_matomo",
        python_callable=process_matomo,
    )

    save_matomo_to_postgres = PythonOperator(
        task_id="save_matomo_to_postgres",
        python_callable=save_matomo_to_postgres,
    )

    copy_logs_to_processed_folder = PythonOperator(
        task_id="copy_log_to_processed_folder",
        python_callable=copy_logs_to_processed_folder,
    )

    refresh_materialized_views = PythonOperator(
        task_id="refresh_materialized_views",
        python_callable=refresh_materialized_views,
    )

    create_metrics_tables.set_upstream(clean_previous_outputs)
    download_catalog.set_upstream(clean_previous_outputs)
    get_new_logs.set_upstream(clean_previous_outputs)
    download_log.set_upstream(get_new_logs)
    process_log.set_upstream(download_log)
    process_log.set_upstream(download_catalog)
    aggregate_log.set_upstream(process_log)
    remove_existing_metrics_from_postgres.set_upstream(create_metrics_tables)
    remove_existing_metrics_from_postgres.set_upstream(aggregate_log)
    save_metrics_to_postgres.set_upstream(remove_existing_metrics_from_postgres)
    copy_logs_to_processed_folder.set_upstream(save_metrics_to_postgres)
    refresh_materialized_views.set_upstream(copy_logs_to_processed_folder)

    # see if we keep matomo in same dag
    process_matomo.set_upstream(download_catalog)
    save_matomo_to_postgres.set_upstream(process_matomo)
