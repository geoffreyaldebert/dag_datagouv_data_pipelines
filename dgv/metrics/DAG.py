from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.dgv.metrics.task_functions import (
    create_metrics_tables,
    get_new_logs,
    copy_log_to_ongoing_folder,
    download_catalog,
    download_log,
    process_log,
    save_metrics_to_postgres,
    copy_log_to_processed_folder,
    refresh_materialized_views,
    process_matomo,
    save_matomo_to_postgres,
    TMP_FOLDER,
)


default_args = {
    "email": ["geoffrey.aldebert@data.gouv.fr"],
    "email_on_failure": False,
    "retries": 3,
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

    copy_log_to_ongoing_folder = PythonOperator(
        task_id="copy_log_to_ongoing_folder",
        python_callable=copy_log_to_ongoing_folder,
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

    process_matomo = PythonOperator(
        task_id="process_matomo",
        python_callable=process_matomo,
    )

    save_metrics_to_postgres = PythonOperator(
        task_id="save_metrics_to_postgres",
        python_callable=save_metrics_to_postgres,
    )

    save_matomo_to_postgres = PythonOperator(
        task_id="save_matomo_to_postgres",
        python_callable=save_matomo_to_postgres,
    )

    copy_log_to_processed_folder = PythonOperator(
        task_id="copy_log_to_processed_folder",
        python_callable=copy_log_to_processed_folder,
    )

    refresh_materialized_views = PythonOperator(
        task_id="refresh_materialized_views",
        python_callable=refresh_materialized_views,
    )

    create_metrics_tables.set_upstream(clean_previous_outputs)
    get_new_logs.set_upstream(create_metrics_tables)
    download_catalog.set_upstream(create_metrics_tables)
    copy_log_to_ongoing_folder.set_upstream(get_new_logs)
    download_log.set_upstream(copy_log_to_ongoing_folder)
    process_log.set_upstream(download_log)
    process_log.set_upstream(download_catalog)
    save_metrics_to_postgres.set_upstream(process_log)
    copy_log_to_processed_folder.set_upstream(save_metrics_to_postgres)
    refresh_materialized_views.set_upstream(copy_log_to_processed_folder)

    # # see if we keep matomo in same dag
    process_matomo.set_upstream(download_catalog)
    save_matomo_to_postgres.set_upstream(process_matomo)
