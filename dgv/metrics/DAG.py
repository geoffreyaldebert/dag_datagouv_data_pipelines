from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from operators.clean_folder import CleanFolderOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.dgv.metrics.task_functions import (
    copy_log_to_ongoing_folder,
    copy_log_to_processed_folder,
    create_metrics_tables,
    download_catalog,
    get_new_logs,
    process_log,
    process_matomo,
    save_metrics_to_postgres,
    save_matomo_to_postgres
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}metrics/"
DAG_NAME = 'dgv_metrics'

default_args = {
    'email': ['geoffrey.aldebert@data.gouv.fr'],
    'email_on_failure': False
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='15 3 * * *',
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60*4),
    tags=["dgv", "metrics"],
    default_args=default_args,
) as dag:

    clean_previous_outputs = CleanFolderOperator(
        task_id="clean_previous_outputs",
        folder_path=TMP_FOLDER
    )

    create_metrics_tables = PythonOperator(
        task_id='create_metrics_tables',
        python_callable=create_metrics_tables,
    )

    get_new_logs = ShortCircuitOperator(
        task_id='get_new_logs',
        python_callable=get_new_logs,
    )

    copy_log_to_ongoing_folder = PythonOperator(
        task_id='copy_log_to_ongoing_folder',
        python_callable=copy_log_to_ongoing_folder,
    )

    download_catalog = PythonOperator(
        task_id='download_catalog',
        python_callable=download_catalog,
    )

    process_log = PythonOperator(
        task_id='process_log',
        python_callable=process_log,
    )

    process_matomo = PythonOperator(
        task_id='process_matomo',
        python_callable=process_matomo,
    )

    save_metrics_to_postgres = PythonOperator(
        task_id='save_metrics_to_postgres',
        python_callable=save_metrics_to_postgres,
    )

    save_matomo_to_postgres = PythonOperator(
        task_id='save_matomo_to_postgres',
        python_callable=save_matomo_to_postgres,
    )

    copy_log_to_processed_folder = PythonOperator(
        task_id='copy_log_to_processed_folder',
        python_callable=copy_log_to_processed_folder,
    )

    create_metrics_tables.set_upstream(clean_previous_outputs)
    get_new_logs.set_upstream(create_metrics_tables)
    download_catalog.set_upstream(create_metrics_tables)
    copy_log_to_ongoing_folder.set_upstream(get_new_logs)
    process_log.set_upstream(copy_log_to_ongoing_folder)
    process_log.set_upstream(download_catalog)
    save_metrics_to_postgres.set_upstream(process_log)
    copy_log_to_processed_folder.set_upstream(save_metrics_to_postgres)
    process_matomo.set_upstream(download_catalog)
    save_matomo_to_postgres.set_upstream(process_matomo)
