from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datagouvfr_data_pipelines.data_processing.sante.finess.task_functions import (
    DAG_NAME,
    TMP_FOLDER,
    DATADIR,
    check_if_modif,
    get_finess_columns,
    get_geoloc_columns,
    build_finess_table,
    send_to_minio,
    publish_on_datagouv,
    send_notification_mattermost,
)

DAG_FOLDER = 'datagouvfr_data_pipelines/data_processing/'

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='0 7 * * *',
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "sante", "finess"],
    default_args=default_args,
) as dag:

    check_if_modif = ShortCircuitOperator(
        task_id='check_if_modif',
        python_callable=check_if_modif,
    )

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {DATADIR}",
    )

    get_finess_columns = PythonOperator(
        task_id='get_finess_columns',
        python_callable=get_finess_columns,
    )

    get_geoloc_columns = PythonOperator(
        task_id='get_geoloc_columns',
        python_callable=get_geoloc_columns,
    )

    build_finess_table = PythonOperator(
        task_id='build_finess_table',
        python_callable=build_finess_table,
    )

    send_to_minio = PythonOperator(
        task_id='send_to_minio',
        python_callable=send_to_minio,
    )

    publish_on_datagouv = PythonOperator(
        task_id='publish_on_datagouv',
        python_callable=publish_on_datagouv,
    )

    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {TMP_FOLDER}",
    )

    send_notification_mattermost = PythonOperator(
        task_id='send_notification_mattermost',
        python_callable=send_notification_mattermost,
    )

    clean_previous_outputs.set_upstream(check_if_modif)

    get_finess_columns.set_upstream(clean_previous_outputs)
    get_geoloc_columns.set_upstream(clean_previous_outputs)

    build_finess_table.set_upstream(get_finess_columns)
    build_finess_table.set_upstream(get_geoloc_columns)

    send_to_minio.set_upstream(build_finess_table)
    publish_on_datagouv.set_upstream(send_to_minio)
    clean_up.set_upstream(publish_on_datagouv)
    send_notification_mattermost.set_upstream(clean_up)
