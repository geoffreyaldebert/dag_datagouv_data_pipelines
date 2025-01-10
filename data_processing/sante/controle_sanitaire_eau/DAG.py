from datetime import datetime, timedelta
import json
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_DAG_HOME,
)
from datagouvfr_data_pipelines.data_processing.sante.controle_sanitaire_eau.task_functions import (
    check_if_modif,
    process_data,
    send_to_minio,
    publish_on_datagouv,
    send_notification_mattermost,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}controle_sanitaire_eau/"
DAG_FOLDER = 'datagouvfr_data_pipelines/data_processing/'
DAG_NAME = 'data_processing_controle_sanitaire_eau'
DATADIR = f"{TMP_FOLDER}data"

with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}sante/controle_sanitaire_eau/config/dgv.json") as fp:
    config = json.load(fp)

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
    tags=["data_processing", "sante", "eau"],
    default_args=default_args,
) as dag:

    check_if_modif = ShortCircuitOperator(
        task_id='check_if_modif',
        python_callable=check_if_modif,
    )

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    type_tasks = {}
    for file_type in config.keys():
        type_tasks[file_type] = [
            PythonOperator(
                task_id=f'send_to_minio_{file_type}',
                python_callable=send_to_minio,
                op_kwargs={
                    "file_type": file_type,
                },
            ),
            PythonOperator(
                task_id=f'publish_on_datagouv_{file_type}',
                python_callable=publish_on_datagouv,
                op_kwargs={
                    "file_type": file_type,
                },
            ),
        ]

    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {TMP_FOLDER}",
    )

    send_notification_mattermost = PythonOperator(
        task_id='send_notification_mattermost',
        python_callable=send_notification_mattermost,
    )

    clean_previous_outputs.set_upstream(check_if_modif)
    process_data.set_upstream(clean_previous_outputs)
    for file_type in config.keys():
        type_tasks[file_type][0].set_upstream(process_data)
        type_tasks[file_type][1].set_upstream(type_tasks[file_type][0])
        clean_up.set_upstream(type_tasks[file_type][1])
    send_notification_mattermost.set_upstream(clean_up)
