from datetime import timedelta, datetime
from collections import defaultdict
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.dgv.monitoring.tops.task_functions import (
    get_top,
    send_tops_to_minio,
    send_stats_to_minio,
    publish_top_mattermost,
)
from datagouvfr_data_pipelines.utils.utils import (
    check_if_first_day_of_year,
    check_if_first_day_of_month,
    check_if_monday,
)

DAG_NAME = "dgv_tops"
MINIO_PATH = "tops/"
# we base ourselves on completed days
yesterday = datetime.today().strftime("%Y-%m-%d")

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="15 6 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["tops", "datagouv", "piwik"],
    default_args=default_args,
    catchup=False,
) as dag:

    check_if_monday = ShortCircuitOperator(
        task_id="check_if_monday", python_callable=check_if_monday
    )

    check_if_first_day_of_month = ShortCircuitOperator(
        task_id="check_if_first_day_of_month",
        python_callable=check_if_first_day_of_month,
    )

    check_if_first_day_of_year = ShortCircuitOperator(
        task_id="check_if_first_day_of_year",
        python_callable=check_if_first_day_of_year,
    )

    freqs = {
        "day": "Journée d'hier",
        "week": "Semaine dernière",
        "month": "Mois dernier",
        "year": "Année dernière",
    }

    classes = {
        "datasets": "jeux de données",
        "reuses": "réutilisations",
    }

    tasks = defaultdict(dict)
    for freq, freq_label in freqs.items():
        tasks[freq]["first"] = [
            PythonOperator(
                task_id=f"get_top_{_class}_{freq}",
                python_callable=get_top,
                templates_dict={
                    "type": _class,
                    "date": yesterday,
                    "period": freq,
                    "title": f"Top 10 des {class_label}",
                },
            ) for _class, class_label in classes.items()
        ]
        tasks[freq]["second"] = [
            PythonOperator(
                task_id=f"publish_top_{freq}_mattermost",
                python_callable=publish_top_mattermost,
                templates_dict={
                    "period": freq,
                    "label": freq_label,
                },
            )
        ]
        prefix = "dai" if freq == "day" else freq
        tasks[freq]["third"] = [
            PythonOperator(
                task_id=f"send_top_{freq}_to_minio",
                python_callable=send_tops_to_minio,
                templates_dict={
                    "period": freq,
                    "minio": f"{MINIO_PATH}piwik_tops_{prefix}ly/{yesterday}/",
                },
            ),
            PythonOperator(
                task_id=f"send_stats_{freq}_to_minio",
                python_callable=send_stats_to_minio,
                templates_dict={
                    "period": freq,
                    "minio": f"{MINIO_PATH}piwik_stats_{prefix}ly/{yesterday}/",
                    "date": yesterday,
                },
            ),
        ]

        for task in tasks[freq]["first"]:
            if freq == "week":
                task.set_upstream(check_if_monday)
            elif freq == "month":
                task.set_upstream(check_if_first_day_of_month)
            elif freq == "year":
                task.set_upstream(check_if_first_day_of_year)

        for parent, child in zip(["first", "second"], ["second", "third"]):
            for child_task in tasks[freq][child]:
                for parent_task in tasks[freq][parent]:
                    child_task.set_upstream(parent_task)
