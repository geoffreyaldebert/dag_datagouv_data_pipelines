import pandas as pd
import os
from unidecode import unidecode
import requests
import json
from zipfile import ZipFile
from io import BytesIO

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    post_remote_resource,
    check_if_recent_update,
    DATAGOUV_URL,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.utils import csv_to_parquet, MOIS_FR

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}rna"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)
with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}rna/config/dgv.json") as fp:
    config = json.load(fp)


def check_if_modif():
    return check_if_recent_update(
        reference_resource_id=config["import"]["csv"][AIRFLOW_ENV]["resource_id"],
        dataset_id="58e53811c751df03df38f42d",
        on_demo=AIRFLOW_ENV == "dev",
    )


def process_rna(ti, file_type):
    assert file_type in ["import", "waldec"]
    resources = requests.get(
        'https://www.data.gouv.fr/api/1/datasets/58e53811c751df03df38f42d/',
        headers={"X-fields": "resources{url}"}
    ).json()['resources']
    latest = sorted([r["url"] for r in resources if file_type in r["url"]])[-1]
    r = requests.get(latest)
    r.raise_for_status()
    columns = None
    with ZipFile(BytesIO(r.content)) as zip_ref:
        for idx, file in enumerate(zip_ref.namelist()):
            print(">", file)
            with zip_ref.open(file) as f:
                df = pd.read_csv(
                    f,
                    sep=';',
                    dtype=str,
                    # encoding="ISO-8859-1", # newer files are utf8-encoded
                )
                if columns and list(df.columns) != columns:
                    print(columns)
                    print(list(df.columns))
                    raise ValueError('Columns differ between dep files')
                columns = list(df.columns)
                punc_to_remove = "!\"#$%&'()*+/;?@[]^_`{|}~"
                for c in df.columns:
                    df[c] = df[c].apply(
                        lambda s: unidecode(s)
                        .replace("\n", " ")
                        .translate(str.maketrans("", "", punc_to_remove))
                        .encode("unicode-escape")
                        .decode()
                        .replace("\\", "")
                        if isinstance(s, str)
                        else s
                    )
                df.to_csv(
                    f"{DATADIR}/{file_type}.csv",
                    index=False,
                    encoding="utf8",
                    mode="w" if idx == 0 else "a",
                    header=idx == 0,
                )
    csv_to_parquet(
        f"{DATADIR}/{file_type}.csv",
        sep=',',
        columns=columns,
    )
    ti.xcom_push(key="latest", value=latest.split('/')[-1].split('.')[0].split('_')[2])


def send_rna_to_minio(file_type):
    minio_open.send_files(
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": f"{file_type}.{ext}",
                "dest_path": "rna/",
                "dest_name": f"{file_type}.{ext}",
            }
            for ext in ["csv", "parquet"]
        ],
        ignore_airflow_env=True,
    )


def publish_on_datagouv(ti, file_type):
    latest = ti.xcom_pull(key="latest", task_ids=f"process_rna_{file_type}")
    y, m, d = latest[:4], latest[4:6], latest[6:]
    date = f"{d} {MOIS_FR[m]} {y}"
    for ext in ["csv", "parquet"]:
        post_remote_resource(
            dataset_id=config[file_type][ext][AIRFLOW_ENV]["dataset_id"],
            resource_id=config[file_type][ext][AIRFLOW_ENV]["resource_id"],
            payload={
                "url": (
                    f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
                    f"/rna/{file_type}.{ext}"
                ),
                "filesize": os.path.getsize(DATADIR + f"/{file_type}.{ext}"),
                "title": (
                    f"Données {file_type.title()} au {date} (format {ext})"
                ),
                "format": ext,
                "description": (
                    f"RNA {file_type.title()} au {date} (format {ext})"
                    " (créé à partir des [fichiers du Ministère de l'intérieur]"
                    "(https://www.data.gouv.fr/fr/datasets/58e53811c751df03df38f42d/))"
                ),
            },
        )


def send_notification_mattermost():
    dataset_id = config["import"]["csv"][AIRFLOW_ENV]["dataset_id"]
    send_message(
        text=(
            ":mega: Données des associations mises à jour.\n"
            f"- Données stockées sur Minio - Bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}\n"
            f"- Données publiées [sur data.gouv.fr]({DATAGOUV_URL}/fr/datasets/{dataset_id})"
        )
    )
