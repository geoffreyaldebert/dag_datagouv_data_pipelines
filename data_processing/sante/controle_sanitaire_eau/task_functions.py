from datetime import datetime
from io import BytesIO
import json
import os
from zipfile import ZipFile

import pandas as pd
import re
import requests

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    post_remote_communautary_resource,
    check_if_recent_update,
    DATAGOUV_URL,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.utils import (
    csv_to_parquet,
    csv_to_csvgz,
)

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}controle_sanitaire_eau"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)

with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}sante/controle_sanitaire_eau/config/dgv.json") as fp:
    config = json.load(fp)


def check_if_modif():
    return check_if_recent_update(
        reference_resource_id=config["RESULT"]["parquet"][AIRFLOW_ENV]["resource_id"],
        dataset_id="5cf8d9ed8b4c4110294c841d",
    )


def process_data():
    # this is done in one task to get the files only once
    resources = requests.get(
        'https://www.data.gouv.fr/api/1/datasets/5cf8d9ed8b4c4110294c841d/',
        headers={"X-fields": "resources{title,url}"}
    ).json()['resources']
    resources = [r for r in resources if re.search(r"dis-\d{4}.zip", r["title"])]
    columns = {file_type: [] for file_type in config.keys()}
    for idx, resource in enumerate(resources):
        print(resource["title"])
        year = int(resource["title"].split(".")[0].split("-")[1])
        r = requests.get(resource["url"])
        r.raise_for_status()
        with ZipFile(BytesIO(r.content)) as zip_ref:
            for _, file in enumerate(zip_ref.namelist()):
                print(">", file)
                file_type = "_".join(file.split("_")[1:-1])
                assert file_type in config.keys()
                with zip_ref.open(file) as f:
                    df = pd.read_csv(
                        f,
                        sep=',',
                        dtype=str,
                    )
                if not columns[file_type]:
                    columns[file_type] = list(df.columns)
                elif list(df.columns) != columns[file_type]:
                    print(columns[file_type])
                    print(list(df.columns))
                    raise ValueError('Columns differ between files')
                df["annee"] = year
                df.to_csv(
                    f"{DATADIR}/{file_type}.csv",
                    index=False,
                    encoding="utf8",
                    mode="w" if idx == 0 else "a",
                    header=idx == 0,
                )
                del df
    for file_type in config.keys():
        csv_to_parquet(
            f"{DATADIR}/{file_type}.csv",
            sep=',',
            dtype={
                # specific dtypes are listed in the config, default to str
                c: config[file_type]["dtype"].get(c, "VARCHAR")
                for c in columns[file_type]
            },
        )
        if file_type == "RESULT":
            # this one is too big for classic csv
            csv_to_csvgz(f"{DATADIR}/{file_type}.csv")


def send_to_minio(file_type):
    minio_open.send_files(
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": f"{file_type}.{ext}",
                "dest_path": "controle_sanitaire_eau/",
                "dest_name": f"{file_type}.{ext}",
            }
            for ext in ["csv" if file_type != "RESULT" else "csv.gz", "parquet"]
        ],
        ignore_airflow_env=True,
    )


def publish_on_datagouv(file_type):
    date = datetime.today().strftime("%d-%m-%Y")
    for ext in ["csv" if file_type != "RESULT" else "csv.gz", "parquet"]:
        post_remote_communautary_resource(
            dataset_id=config[file_type][ext][AIRFLOW_ENV]["dataset_id"],
            resource_id=config[file_type][ext][AIRFLOW_ENV]["resource_id"],
            payload={
                "url": (
                    f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
                    f"/controle_sanitaire_eau/{file_type}.{ext}"
                ),
                "filesize": os.path.getsize(DATADIR + f"/{file_type}.{ext}"),
                "title": (
                    f"Données {file_type} (format {ext})"
                ),
                "format": ext,
                "description": (
                    f"{file_type} (format {ext})"
                    " (créé à partir des [fichiers du Ministère des Solidarités et de la santé]"
                    f"({DATAGOUV_URL}/fr/datasets/{config[file_type][ext][AIRFLOW_ENV]['dataset_id']}/))"
                    f" (dernière mise à jour le {date})"
                ),
            },
        )


def send_notification_mattermost():
    dataset_id = config["RESULT"]["parquet"][AIRFLOW_ENV]["dataset_id"]
    send_message(
        text=(
            ":mega: Données du contrôle sanitaire de l'eau mises à jour.\n"
            f"- Données stockées sur Minio - Bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}\n"
            f"- Données publiées [sur data.gouv.fr]({DATAGOUV_URL}/fr/datasets/{dataset_id}/#/community-resources)"
        )
    )
