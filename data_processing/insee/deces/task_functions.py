import pandas as pd
import re
import requests
import json
import os
from datetime import datetime, timedelta, timezone

from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.utils import csv_to_parquet, MOIS_FR
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.datagouv import post_remote_resource, DATAGOUV_URL
from datagouvfr_data_pipelines.utils.mattermost import send_message

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}deces"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)


def check_if_modif():
    resources = requests.get(
        'https://www.data.gouv.fr/api/1/datasets/5de8f397634f4164071119c5/',
        headers={"X-fields": "resources{internal{last_modified_internal}}"}
    ).json()['resources']
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    return True
    # return any(
    #     r["internal"]["last_modified_internal"] >= yesterday for r in resources
    # )


def clean_period(file_name):
    return file_name.replace('deces-', '').replace('.txt', '')


def build_year_month(period):
    if "m" not in period:
        return period
    year, month = period.split('-')
    month = MOIS_FR[month.replace('m', '')]
    return f"{month} {year}"


def get_fields(row):
    nom_prenom = row[:80].strip()
    d = {
        "nom": nom_prenom.split("*")[0],
        "prenoms": nom_prenom.split("*")[1].replace('/', '').replace(' ', ','),
        "sexe": row[80].replace('1', 'M').replace('2', 'F'),
        "date_naissance": row[81:89],
        "code_insee_naissance": row[89:94],
        "commune_naissance": row[94:124].strip(),
        # quite some issues in the countries, maybe a cleaning func?
        # or do we want to stick to the original?
        "pays_naissance": row[124:154].strip() or 'FRANCE METROPOLITAINE',
        "date_deces": row[154:162],
        "code_insee_deces": row[162:167],
        "numero_acte_deces": row[167:176].strip(),
    }
    return d


def gather_data(ti):
    print("Getting resources list")
    resources = requests.get(
        "https://www.data.gouv.fr/api/1/datasets/5de8f397634f4164071119c5/",
        headers={"X-fields": "resources{url,title}"},
    ).json()["resources"]
    year_regex = r'deces-\d{4}.txt'
    month_regex = r'deces-\d{4}-m\d{2}.txt'
    full_years = []
    urls = {}
    for r in resources:
        if re.match(year_regex, r['title']):
            urls[clean_period(r['title'])] = r['url']
            full_years.append(r['title'][6:10])
    print(full_years)
    for r in resources:
        if re.match(month_regex, r['title']) and r['title'][6:10] not in full_years:
            print(r['title'])
            urls[clean_period(r['title'])] = r['url']

    opposition_url = [r["url"] for r in resources if "opposition" in r["title"]]
    if len(opposition_url) != 1:
        raise ValueError(f"There should be exactly one opposition file, {len(opposition_url)} found")
    df_opposition = pd.read_csv(
        opposition_url[0],
        sep=';',
        dtype=str,
    )
    df_opposition.rename(
        {
            "Date de décès": "date_deces",
            "Code du lieu de décès": "code_insee_deces",
            "Numéro d'acte de décès": "numero_acte_deces",
        },
        axis=1,
        inplace=True,
    )
    df_opposition["opposition"] = True

    errors = []
    columns = {}
    for idx, (origin, rurl) in enumerate(urls.items()):
        data = []
        print(f'Proccessing {origin}')
        rows = requests.get(rurl).text.split('\n')
        for r in rows:
            if not r:
                continue
            try:
                fields = get_fields(r)
                data.append({**fields, "fichier_origine": origin})
            except:
                print(r)
                errors.append(r)
        # can't have the whole dataframe in RAM, so saving in batches
        df = pd.merge(
            pd.DataFrame(data),
            df_opposition,
            how="left",
            on=["date_deces", "code_insee_deces", "numero_acte_deces"]
        )
        df["opposition"] = df["opposition"].fillna(False)
        del data
        df.to_csv(
            DATADIR + '/deces.csv',
            index=False,
            mode="w" if idx == 0 else "a",
            header=idx == 0,
        )
        if idx == 0:
            columns = df.columns
        del df
    print(f"> {len(errors)} erreur(s)")
    # conversion to parquet, all columns are considered strings by default which is fine
    csv_to_parquet(
        DATADIR + '/deces.csv',
        sep=',',
        columns=columns,
    )

    ti.xcom_push(key="min_date", value=min(urls.keys()))
    ti.xcom_push(key="max_date", value=max(urls.keys()))


def send_to_minio():
    minio_open.send_files(
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": f"deces.{_ext}",
                "dest_path": "deces/",
                "dest_name": f"deces.{_ext}",
            }
            for _ext in ["csv", "parquet"]
        ],
        ignore_airflow_env=True,
    )


def publish_on_datagouv(ti):
    min_date = ti.xcom_pull(key="min_date", task_ids="gather_data")
    max_date = ti.xcom_pull(key="max_date", task_ids="gather_data")
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}insee/deces/config/dgv.json") as fp:
        data = json.load(fp)
    for _ext in ["csv", "parquet"]:
        post_remote_resource(
            dataset_id=data[f"deces_{_ext}"][AIRFLOW_ENV]["dataset_id"],
            resource_id=data[f"deces_{_ext}"][AIRFLOW_ENV]["resource_id"],
            payload={
                "url": (
                    f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
                    f"/deces/deces.{_ext}"
                ),
                "filesize": os.path.getsize(DATADIR + f"/deces.{_ext}"),
                "title": (
                    f"Décès de français.es entre {min_date} et {build_year_month(max_date)} (format {_ext})"
                ),
                "format": _ext,
                "description": (
                    f"Décès de français.es entre {min_date} et {max_date} (format {_ext})"
                    " (créé à partir des [fichiers de l'INSEE]"
                    "(https://www.data.gouv.fr/fr/datasets/5de8f397634f4164071119c5/))"
                ),
            },
        )


def notification_mattermost():
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}insee/deces/config/dgv.json") as fp:
        data = json.load(fp)
    dataset_id = data["deces_csv"][AIRFLOW_ENV]["dataset_id"]
    send_message(
        f"Données décès agrégées :"
        f"\n- uploadées sur Minio"
        f"\n- publiées [sur {'demo.' if AIRFLOW_ENV == 'dev' else ''}data.gouv.fr]"
        f"({DATAGOUV_URL}/fr/datasets/{dataset_id}/)"
    )