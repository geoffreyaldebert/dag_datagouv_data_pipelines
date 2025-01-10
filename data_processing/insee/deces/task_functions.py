import pandas as pd
import re
import requests
import json
import os
from datetime import datetime, timedelta

from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.utils import csv_to_parquet, MOIS_FR
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.datagouv import (
    post_remote_resource,
    update_dataset_or_resource_metadata,
    check_if_recent_update,
    DATAGOUV_URL,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}deces"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)
with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}insee/deces/config/dgv.json") as fp:
    config = json.load(fp)


def check_if_modif():
    return check_if_recent_update(
        reference_resource_id=config["deces_csv"][AIRFLOW_ENV]["resource_id"],
        dataset_id="5de8f397634f4164071119c5",
        on_demo=AIRFLOW_ENV == "dev",
    )


def clean_period(file_name):
    return file_name.replace('deces-', '').replace('.txt', '')


def build_temporal_coverage(min_date, max_date):
    # min_date is just a year
    min_iso = f"{min_date}-01-01T00:00:00.000000Z"
    # max_date looks like YYYY-mMM, we're setting the end to the end of the month
    tmp_max = datetime.strptime(f"{max_date.replace('m', '')}-01", "%Y-%m-%d")
    tmp_max = (tmp_max.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    max_iso = f"{tmp_max.strftime('%Y-%m-%d')}T00:00:00.000000Z"
    return min_iso, max_iso


def build_year_month(period):
    if "m" not in period:
        return period
    year, month = period.split('-')
    month = MOIS_FR[month.replace('m', '')]
    return f"{month} {year}"


def reshape_date(date_no_dash):
    year = date_no_dash[:4]
    month = date_no_dash[4:6]
    day = date_no_dash[6:8]
    return f"{year}-{month}-{day}"


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
        del df
    print(f"> {len(errors)} erreur(s)")
    # conversion to parquet, opposition is a boolean, dates should be dates but
    # partially missing ones are uncastable (e.g 1950-00-12)
    dtype = {
        "nom": "VARCHAR",
        "prenoms": "VARCHAR",
        "sexe": "VARCHAR",
        "date_naissance": "VARCHAR",
        "code_insee_naissance": "VARCHAR",
        "commune_naissance": "VARCHAR",
        "pays_naissance": "VARCHAR",
        "date_deces": "VARCHAR",
        "code_insee_deces": "VARCHAR",
        "numero_acte_deces": "VARCHAR",
        "fichier_origine": "VARCHAR",
        "opposition": "BOOLEAN",
    }
    csv_to_parquet(
        DATADIR + '/deces.csv',
        sep=',',
        dtype=dtype,
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
    for _ext in ["csv", "parquet"]:
        post_remote_resource(
            dataset_id=config[f"deces_{_ext}"][AIRFLOW_ENV]["dataset_id"],
            resource_id=config[f"deces_{_ext}"][AIRFLOW_ENV]["resource_id"],
            payload={
                "url": (
                    f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
                    f"/deces/deces.{_ext}"
                ),
                "filesize": os.path.getsize(DATADIR + f"/deces.{_ext}"),
                "title": (
                    f"Personnes décédées entre {min_date} et {build_year_month(max_date)} (format {_ext})"
                ),
                "format": _ext,
                "description": (
                    f"Personnes décédées entre {min_date} et {build_year_month(max_date)} (format {_ext})"
                    " (créé à partir des [fichiers de l'INSEE]"
                    "(https://www.data.gouv.fr/fr/datasets/5de8f397634f4164071119c5/))"
                ),
            },
        )
    min_iso, max_iso = build_temporal_coverage(min_date, max_date)
    update_dataset_or_resource_metadata(
        payload={
            "temporal_coverage": {
                "start": min_iso,
                "end": max_iso,
            },
        },
        dataset_id=config["deces_csv"][AIRFLOW_ENV]["dataset_id"]
    )


def notification_mattermost():
    dataset_id = config["deces_csv"][AIRFLOW_ENV]["dataset_id"]
    send_message(
        f"Données décès agrégées :"
        f"\n- uploadées sur Minio"
        f"\n- publiées [sur {'demo.' if AIRFLOW_ENV == 'dev' else ''}data.gouv.fr]"
        f"({DATAGOUV_URL}/fr/datasets/{dataset_id}/)"
    )
