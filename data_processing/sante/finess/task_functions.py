from datetime import datetime
import os
import json
import requests
import pandas as pd
from io import BytesIO, StringIO
from bs4 import BeautifulSoup
import pymupdf

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
from datagouvfr_data_pipelines.utils.utils import csv_to_parquet

DAG_NAME = 'data_processing_finess'
DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}finess/"
DATADIR = f"{TMP_FOLDER}data"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)

with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}sante/finess/config/dgv.json") as fp:
    config = json.load(fp)


def check_if_modif():
    return check_if_recent_update(
        reference_resource_id=config["csv"]["prod"]["resource_id"],
        dataset_id="53699569a3a729239d2046eb",
    )


def get_finess_columns(ti):
    doc = pymupdf.open(
        stream=BytesIO(requests.get(
            "https://www.data.gouv.fr/fr/datasets/r/d06a0924-9931-4a60-83b6-93abdb6acfd6"
        ).content),
        filetype="pdf",
    )
    xml = ""
    for page in doc:
        content = page.get_text().split("\n")
        for row in content:
            if row.strip().startswith("<"):
                xml += row + "\n"
    schema = BeautifulSoup(xml, "xml")
    columns = []
    for col in schema.find_all("xs:simpleType"):
        restr = col.find_next("xs:restriction")
        restriction_clean = None
        if restr["base"] == "xs:date":
            restriction_clean = "date"
        if restr.find_all("pattern"):
            restriction_clean = "pattern:" + restr.find_all("pattern")[0]["value"]
        if restr.find_all("minLength"):
            restriction_clean = "minLength:" + restr.find_all("minLength")[0]["value"]
        if restr.find_all("maxLength"):
            if restriction_clean is not None:
                restriction_clean += "&"
                restriction_clean += "maxLength:" + restr.find_all("maxLength")[0]["value"]
            else:
                restriction_clean = "maxLength:" + restr.find_all("maxLength")[0]["value"]
        if restr.find_all("enumeration"):
            restriction_clean = "enumeration:" + "|".join(t["value"] for t in restr.find_all("enumeration"))
        if col["name"][4:] == "NumeroFiness":
            # it's actually two columns
            columns.append({
                "column_name": "nofinesset",
                "constraints": restriction_clean,
            })
            columns.append({
                "column_name": "nofinessej",
                "constraints": restriction_clean,
            })
        else:
            columns.append({
                "column_name": col["name"][4:],
                "constraints": restriction_clean,
            })
        if all(c["column_name"] != "nofinesset" for c in columns):
            raise ValueError("Columns retrieval went wrong:", columns)
    ti.xcom_push(key="finess_columns", value=columns)


def get_geoloc_columns(ti):
    doc = pymupdf.open(
        stream=BytesIO(requests.get(
            "https://www.data.gouv.fr/fr/datasets/r/d1a2f35f-8823-400f-9296-6eb7361ddf6f"
        ).content),
        filetype="pdf",
    )
    xml = ""
    for page in doc:
        content = page.get_text().split("\n")
        for row in content:
            # the condition is slightly different because the file is not structured the same way
            if row.strip().startswith(("<", "-<")) or "=" in row:
                xml += row.replace("-<", "<") + "\n"
    schema = BeautifulSoup(xml, "xml")
    for el in schema.find_all("xs:element"):
        if el.get("name") == "geolocalisation":
            break
    columns_geoloc = [c["name"] for c in el.find_all("xs:element")]
    ti.xcom_push(key="geoloc_columns", value=columns_geoloc)


def build_finess_table(ti):
    finess_columns = ti.xcom_pull(key="finess_columns", task_ids="get_finess_columns")
    # this one is the "normal" Finess file
    print("Getting standard Finess")
    df_finess = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets/r/2ce43ade-8d2c-4d1d-81da-ca06c82abc68",
        sep=";",
        skiprows=1,
        names=["index"] + [c["column_name"] for c in finess_columns],
        dtype=str,
    )
    # we also retrieve the geolocalised version, because some row can be missing
    print("Getting geolocalised file")
    rows = requests.get(
        "https://www.data.gouv.fr/fr/datasets/r/98f3161f-79ff-4f16-8f6a-6d571a80fea2"
    ).content.decode("utf8").split("\n")
    classic, geoloc, other = [], [], []
    # this file is "divided" into two sections:
    # - the first one is (allegedly) the same as the other file (but sometimes not exactly)
    # - the second part is the geolocalised data (which needs merging with the upper part)
    for row in rows:
        if row.startswith("structureet"):
            classic.append(row)
        elif row.startswith("geoloc"):
            geoloc.append(row)
        else:
            other.append(row)
    # there should be only one unwanted row, the first one
    if len(other) != 1:
        raise ValueError("Too many unexpected rows:", other)
    df_finess_geoloc = pd.read_csv(
        StringIO("\n".join(classic)),
        sep=";",
        names=["index"] + [c["column_name"] for c in finess_columns],
        dtype=str,
    )
    # retrieving missing rows
    missing = df_finess_geoloc.loc[
        ~(df_finess_geoloc["nofinesset"].isin(set(df_finess["nofinesset"].to_list())))
        | ~(df_finess_geoloc["nofinessej"].isin(set(df_finess["nofinessej"].to_list())))
    ]
    print(f"Adding {len(missing)} rows from geoloc")
    final_finess = pd.concat([df_finess, missing], ignore_index=True)
    # processing the geloc part of the file
    print("Creating geoloc table")
    geoloc_columns = ti.xcom_pull(key="geoloc_columns", task_ids="get_geoloc_columns")
    df_geoloc = pd.read_csv(
        StringIO("\n".join(geoloc)),
        sep=";",
        names=["index"] + geoloc_columns,
        dtype=str,
    )
    # merging the two parts
    print("> Merging")
    merged = pd.merge(
        final_finess.drop("index", axis=1),
        df_geoloc.drop("index", axis=1),
        on="nofinesset",
        how="outer",
    )
    merged.to_csv(
        DATADIR + "/finess_geoloc.csv",
        index=False,
        sep=";",  # because "," is in the sourcecoordet column
    )
    dtype = {
        col["column_name"]: "DATE" if col["constraints"] == "date" else "VARCHAR"
        for col in finess_columns
    } | {
        name: (
            "FLOAT" if name in ["coordxet", "coordyet"]
            else "DATE" if "date" in name
            else "VARCHAR"
        ) for name in geoloc_columns if name != "nofinesset"
    }
    csv_to_parquet(
        DATADIR + "/finess_geoloc.csv",
        dtype=dtype,
        sep=";",
    )


def send_to_minio():
    minio_open.send_files(
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": f"finess_geoloc.{_ext}",
                "dest_path": "finess/",
                "dest_name": f"finess_geoloc.{_ext}",
            }
            for _ext in ["csv", "parquet"]
        ],
        ignore_airflow_env=True,
    )


def publish_on_datagouv():
    date = datetime.today().strftime("%d-%m-%Y")
    for ext in ["csv", "parquet"]:
        post_remote_communautary_resource(
            dataset_id=config[ext][AIRFLOW_ENV]["dataset_id"],
            resource_id=config[ext][AIRFLOW_ENV]["resource_id"],
            payload={
                "url": (
                    f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
                    f"/finess/finess_geoloc.{ext}"
                ),
                "filesize": os.path.getsize(DATADIR + f"/finess_geoloc.{ext}"),
                "title": (
                    f"Finess des établissements géolocalisés au {date} (format {ext})"
                ),
                "format": ext,
                "description": (
                    f"Finess des établissements géolocalisés (format {ext})"
                    " (créé à partir des [fichiers du Ministère des Solidarités et de la santé]"
                    f"({DATAGOUV_URL}/fr/datasets/{config[ext][AIRFLOW_ENV]['dataset_id']}/))"
                    f" (dernière mise à jour le {date})"
                ),
            },
        )


def send_notification_mattermost():
    dataset_id = config["csv"][AIRFLOW_ENV]["dataset_id"]
    send_message(
        text=(
            ":mega: Données Finess mises à jour.\n"
            f"- Données stockées sur Minio - Bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}\n"
            f"- Données publiées [sur data.gouv.fr]({DATAGOUV_URL}/fr/datasets/{dataset_id}/#/community-resources)"
        )
    )
