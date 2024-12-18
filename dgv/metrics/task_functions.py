import glob
import logging
import os
import re
import tarfile
from datetime import date, datetime, timedelta
from typing import Optional
from dataclasses import dataclass

import pandas as pd
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from tqdm import tqdm

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    MINIO_BUCKET_INFRA,
)
from datagouvfr_data_pipelines.utils.download import download_files
from datagouvfr_data_pipelines.utils.minio import MinIOClient, File as MinioFile
from datagouvfr_data_pipelines.utils.postgres import (
    File,
    copy_file,
    execute_sql_file,
)


tqdm.pandas(desc="pandas progress bar", mininterval=5)

minio_infra = MinIOClient(bucket=MINIO_BUCKET_INFRA)
conn = BaseHook.get_connection("POSTGRES_METRIC")

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}metrics/"
DAG_FOLDER = "datagouvfr_data_pipelines/dgv/metrics/"
DB_METRICS_SCHEMA = Variable.get("DB_METRICS_SCHEMA", "metric")

# Segments to analyse in the HAProxy logs
# e.g: for https://www.data.gouv.fr/fr/datasets/ it is "fr"
API_SEGMENTS = ["api/1", "api/2"]
WEB_SEGMENTS = ["fr", "en", "es"]
SEGMENTS = API_SEGMENTS + WEB_SEGMENTS
# resources-static are an exception in the code which pattern has to be manually
# added into the relevant DataGouvObject
OBJ_STATIC_LIST = ["resources-static"]


@dataclass
class DataGouvObject:
    type: str
    resource_id: str
    columns: dict[str, str]
    log_patterns: dict[str, str]
    catalog_destination_path: str = TMP_FOLDER
    resources_static: Optional[str] = None

    def __post_init__(self):
        self.catalog_destination_name = f"catalog_{self.type}.csv"
        self.catalog_url = f"https://www.data.gouv.fr/fr/datasets/r/{self.resource_id}"
        # Output columns should be in the same order as the tables DDL in create_tables.sql
        self.output_columns = (
            ["date_metric"]
            + [
                column
                for column in self.columns.values()
                # url and slug are always excluded from the column output
                if column not in ["url", "slug"]
            ]
            + ["nb_visit", "nb_visit_apis", "nb_visit_total"]
            + [f"nb_visit_{segment.replace('/', '')}" for segment in SEGMENTS]
        )

    def get_catalog_columns(self) -> list[str]:
        return list(self.columns.keys())


# Python lists are ordered
# As such, "resources" has to be before "datasets" so patterns don't overlap
OBJ_CONFIG = [
    DataGouvObject(
        type="resources",
        resource_id="4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d",
        columns={
            "url": "url",
            "id": "resource_id",
            "dataset.id": "dataset_id",
            "dataset.organization_id": "organization_id",
        },
        log_patterns={
            segment.replace("/", ""): f"/{segment}/datasets/r/" for segment in SEGMENTS
        }
        | {"resources-static": "/resources/"},
    ),
    DataGouvObject(
        type="datasets",
        resource_id="f868cca6-8da1-4369-a78d-47463f19a9a3",
        columns={
            "slug": "slug",
            "id": "dataset_id",
            "organization_id": "organization_id",
        },
        log_patterns={
            segment.replace("/", ""): f"/{segment}/datasets/" for segment in SEGMENTS
        },
    ),
    DataGouvObject(
        type="organizations",
        resource_id="b7bbfedc-2448-4135-a6c7-104548d396e7",
        columns={
            "slug": "slug",
            "id": "organization_id",
        },
        log_patterns={
            segment.replace("/", ""): f"/{segment}/organizations/"
            for segment in SEGMENTS
        },
    ),
    DataGouvObject(
        type="reuses",
        resource_id="970aafa0-3778-4d8b-b9d1-de937525e379",
        columns={
            "slug": "slug",
            "id": "reuse_id",
            "organization_id": "organization_id",
        },
        log_patterns={
            segment.replace("/", ""): f"/{segment}/reuses/" for segment in SEGMENTS
        },
    ),
    DataGouvObject(
        type="dataservices",
        resource_id="322d1475-f36a-472d-97ce-d218c8f79092",
        columns={
            "slug": "slug",
            "id": "dataservice_id",
            "organization_id": "organization_id",
        },
        log_patterns={
            segment.replace("/", ""): f"/{segment}/dataservices/"
            for segment in SEGMENTS
        },
    ),
]


def create_metrics_tables() -> None:
    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            File(
                source_path=f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}sql/",
                source_name="create_tables.sql",
                column_order=None,
                header=None,
            )
        ],
    )


def get_new_logs(ti) -> bool:
    new_logs = minio_infra.get_files_from_prefix(prefix="metrics-logs/new/")
    ti.xcom_push(key="new_logs", value=new_logs)
    if new_logs:
        return True
    else:
        return False


def copy_log(new_logs, source_folder, target_folder) -> None:
    for nl in new_logs:
        minio_infra.copy_object(
            MINIO_BUCKET_SOURCE=MINIO_BUCKET_INFRA,
            MINIO_BUCKET_TARGET=MINIO_BUCKET_INFRA,
            path_source=nl,
            path_target=nl.replace(source_folder, target_folder),
            remove_source_file=True,
        )


def copy_log_to_ongoing_folder(ti) -> None:
    new_logs = ti.xcom_pull(key="new_logs", task_ids="get_new_logs")
    copy_log(new_logs, "/new/", "/ongoing/")


def copy_log_to_processed_folder(ti) -> None:
    new_logs = ti.xcom_pull(key="new_logs", task_ids="get_new_logs")
    new_logs = [nl.replace("/new/", "/ongoing/") for nl in new_logs]
    copy_log(new_logs, "/ongoing/", "/processed/")


def download_catalog() -> None:
    download_files(
        [
            {
                "url": obj_config.catalog_url,
                "dest_path": obj_config.catalog_destination_path,
                "dest_name": obj_config.catalog_destination_name,
            }
            for obj_config in OBJ_CONFIG
        ]
    )


def remove_files_if_exists(folder: str) -> None:
    isExist = os.path.exists(f"{TMP_FOLDER}{folder}")
    if not isExist:
        os.makedirs(f"{TMP_FOLDER}{folder}")
    files = glob.glob(f"{TMP_FOLDER}{folder}/*")
    for f in files:
        os.remove(f)


def get_dict(df: pd.DataFrame, obj_property: str) -> dict[str, str]:
    """
    Creates a dictionary from a DataFrame where keys are derived from the specified
    property in the DataFrame and values are the corresponding 'id' column values.

    Args:
        df (pd.DataFrame): A pandas DataFrame containing the data to process.
        obj_property (str): The column name in the DataFrame whose values will be used as keys in the dictionary.
    """
    arr: dict[str, str] = {}
    for _, row in df.iterrows():
        if (
            isinstance(row[obj_property], str)
            and "static.data.gouv.fr" in row[obj_property]
        ):
            arr[row[obj_property]] = row["id"]
        else:
            arr[row[obj_property]] = row["id"]
            arr[row["id"]] = row["id"]
    return arr


def get_info(parsed_line: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Retrieve information related to the datasets, organisation or resources
    from the anonymised HAProxy logs.

    Args:
        parsed_line (str): HAProxy line to parse

    Example:
        parsed_line = '2024-11-13T00:00:23.927326+01:00 slb-04 haproxy[260742]: 127.0.0.1:37959 '
                      '[13/Nov/2024:00:00:23.908] DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-06 '
                      '0/0/2/16/+18 302 +684 - - --NN 222/189/4/1/0 0/0 '
                      '"GET /fr/datasets/r/ee16d126-af0f-4b3b-84d3-080ef8bc0abd HTTP/1.1"'
        Output:
            slug_line = "ee16d126-af0f-4b3b-84d3-080ef8bc0abd"
            type = "resources"
            segment = "fr"
    """
    static_slug_line, static_obj_type, static_segment = (None, None, None)
    # DATAGOUVFR_RGS: service behind the RGS certificate so www.data.gouv.fr
    url_match = re.search(
        r" DATAGOUVFR_RGS~ .* (200|302) .* \"GET (/[^\s]+)", parsed_line
    )
    if url_match:
        id_match = url_match.group(2)
        for obj_config in OBJ_CONFIG:
            for segment, pattern in obj_config.log_patterns.items():
                if pattern in id_match:
                    if segment in OBJ_STATIC_LIST:
                        # Lowest pattern priority
                        static_slug_line = (
                            f"https://static.data.gouv.fr{url_match}".replace(";", "")
                        )
                        static_obj_type = segment
                        static_segment = segment
                    else:
                        slug_line = (
                            id_match.replace(pattern, "").split("/")[0].replace(";", "")
                        )
                        return slug_line, obj_config.type, segment

    return static_slug_line, static_obj_type, static_segment


def save_list_obj(list_obj: list[dict[str, str]], list_of_types: list[str]) -> None:
    """
    Each dict in the input list must contain a 'type' key. The function categorizes
    objects based on their types and writes them to corresponding CSV files.

    Args:
        list_obj (list[dict[str, str]]): A list of dictionaries where each dictionary represents
            an object with keys such as 'resources', 'datasets', etc.
        list_of_types (list[str]): The list of type keys.
    """
    # Split the objects per type
    lists_per_type = {type: [] for type in list_of_types}
    for obj in list_obj:
        lists_per_type[obj["type"]].append(obj)

    # Append each type's list in a separate file
    for type, list_obj in lists_per_type.items():
        file_object = open(f"{TMP_FOLDER}found/found_{type}.csv", "a")
        for item in list_obj:
            file_object.write(f"{item['date']};{item['id']};{item['segment']}\n")


def download_log(ti):
    new_logs_path = ti.xcom_pull(key="new_logs", task_ids="get_new_logs")
    ongoing_logs_path = [nl.replace("/new/", "/ongoing/") for nl in new_logs_path]

    logging.info("downloading files...")
    for path in ongoing_logs_path:
        minio_infra.download_files(
            list_files=[
                MinioFile(
                    source_path="metrics-logs/ongoing/",
                    source_name=path.split("/")[-1],
                    dest_path=TMP_FOLDER,
                    dest_name=path.split("/")[-1],
                    content_type=None,
                )
            ]
        )

    dates_to_process = set(d.split("/")[-1].split("-")[2] for d in ongoing_logs_path)
    ti.xcom_push(key="dates_to_process", value=dates_to_process)


def parse(lines: list[bytes], date: str) -> None:
    """
    Parses a list of log lines, extracts the slug, type, and segment and saves those to files.

    Args:
        lines (list[bytes]): A list of log lines.
        date (str): The date associated with the log lines, formatted as a string.
    """
    patterns_types = [obj.type for obj in OBJ_CONFIG] + OBJ_STATIC_LIST

    list_obj = []
    for b_line in lines:
        try:
            parsed_line = b_line.decode("utf-8")

            slug_line, type_detect, segment = get_info(parsed_line)
            if slug_line:
                list_obj.append(
                    {
                        "type": type_detect,
                        "id": slug_line,
                        "date": date,
                        "segment": segment,
                    }
                )
                if len(list_obj) == 10000:
                    save_list_obj(list_obj, patterns_types)
                    list_obj = []
        except Exception as err:
            raise Exception(f"Sorry, problem with line: {b_line}\n{err}")

    save_list_obj(list_obj, patterns_types)


def get_unique_dates(
    first_list: list[str], second_list: Optional[list[str]]
) -> list[str]:
    """
    Returns a list of unique dates from two input lists.

    Args:
        first_list (list[str]): The primary list of dates as strings.
        second_list (Optional[list[str]]): The secondary list of dates as strings. Can be None.

    Returns:
        list[str]: The list of unique dates.
    """
    in_first = set(first_list)
    in_second = set(second_list) if second_list else set()
    in_second_but_not_in_first = in_second - in_first
    result = list(in_first) + list(in_second_but_not_in_first)
    return result


def aggregate_obj_type(log_date: str, obj_config: DataGouvObject) -> list[str]:
    """
    Calculate aggregated usage metrics by date and resource type.

    Args:
        log_date (str): aggregation date.
        obj_config (DataGouvObject): object to aggregate (eg. dataset, resource..).

    Returns:
        list[str]: List of the processed dates.
    """
    try:
        logging.info(f"---- {obj_config.type} ----")
        df_catalog = pd.read_csv(
            f"{obj_config.catalog_destination_path}{obj_config.catalog_destination_name}",
            dtype=str,
            sep=";",
            usecols=obj_config.get_catalog_columns(),
        )
        df = pd.read_csv(
            f"{TMP_FOLDER}found/found_{obj_config.type}.csv",
            dtype=str,
            sep=";",
            header=None,
        )
        if obj_config.type not in ["resources"]:
            catalog_dict = get_dict(df_catalog, "slug")
            df["id"] = df[1].apply(
                lambda x: catalog_dict[x] if x in catalog_dict else None
            )
            df = df.rename(columns={0: "date_metric", 2: "segment"})
            df = df.drop(columns=[1])
        else:
            df = df.rename(columns={0: "date_metric", 1: "id", 2: "segment"})
            df = pd.merge(df, df_catalog[["id", "url"]], on="id", how="left")
            df["is_static"] = df["url"].apply(
                lambda x: True if "static.data.gouv.fr" in str(x) else False
            )
            logging.info("shape with static", df.shape[0])
            df = df[not df["is_static"], ["date_metric", "id", "segment"]]
            logging.info("shape without static", df.shape[0])

            df_static = pd.read_csv(
                f"{TMP_FOLDER}found/found_resources-static.csv",
                dtype=str,
                header=None,
                sep=";",
            )
            df_static = df_static.rename(
                columns={0: "date_metric", 1: "url", 2: "segment"}
            )
            df_static = pd.merge(
                df_static, df_catalog[["id", "url"]], on="url", how="left"
            )
            df_static = df_static[df_static["id"].notna()][
                ["date_metric", "id", "segment"]
            ]

            df = pd.concat([df, df_static])

        df = df.groupby(["date_metric", "id"], as_index=False).aggregate(
            nb_visit=(
                "segment",
                lambda x: x.isin(
                    [segment.replace("/", "") for segment in WEB_SEGMENTS]
                ).sum(),
            ),
            nb_visit_apis=(
                "segment",
                lambda x: x.isin(
                    [segment.replace("/", "") for segment in API_SEGMENTS]
                ).sum(),
            ),
            nb_visit_total=("segment", "count"),
            **{
                f"nb_visit_{segment.replace('/', '')}": (
                    "segment",
                    lambda x, s=segment.replace("/", ""): (x == s).sum(),
                )
                for segment in SEGMENTS
            },
        )
        df.sort_values(by="nb_visit", ascending=False)
        df = pd.merge(
            df,
            df_catalog,
            on="id",
            how="left",
        )
        df = df.rename(columns=obj_config.columns)
        df[obj_config.output_columns].to_csv(
            path_or_buf=f"{TMP_FOLDER}outputs/{obj_config.type}-{log_date}.csv",
            index=False,
            header=False,
        )
        logging.info(
            f"Output {obj_config.type}-{log_date}.csv has been created with columns: {obj_config.output_columns}"
        )
    except pd.errors.EmptyDataError:
        logging.error(f"empty data {obj_config.type}")
    except FileNotFoundError:
        logging.error("no data resources file")
    return list(df["date_metric"].unique())


def process_log(ti) -> None:
    """Aggregates log files by group of available dates and log types."""

    dates_to_process = ti.xcom_pull(key="dates_to_process", task_ids="download_log")
    dates_processed = []
    remove_files_if_exists("outputs")

    # analyser toutes les dates différentes
    for log_date in dates_to_process:
        remove_files_if_exists("found")
        logging.info("---------------")
        logging.info(f"Processed date: {log_date}")
        lines = []
        for file_name in glob.glob(f"{TMP_FOLDER}/*{log_date}*.tar.gz"):
            with tarfile.open(file_name, "r:gz") as tar:
                for log_file in tar:
                    log_data = tar.extractfile(log_file)
                    if log_data:
                        lines += log_data.readlines()

        logging.info("parse haproxy lines")
        isoformat_log_date = datetime.strptime(log_date, "%d%m%Y").date().isoformat()
        parse(lines, isoformat_log_date)

        for obj_config in OBJ_CONFIG:
            if obj_config.type not in ["resources"]:
                processed_dates = aggregate_obj_type(
                    log_date=log_date,
                    obj_config=obj_config,
                )
                dates_processed = get_unique_dates(dates_processed, processed_dates)

    ti.xcom_push(key="dates_processed", value=dates_processed)


@simple_connection_retry
def get_matomo_outlinks(
    model: str,
    slug: str,
    target: pd.Series,
    metric_date: date,
) -> int:
    matomo_url = "https://stats.data.gouv.fr/index.php"
    params = {
        "module": "API",
        "method": "Actions.getOutlinks",
        "actionType": "url",
        "segment": f"actionUrl==https://www.data.gouv.fr/fr/{model}/{slug}/",
        "format": "JSON",
        "token_auth": "anonymous",
        "idSite": 109,
        "period": "day",
        "date": metric_date.isoformat(),
    }
    tries = 5
    while tries > 0:
        matomo_res = requests.get(matomo_url, params=params)
        # The Matomo API is sometimes unstable and required a few retries
        if matomo_res.status_code in ["502"]:
            time.sleep(0.5)
            tries-=1
        break
    matomo_res.raise_for_status()
    return sum(
        outlink["nb_hits"]
        for outlink in matomo_res.json()
        if outlink["label"] in target
    )


def sum_outlinks_by_orga(
    df_orga: pd.DataFrame,
    df_outlinks: pd.DataFrame,
    model: str,
) -> pd.DataFrame:
    df_outlinks = df_outlinks.groupby("organization_id", as_index=False).sum()
    df_outlinks = df_outlinks.rename(columns={"outlinks": f"{model}_outlinks"})
    df_orga = pd.merge(df_orga, df_outlinks, on="organization_id", how="left").fillna(0)
    df_orga[f"{model}_outlinks"] = df_orga[f"{model}_outlinks"].astype(int)
    return df_orga


def process_matomo() -> None:
    """
    Fetch matomo metrics for external links for reuses and sum these by orga
    """
    if not os.path.exists(f"{TMP_FOLDER}matomo-outputs/"):
        os.makedirs(f"{TMP_FOLDER}matomo-outputs/")

    # Which timespan to target?
    yesterday = date.today() - timedelta(days=1)
    logging.info("get matamo outlinks for reuses")
    reuses_obj = next((obj for obj in OBJ_CONFIG if obj.type == "reuses"))
    df_catalog = pd.read_csv(
        f"{TMP_FOLDER}{reuses_obj.catalog_destination_name}",
        dtype=str,
        sep=";",
        usecols=["id", "slug", "remote_url", "organization_id"],
    )
    df_catalog["outlinks"] = df_catalog.progress_apply(
        lambda x: get_matomo_outlinks(reuses_obj.type, x.slug, x.remote_url, yesterday),
        axis=1,
    )
    df_catalog["date_metric"] = yesterday.isoformat()
    df_catalog.to_csv(
        f"{TMP_FOLDER}matomo-outputs/{reuses_obj.type}-outlinks.csv",
        columns=["date_metric", "id", "organization_id", "outlinks"],
        index=False,
        header=False,
    )

    organizations_obj = next((obj for obj in OBJ_CONFIG if obj.type == "organizations"))
    df_orga = pd.read_csv(
        f"{TMP_FOLDER}{organizations_obj.catalog_destination_name}",
        dtype=str,
        sep=";",
        usecols=organizations_obj.get_catalog_columns(),
    )
    df_orga = df_orga.rename(columns=organizations_obj.columns)

    df_orga = sum_outlinks_by_orga(df_orga, df_catalog, reuses_obj.type)

    df_orga["date_metric"] = yesterday.isoformat()
    df_orga.to_csv(
        f"{TMP_FOLDER}matomo-outputs/organizations-outlinks.csv",
        columns=["date_metric", "organization_id", "reuses_outlinks"],
        index=False,
        header=False,
    )


def save_metrics_to_postgres(ti) -> None:
    for obj_config in OBJ_CONFIG:
        for lf in glob.glob(f"{TMP_FOLDER}outputs/{obj_config.type}-*"):
            if "-id-" not in lf and "-static-" not in lf:
                copy_file(
                    PG_HOST=conn.host,
                    PG_PORT=conn.port,
                    PG_DB=conn.schema,
                    PG_TABLE=f"{DB_METRICS_SCHEMA}.visits_{obj_config.type}",
                    PG_USER=conn.login,
                    PG_PASSWORD=conn.password,
                    list_files=[
                        File(
                            source_path="/".join(lf.split("/")[:-1]) + "/",
                            source_name=lf.split("/")[-1],
                            column_order="("
                            + ", ".join(obj_config.output_columns)
                            + ")",
                            header=None,
                        )
                    ],
                    has_header=False,
                )


def save_matomo_to_postgres() -> None:
    config = [
        {
            "name": "reuses",
            "columns": "(date_metric, reuse_id, organization_id, nb_outlink)",
        },
        {
            "name": "organizations",
            "columns": "(date_metric, organization_id, nb_outlink)",
        },
    ]
    for obj in config:
        for lf in glob.glob(f"{TMP_FOLDER}matomo-outputs/{obj['name']}-*"):
            copy_file(
                PG_HOST=conn.host,
                PG_PORT=conn.port,
                PG_DB=conn.schema,
                PG_TABLE=f"{DB_METRICS_SCHEMA}.matomo_{obj['name']}",
                PG_USER=conn.login,
                PG_PASSWORD=conn.password,
                list_files=[
                    File(
                        source_path="/".join(lf.split("/")[:-1]) + "/",
                        source_name=lf.split("/")[-1],
                        column_order=obj["columns"],
                        header=None,
                    )
                ],
                has_header=False,
            )


def refresh_materialized_views() -> None:
    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            File(
                source_path=f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}sql/",
                source_name="refresh_materialized_views.sql",
                column_order=None,
                header=None,
            )
        ],
    )
