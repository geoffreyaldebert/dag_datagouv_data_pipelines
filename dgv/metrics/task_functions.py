from collections import defaultdict
import re
from typing import Any, Optional

import pandas as pd
import requests

from datagouvfr_data_pipelines.dgv.metrics.config import DataGouvLog
from datagouvfr_data_pipelines.utils.filesystem import save_list_of_dict_to_csv
from datagouvfr_data_pipelines.utils.retry import simple_connection_retry


def get_catalog_id_mapping(df: pd.DataFrame, column: str) -> dict[str, str]:
    """
    Map in a dictionary each ID from a catalog to its ID and the column (e.g. slug).
    The dataframe in input requires an `id` column

    Args:
        df (pd.DataFrame): A pandas DataFrame containing the data to process.
        column (str): The column name in the DataFrame whose values will be used in combination of the id in the dictionary.
    """
    mapping: dict[str, str] = {}
    for _, row in df.iterrows():
        mapping[row[column]] = row["id"]
        mapping[row["id"]] = row["id"]
    return mapping


def save_log_infos_to_csv(
    logs_info_per_type: dict[str, list[dict[str, str]]], output_path: str
) -> None:
    """
    Saves the logs info to disk, each type's list in a separate file.

    Args:
        lists_per_type (Dict[str, List[Dict[str, str]]]): A dictionary where the keys are the types
            and the values are lists of objects of that type.
    """
    for type, list_obj in logs_info_per_type.items():
        destination_file = f"{output_path}found_{type}.csv"
        save_list_of_dict_to_csv(list_obj, destination_file)


def parse_logs(
    logs: list[bytes], date: str, logs_config: list[DataGouvLog], output_path: str
) -> int:
    """
    Parses a list of log lines, extracts the slug, type, and segment and saves those to files.

    Args:
        logs (list[bytes]): A list of log lines.
        date (str): The date associated with the log lines, formatted as a string.
        logs_config (list[DataGouvLog]): List of DataGouvLog objects.
        output_path (str): Path were the parsing output should be located.
    """
    lists_per_type = defaultdict(list)
    n_logs = 0
    n_logs_total = 0
    for b_log in logs:
        try:
            parsed_log = b_log.decode("utf-8")

            slug_line, type_detect, segment = extract_log_info(parsed_log, logs_config)
            if slug_line:
                n_logs += 1
                lists_per_type[type_detect].append(
                    {
                        "id": slug_line,
                        "date_metric": date,
                        "segment": segment,
                    }
                )
                if n_logs == 20000:
                    save_log_infos_to_csv(lists_per_type, output_path)
                    lists_per_type = defaultdict(list)
                    n_logs_total += n_logs
                    n_logs = 0
        except Exception as err:
            raise Exception(f"Problem parsing the log: {b_log!r}\n{err}")

    save_log_infos_to_csv(lists_per_type, output_path)
    n_logs_total += n_logs

    return n_logs_total


def extract_log_info(
    log: str, logs_config: list[DataGouvLog]
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Retrieve information related to the datasets, organisation or resources
    from the anonymised HAProxy logs.

    Args:
        parsed_line (str): HAProxy line to parse
        logs_config (list[DataGouvLog]): list of DataGouvLog objects

    Returns:
        (str, str, str): slug line, type and segment of the log.

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

    for obj_config in logs_config:
        for segment, pattern in obj_config.log_patterns.items():
            pattern_result = re.search(pattern, log)
            if pattern_result:
                object_slug = pattern_result.group(1)
                if object_slug:
                    return object_slug, obj_config.type, segment

    return (None, None, None)


def sum_outlinks_by_orga(
    df_orga: pd.DataFrame,
    df_outlinks: pd.DataFrame,
    model: str,
) -> pd.DataFrame:
    df_outlinks = df_outlinks.groupby("organization_id", as_index=False).sum()
    df_outlinks = df_outlinks.rename(columns={"outlinks": f"{model}_outlinks"})
    df_orga = pd.merge(df_orga, df_outlinks, on="organization_id", how="left").fillna(
        "0"
    )
    df_orga[f"{model}_outlinks"] = df_orga[f"{model}_outlinks"].astype(int)
    return df_orga


@simple_connection_retry
def get_matomo_outlinks(
    model: str,
    slug: str,
    target: str,
    metric_date: str,
) -> int:
    """
    Fetches the count of external URL hits (outlinks) from Matomo
    for a specific model and slug on data.gouv.fr.

    Args:
        model (str): The model type (e.g., 'dataset', 'organization').
        slug (str): The unique identifier for the model
        target (str): The target URL to match in the outlinks.
        metric_date (str): The date for which to fetch the metrics,
            read https://developer.matomo.org/api-reference/reporting-api for accepted formats.

    Returns:
        int: The total count of outlink hits for the specified target URL.

    Raises:
        requests.exceptions.HTTPError: If the HTTP request to Matomo fails.
    """

    matomo_url = "https://stats.data.gouv.fr/index.php"
    params: dict[str, Any] = {
        "module": "API",
        "method": "Actions.getOutlinks",
        "actionType": "url",
        "segment": f"actionUrl==https://www.data.gouv.fr/fr/{model}/{slug}/",
        "format": "JSON",
        "token_auth": "anonymous",
        "idSite": 109,
        "period": "day",
        "date": metric_date,
    }
    matomo_res = requests.get(matomo_url, params=params)
    matomo_res.raise_for_status()
    return sum(
        outlink["nb_hits"]
        for outlink in matomo_res.json()
        if outlink["label"] in target
    )
