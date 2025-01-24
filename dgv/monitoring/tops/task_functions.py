from dateutil.relativedelta import relativedelta
from datetime import datetime
import requests
import pandas as pd

from datagouvfr_data_pipelines.config import (
    MATTERMOST_DATAGOUV_REPORTING,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.datagouv import DATAGOUV_MATOMO_ID
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import MinIOClient

BASE_URL = "https://stats.data.gouv.fr/index.php"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)

PARAMS_TOPS = {
    "module": "API",
    "idSite": DATAGOUV_MATOMO_ID,
    # "date": "yesterday",
    # "period": "day",
    "format": "json",
    "method": "Actions.getPageUrls",
    "expanded": 1,
    "flat": 1,
    "filter_limit": 12,
    "filter_column": "label",
}

PARAMS_GENERAL = {
    "module": "API",
    "idSite": DATAGOUV_MATOMO_ID,
    # "date": "2021-11-01,2021-12-01",
    # "period": "range",
    "format": "json",
    "method": "Actions.get",
    "expanded": 1,
    "flat": 1,
}


def build_start(end, freq):
    if freq == "day":
        # we want only one day
        return end
    elif freq == "week":
        # 6 days back, 7 days in total
        return end + relativedelta(days=-6)
    elif freq == "month":
        return end + relativedelta(months=-1)
    elif freq == "year":
        return end + relativedelta(years=-1)


def compute_top(_class, date, title):
    textTop = ""
    PARAMS_TOPS["period"] = "range"
    PARAMS_TOPS["date"] = date
    PARAMS_TOPS["filter_pattern"] = f"/fr/{_class}/"
    print(PARAMS_TOPS)
    r = requests.get(BASE_URL, params=PARAMS_TOPS)
    arr = []
    for data in r.json():
        if "url" in data:
            if data["url"] not in [
                "https://www.data.gouv.fr/fr/datasets/",
                "https://www.data.gouv.fr/fr/reuses/",
            ]:
                r2 = requests.get(
                    data["url"].replace(
                        "https://www.data.gouv.fr/fr/",
                        "https://www.data.gouv.fr/api/1/",
                    )
                )
                print(data)
                arr.append({
                    "value": data["nb_visits"],
                    "url": data["url"],
                    "name": r2.json().get("title", data["url"])
                })
                textTop += (
                    f"`{data['nb_visits']}`".ljust(10)
                    + data["url"]
                    + "\n"
                )
    mydict = {
        "name": title,
        "unit": "visites",
        "values": arr[:10],
        "date_maj": datetime.today().strftime("%Y-%m-%d"),
    }
    return textTop, mydict


def compute_general(date):
    print(date)
    PARAMS_GENERAL["date"] = date
    PARAMS_GENERAL["period"] = "range" if "," in date else "day"
    print(PARAMS_GENERAL)
    r = requests.get(BASE_URL, params=PARAMS_GENERAL)
    pageviews, uniq_pageviews, downloads = [], [], []
    for data in r.json():
        print(data)
        pageviews.append({
            "date": date,
            "value": data["nb_pageviews"],
        })
        uniq_pageviews.append({
            "date": date,
            "value": data["nb_uniq_pageviews"],
        })
        downloads.append({
            "date": date,
            "value": data["nb_downloads"],
        })
    return pageviews, uniq_pageviews, downloads


def get_top(ti, **kwargs):
    piwik_info = kwargs.get("templates_dict")
    end = datetime.strptime(piwik_info["date"], "%Y-%m-%d")
    start = build_start(end, piwik_info["period"])
    textTop, mydict = compute_top(
        piwik_info["type"],
        start.strftime("%Y-%m-%d") + "," + end.strftime("%Y-%m-%d"),
        piwik_info["title"],
    )
    ti.xcom_push(key="top_" + piwik_info["type"], value=textTop)
    ti.xcom_push(key="top_" + piwik_info["type"] + "_dict", value=mydict)


def get_stats(dates, period):
    pageviews = []
    uniq_pageviews = []
    downloads = []
    for d in dates:
        start_date = datetime.strptime(str(d)[:10], "%Y-%m-%d")
        if period in ["month", "year"]:
            end_date = start_date + relativedelta(**{f"{period}s": 1})
            matomodate = (
                start_date.strftime("%Y-%m-%d") + "," + end_date.strftime("%Y-%m-%d")
            )
        elif period in ["day", "week"]:
            matomodate = start_date.strftime("%Y-%m-%d")
        pv, upv, dl = compute_general(matomodate)
        pageviews.append(pv)
        uniq_pageviews.append(upv)
        downloads.append(dl)
    return pageviews, uniq_pageviews, downloads


def publish_top_mattermost(ti, **kwargs):
    publish_info = kwargs.get("templates_dict")
    print(publish_info)
    for _class in ["datasets", "reuses"]:
        top = ti.xcom_pull(
            key=f"top_{_class}", task_ids=f"get_top_{_class}_" + publish_info["period"]
        )
        header = (
            ":rolled_up_newspaper: **Top 10 jeux de données** - "
            if _class == "datasets"
            else ":artist: **Top 10 réutilisations** - "
        )
        message = (
            header
            + f"{publish_info['label']} (visites)\n\n{top}"
        )
        send_message(message, MATTERMOST_DATAGOUV_REPORTING)


def send_tops_to_minio(ti, **kwargs):
    publish_info = kwargs.get("templates_dict")
    for _class in ["datasets", "reuses"]:
        top = ti.xcom_pull(
            key=f"top_{_class}_dict", task_ids=f"get_top_{_class}_" + publish_info["period"]
        )
        minio_open.dict_to_bytes_to_minio(top, publish_info["minio"] + f"top_{_class}.json")


def send_stats_to_minio(**kwargs):
    piwik_info = kwargs.get("templates_dict")
    end = datetime.strptime(piwik_info["date"], "%Y-%m-%d")
    start = build_start(end, piwik_info["period"])
    dates = pd.date_range(
        start,
        end,
        freq=(
            "MS" if piwik_info["period"] == "month"
            else "YS" if piwik_info["period"] == "year"
            else "D"
        ),
    )
    pageviews, uniq_pageviews, downloads = get_stats(dates, piwik_info["period"])
    mydict = {
        "name": "Nombre de visites",
        "unit": "visites",
        "values": pageviews,
        "date_maj": piwik_info["date"],
    }
    minio_open.dict_to_bytes_to_minio(mydict, piwik_info["minio"] + "visits.json")

    mydict = {
        "name": "Nombre de visiteurs uniques",
        "unit": "visiteurs",
        "values": uniq_pageviews,
        "date_maj": piwik_info["date"],
    }
    minio_open.dict_to_bytes_to_minio(mydict, piwik_info["minio"] + "uniq_visits.json")

    mydict = {
        "name": "Nombre de téléchargements",
        "unit": "téléchargements",
        "values": downloads,
        "date_maj": piwik_info["date"],
    }
    minio_open.dict_to_bytes_to_minio(mydict, piwik_info["minio"] + "downloads.json")
