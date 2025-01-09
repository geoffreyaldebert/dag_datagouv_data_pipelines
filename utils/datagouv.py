import dateutil
from typing import TypedDict, Iterator, Optional, Callable
import requests
from datetime import datetime
import re

from datagouvfr_data_pipelines.utils.retry import simple_connection_retry
from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    DATAGOUV_SECRET_API_KEY,
    DEMO_DATAGOUV_URL,
    DEMO_DATAGOUV_SECRET_API_KEY,
)

if AIRFLOW_ENV == "dev":
    DATAGOUV_URL = DEMO_DATAGOUV_URL
    ORGA_REFERENCE = "63e3ae4082ddaa6c806b8417"
if AIRFLOW_ENV == "prod":
    DATAGOUV_URL = "https://www.data.gouv.fr"
    ORGA_REFERENCE = "646b7187b50b2a93b1ae3d45"
VALIDATA_BASE_URL = "https://preprod-api-validata.dataeng.etalab.studio"

SPAM_WORDS = [
    'free',
    'gratuit',
    'allah',
    'jesus'
    'call',
    'promo',
    'argent',
    'reduction',
    'economisez',
    'urgent',
    'recompense',
    'discount',
    'money',
    'gagner',
    'libido',
    'sex',
    'viagra',
    'bitcoin',
    'cash',
    'satisfied',
    'miracle',
    'weight loss',
    'voyance',
    'streaming',
    'benefits',
    'escort',
    'kbis',
    'hack',
    'fuck',
    'macron',
    'acte de',
    'siret',
    'casier judiciaire',
    'officiel',
    'annuaire',
    'carte grise',
    'passeport',
    'administratif',
    "repertoire d'entreprises",
    'documents professionnels',
    'immatriculation',
    'greffe',
    'juridique',
    'seo',
    'demarche',
    'B2B',
    'documents legaux',
    'entrepreneur',
    'visa',
]

datagouv_session = requests.Session()
datagouv_session.headers.update({"X-API-KEY": DATAGOUV_SECRET_API_KEY})


class File(TypedDict):
    dest_path: str
    dest_name: str


@simple_connection_retry
def create_dataset(
    payload: dict,
) -> dict:
    """Create a dataset in data.gouv.fr

    Args:
        payload: payload for dataset containing at minimum title

    Returns:
        json: return API result in a dictionnary
    """
    r = datagouv_session.post(
        f"{DATAGOUV_URL}/api/1/datasets/", json=payload
    )
    r.raise_for_status()
    return r.json()


@simple_connection_retry
def post_resource(
    file_to_upload: File,
    dataset_id: str,
    resource_id: Optional[str] = None,
    payload: Optional[dict] = None,
    on_demo: bool = False,
) -> requests.Response:
    """Upload a resource in data.gouv.fr

    Args:
        file_to_upload: Dictionnary containing `dest_path` and `dest_name` where resource to upload is stored
        dataset_id: ID of the dataset where to store resource
        resource_id: ID of the resource where to upload file. If it is a new resource, leave it to None
        payload: payload to update the resource's metadata (if resource_id is specified)
        on_demo: force publication on demo

    Returns:
        json: return API result in a dictionnary
    """
    if on_demo or AIRFLOW_ENV == "dev":
        datagouv_url = "https://demo.data.gouv.fr"
        datagouv_session.headers.update({"X-API-KEY": DEMO_DATAGOUV_SECRET_API_KEY})
    else:
        datagouv_url = DATAGOUV_URL

    if not file_to_upload['dest_path'].endswith('/'):
        file_to_upload['dest_path'] += '/'
    files = {
        "file": open(
            f"{file_to_upload['dest_path']}{file_to_upload['dest_name']}",
            "rb",
        )
    }
    if resource_id:
        url = f"{datagouv_url}/api/1/datasets/{dataset_id}/resources/{resource_id}/upload/"
    else:
        url = f"{datagouv_url}/api/1/datasets/{dataset_id}/upload/"
    r = datagouv_session.post(url, files=files)
    r.raise_for_status()
    if not resource_id:
        resource_id = r.json()['id']
        print("Resource was given this id:", resource_id)
        url = f"{datagouv_url}/api/1/datasets/{dataset_id}/resources/{resource_id}/upload/"
    if resource_id and payload:
        r = update_dataset_or_resource_metadata(
            payload=payload,
            dataset_id=dataset_id,
            resource_id=resource_id,
            on_demo=on_demo,
        )
    return r


@simple_connection_retry
def post_remote_resource(
    dataset_id: str,
    payload: dict,
    resource_id: Optional[str] = None,
    on_demo: bool = False,
) -> dict:
    """Create a post in data.gouv.fr

    Args:
        dataset_id: id of the dataset
        payload: payload of metadata
        resource_id: resource id (if modifying an existing resource)
        on_demo: force publication on demo

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    if on_demo or AIRFLOW_ENV == "dev":
        datagouv_url = "https://demo.data.gouv.fr"
        datagouv_session.headers.update({"X-API-KEY": DEMO_DATAGOUV_SECRET_API_KEY})
    else:
        datagouv_url = DATAGOUV_URL

    if resource_id:
        url = f"{datagouv_url}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
        print(f"Putting '{payload['title']}' at {url}")
        r = update_dataset_or_resource_metadata(
            payload=payload,
            dataset_id=dataset_id,
            resource_id=resource_id,
            on_demo=on_demo,
        )
    else:
        url = f"{datagouv_url}/api/1/datasets/{dataset_id}/resources/"
        print(f"Posting '{payload['title']}' at {url}")
        if "filetype" not in payload:
            payload.update({"filetype": "remote"})
        r = datagouv_session.post(
            url,
            json=payload,
        )
    r.raise_for_status()
    return r.json()


@simple_connection_retry
def delete_dataset_or_resource(
    dataset_id: str,
    resource_id: Optional[str] = None,
) -> dict:
    """Delete a dataset or a resource in data.gouv.fr

    Args:
        dataset_id: ID of the dataset
        resource_id: ID of the resource.
        If resource is None, the dataset will be deleted. Else only the resource.
        Defaults to None.

    Returns:
        json: return API result in a dictionnary
    """
    if resource_id:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
    else:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/"

    r = datagouv_session.delete(url)
    r.raise_for_status()
    return r.json()


@simple_connection_retry
def get_dataset_or_resource_metadata(
    dataset_id: str,
    resource_id: Optional[str] = None,
) -> dict:
    """Retrieve dataset or resource metadata from data.gouv.fr

    Args:
        dataset_id: ID ot the dataset
        resource_id: ID of the resource.
        If resource_id is None, it will be dataset metadata which will be
        returned. Else it will be resouce_id's ones. Defaults to None.

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    if resource_id:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
    else:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}"
    r = datagouv_session.get(url)
    r.raise_for_status()
    return r.json()


@simple_connection_retry
def get_dataset_from_resource_id(
    resource_id: str,
) -> str:
    """Return dataset ID from resource ID from data.gouv.fr

    Args:
        resource_id: ID of a resource

    Returns:
       str: the dataset id
    """
    url = f"{DATAGOUV_URL}/api/2/datasets/resources/{resource_id}/"
    r = datagouv_session.get(url)
    r.raise_for_status()
    return r.json()['dataset_id']


@simple_connection_retry
def update_dataset_or_resource_metadata(
    payload: dict,
    dataset_id: str,
    resource_id: Optional[str] = None,
    on_demo: bool = False,
) -> requests.Response:
    """Update metadata to dataset or resource in data.gouv.fr

    Args:
        payload: metadata to upload.
        dataset_id: ID of the dataset
        resource_id: ID of the resource.
        If resource_id is None, it will be dataset metadata which will be
        updated. Else it will be resouce_id's ones. Defaults to None.
        on_demo: force publication on demo

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    if on_demo or AIRFLOW_ENV == "dev":
        datagouv_url = "https://demo.data.gouv.fr"
        datagouv_session.headers.update({"X-API-KEY": DEMO_DATAGOUV_SECRET_API_KEY})
    else:
        datagouv_url = DATAGOUV_URL
    if resource_id:
        url = f"{datagouv_url}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
    else:
        url = f"{datagouv_url}/api/1/datasets/{dataset_id}/"

    r = datagouv_session.put(url, json=payload)
    r.raise_for_status()
    return r


@simple_connection_retry
def update_dataset_or_resource_extras(
    payload: dict,
    dataset_id: str,
    resource_id: Optional[str] = None,
) -> requests.Response:
    """Update specific extras to a dataset or resource in data.gouv.fr

    Args:
        payload: Payload contaning extra and its value
        dataset_id: ID of the dataset.
        resource_id: ID of the resource.
        If resource_id is None, it will be dataset extras which will be
        updated. Else it will be resouce_id's ones. Defaults to None.

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    if resource_id:
        url = f"{DATAGOUV_URL}/api/2/datasets/{dataset_id}/resources/{resource_id}/extras/"
    else:
        url = f"{DATAGOUV_URL}/api/2/datasets/{dataset_id}/extras/"
    r = datagouv_session.put(url, json=payload)
    r.raise_for_status()
    return r


@simple_connection_retry
def delete_dataset_or_resource_extras(
    extras: list,
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    """Delete extras from a dataset or resoruce in data.gouv.fr

    Args:
        extras: List of extras to delete.
        dataset_id: ID of the dataset.
        resource_id: ID of the resource.
        If resource_id is None, it will be dataset extras which will be
        deleted. Else it will be resouce_id's ones. Defaults to None.

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    if resource_id:
        url = f"{DATAGOUV_URL}/api/2/datasets/{dataset_id}/resources/{resource_id}/extras/"
    else:
        url = f"{DATAGOUV_URL}/api/2/datasets/{dataset_id}/extras/"
    r = datagouv_session.delete(url, json=extras)
    if r.status_code == 204:
        return {"message": "ok"}
    else:
        return r.json()


@simple_connection_retry
def create_post(
    name: str,
    headline: str,
    content: str,
    body_type: str,
    tags: Optional[list] = [],
) -> dict:
    """Create a post in data.gouv.fr

    Args:
        name: name of post.
        headline: headline of post
        content: content of post
        body_type: body type of post (html or markdown)
        tags: Option list of tags for post

    Returns:
       json: return API result in a dictionnary containing metadatas
    """

    r = datagouv_session.post(
        f"{DATAGOUV_URL}/api/1/posts/",
        json={
            'name': name,
            'headline': headline,
            'content': content,
            'body_type': body_type,
            'tags': tags
        },
    )
    assert r.status_code == 201
    return r.json()


def get_created_date(data: dict, date_key: str) -> datetime:
    # Helper to get created date based on a date_key that could be nested, using . as a separator
    for key in date_key.split('.'):
        data = data.get(key)
    created = dateutil.parser.parse(data)
    return created


@simple_connection_retry
def get_last_items(
    endpoint: str,
    start_date: datetime,
    end_date=None,
    date_key='created_at',
    sort_key='-created',
) -> list:
    results = []
    data = get_all_from_api_query(
        f"https://www.data.gouv.fr/api/1/{endpoint}/?sort={sort_key}",
        # this WILL fail locally for users because of token mismath (demo/prod)
        auth=endpoint == "users",
    )
    for d in data:
        created = get_created_date(d, date_key)
        if end_date and created.timestamp() > end_date.timestamp():
            continue
        elif created.timestamp() < start_date.timestamp():
            break
        results.append(d)
    return results


@simple_connection_retry
def get_latest_comments(start_date: datetime, end_date: datetime = None) -> list:
    """
    Get latest comments posted on discussions, stored as a list of tuples:
    ("discussion_id:comment_timestamp", subject, comment)
    """
    results = []
    data = get_all_from_api_query(
        "https://www.data.gouv.fr/api/1/discussions/?sort=-discussion.posted_on"
    )
    for d in data:
        latest_comment = datetime.fromisoformat(d['discussion'][-1]['posted_on'])
        if latest_comment.timestamp() < start_date.timestamp():
            break
        # going up from the latest comment
        for comment in d['discussion'][::-1]:
            posted_ts = datetime.fromisoformat(comment['posted_on']).timestamp()
            if end_date and posted_ts > end_date.timestamp():
                continue
            elif posted_ts < start_date.timestamp():
                break
            results.append({
                "comment_id": f"{d['id']}|{comment['posted_on']}",
                "discussion_subject": d['subject'],
                "discussion_title": d['title'],
                "discussion_created": d['created'],
                "comment": comment,
            })
    return results


@simple_connection_retry
def post_remote_communautary_resource(
    dataset_id: str,
    payload: dict,
    resource_id: Optional[str] = None,
) -> dict:
    """Post a remote communautary resource on data.gouv.fr

    Args:
        dataset_id: id of the dataset
        payload: payload of metadata
        resource_id: id of the resource to modify if the resource already exists
    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    community_resource_url = f"{DATAGOUV_URL}/api/1/datasets/community_resources"
    dataset_link = f"{DATAGOUV_URL}/fr/datasets/{dataset_id}/#/community-resources"

    print("Payload content:\n", payload)
    if resource_id:
        print(f"Updating resource at {dataset_link} from {payload['remote_url']}")
        # Update resource
        refined_url = community_resource_url + f"/{resource_id}"
        r = datagouv_session.put(
            refined_url,
            json=payload,
        )

    else:
        print(f"Creating resource at {dataset_link} from {payload['remote_url']}")
        # Create resource
        r = datagouv_session.post(
            community_resource_url,
            json=payload,
        )
    r.raise_for_status()
    return r.json()


@simple_connection_retry
def get_all_from_api_query(
    base_query: str,
    next_page: str = 'next_page',
    ignore_errors: bool = False,
    mask: Optional[str] = None,
    auth: bool = False,
) -> Iterator[dict]:
    """/!\ only for paginated endpoints"""
    def get_link_next_page(elem: dict, separated_keys: str):
        result = elem
        for k in separated_keys.split('.'):
            result = result[k]
        return result
    # certain endpoints require authentification but otherwise we're not using it
    # when running locally this can trigger 401 (if you use your dev/demo token in prod)
    # to prevent, overwrite the API key with a valid prod key down here
    # DATAGOUV_SECRET_API_KEY = ""
    headers = {"X-API-KEY": DATAGOUV_SECRET_API_KEY} if auth else {}
    if mask is not None:
        headers["X-fields"] = mask + f",{next_page}"
    r = requests.get(base_query, headers=headers)
    if not ignore_errors:
        r.raise_for_status()
    for elem in r.json()["data"]:
        yield elem
    while get_link_next_page(r.json(), next_page):
        r = requests.get(get_link_next_page(r.json(), next_page), headers=headers)
        if not ignore_errors:
            r.raise_for_status()
        for data in r.json()['data']:
            yield data


@simple_connection_retry
# Function to post a comment on a dataset
def post_comment_on_dataset(dataset_id: str, title: str, comment: str) -> requests.Response:
    post_object = {
        "title": title,
        "comment": comment,
        "subject": {"class": "Dataset", "id": dataset_id},
    }
    r = datagouv_session.post(
        f"{DATAGOUV_URL}/fr/datasets/{dataset_id}/discussions/",
        json=post_object
    )
    r.raise_for_status()
    return r


def title_sort(resources: list[dict]) -> list[dict]:
    return [{'id': r['id']} for r in sorted(resources, key=lambda r: r['title'])]


@simple_connection_retry
def sort_resources(
    dataset_id: str,
    sort_func: Callable = title_sort,
) -> dict:
    # sort_func is required to take a list of resources and
    # return a list of dicts [{'id': resource_id}, ...] in the desired order
    resources = datagouv_session.get(
        f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/",
    ).json()['resources']
    sorted_ids = sort_func(resources)
    if len(sorted_ids) != len(resources):
        raise ValueError("The sorted list has a different number of elements, aborting")
    if len(sorted_ids) != len(set(r["id"] for r in sorted_ids)):
        raise ValueError("An id has been duplicated in the sort process", resources, "vs", sorted_ids)
    r = datagouv_session.put(
        f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/",
        json=sorted_ids,
    )
    r.raise_for_status()
    return r.json()


@simple_connection_retry
def get_awaiting_spam_comments() -> dict:
    r = datagouv_session.get("https://www.data.gouv.fr/api/1/spam/")
    r.raise_for_status()
    return r.json()


@simple_connection_retry
def check_duplicated_orga(slug: str) -> tuple[bool, Optional[str]]:
    duplicate_slug_pattern = r'-\d+$'
    if re.search(duplicate_slug_pattern, slug) is not None:
        suffix = re.findall(duplicate_slug_pattern, slug)[0]
        original_orga = slug[:-len(suffix)]
        url_dup = f"https://data.gouv.fr/api/1/organizations/{original_orga}/"
        test_orga = requests.get(url_dup)
        # only considering a duplicate if the original slug is taken (not not found or deleted)
        if test_orga.status_code not in [404, 410]:
            return True, url_dup
    return False, None
