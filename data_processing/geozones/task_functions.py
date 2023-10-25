from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    DATAGOUV_SECRET_API_KEY,
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.utils.datagouv import post_resource, DATAGOUV_URL
from datagouvfr_data_pipelines.utils.mattermost import send_message
import os
import pandas as pd
import json
from datetime import datetime
import requests
from io import BytesIO
from urllib.parse import quote_plus, urlencode

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}geozones/data"


def download_and_process_geozones():
    endpoint = "https://rdf.insee.fr/sparql?query="
    query = """PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfschema:<http://www.w3.org/2000/01/rdf-schema#>
    PREFIX igeo:<http://rdf.insee.fr/def/geo#>

    SELECT *
    WHERE {
        ?zone rdf:type ?territory .
        ?zone igeo:nom ?nom .
        ?zone igeo:codeINSEE ?codeINSEE .
        OPTIONAL {
            ?zone igeo:nomSansArticle ?nomSansArticle .
            ?zone igeo:codeArticle ?codeArticle .
            ?suppression_evt igeo:suppression ?zone .
        }
    }"""
    params = {
        'format': 'application/csv;grid=true',
    }
    url = endpoint + quote_plus(query, safe='*') + '&' + urlencode(params)
    print(url)
    r = requests.get(url)
    bytes_io = BytesIO(r.content)
    df = pd.read_csv(bytes_io)
    df['type'] = df['territory'].apply(lambda x: x.split('#')[1])
    df['is_deleted'] = df['suppression_evt'].apply(lambda s: isinstance(s, str))
    for c in df.columns:
        if c != 'is_deleted':
            df[c] = df[c].apply(str)
    map_type = {
        "Etat": "country",
        "Region": "fr:region",
        "Departement": "fr:departement",
        "CollectiviteDOutreMer": "fr:departement",
        "Intercommunalite": "fr:epci",
        "Arrondissement": "fr:arrondissement",
        "ArrondissementMunicipal": "fr:arrondissement",
        "Commune": "fr:commune",
        "CommuneDeleguee": "fr:commune",
        "CommuneAssociee": "fr:commune",
    }
    df = df.loc[df['type'].isin(map_type)]
    df['level'] = df['type'].apply(lambda x: map_type.get(x, x))
    df['_id'] = df['level'] + ':' + df['codeINSEE']
    df = df.rename({"zone": "uri"}, axis=1)
    df = df.drop(['territory', 'suppression_evt'], axis=1)
    df = df.loc[
        (df['type'] != 'Arrondissement') |
        ((df['type'] == 'Arrondissement') & (df['nom'].str.contains('|'.join(['Paris', 'Lyon', 'Marseille']))))
    ]
    df = df.loc[
        df['level'] != 'country'
    ]

    # get countries (with ISO alpha 2 code) from another source
    countries = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets/r/2b38f28d-15e7-4f0c-b61d-6ca1d9b1cfa2",
        sep=';',
        encoding='cp1252',
        dtype=str
    )
    countries = countries.loc[countries['SOUVERAIN'] == 'O']
    countries['uri'] = countries['CODE_COG'].apply(
        lambda x: "http://id.insee.fr/geo/etat/" + x if isinstance(x, str) else x
    )
    countries.rename({
        'NOM_LONG_ETAT': 'nom',
        'NOM_COURT': 'nomSansArticle',
    }, axis=1, inplace=True)
    countries['codeArticle'] = None
    countries['type'] = 'country'
    countries['is_deleted'] = False
    countries['level'] = 'country'
    countries['codeINSEE'] = countries['ISO_alpha2'].apply(
        lambda x: x.lower() if isinstance(x, str) else x
    )
    countries['_id'] = countries['ISO_alpha2'].apply(
        lambda x: "country:" + x.lower() if isinstance(x, str) else x
    )
    countries = countries[
        ['uri', 'nom', 'codeINSEE', 'nomSansArticle', 'codeArticle', 'type', 'is_deleted', 'level', '_id']
    ]
    countries_json = json.loads(countries.to_json(orient='records'))

    export = json.loads(df.to_json(orient='records'))
    export.extend([
        {
            "uri": "http://id.insee.fr/geo/world",
            "nom": "Monde",
            "codeINSEE": "WORLD",
            "nomSansArticle": "Monde",
            "codeArticle": None,
            "type": "country-group",
            "is_deleted": False,
            "level": "country-group",
            "_id": "country-group:world"
        },
        {
            "uri": "http://id.insee.fr/geo/europe",
            "nom": "Union Européenne",
            "codeINSEE": "EU",
            "nomSansArticle": "Union Européenne",
            "codeArticle": None,
            "type": "country-group",
            "is_deleted": False,
            "level": "country-group",
            "_id": "country-group:eu"
        },
    ])
    export.extend(countries_json)
    os.mkdir(DATADIR)
    with open(DATADIR + '/export_geozones.json', 'w', encoding='utf8') as f:
        json.dump(export, f, ensure_ascii=False, indent=4)

    with open(DATADIR + '/export_countries.json', 'w', encoding='utf8') as f:
        json.dump(
            countries_json,
            f, ensure_ascii=False, indent=4
        )


def post_geozones():
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}geozones/config/dgv.json") as fp:
        data = json.load(fp)
    geozones_file = {
        "dest_path": f"{DATADIR}/",
        "dest_name": "export_geozones.json",
    }
    payload = {
        "description": f"Géozones créées à partir du [fichier de l'INSEE](https://rdf.insee.fr/geo/index.html) ({datetime.today()})",
        "filesize": os.path.getsize(os.path.join(DATADIR + '/export_geozones.json')),
        "mime": "application/json",
        "title": f"Géozones ({datetime.today()})",
        "type": "main",
    }
    post_resource(
        api_key=DATAGOUV_SECRET_API_KEY,
        file_to_upload=geozones_file,
        dataset_id=data['geozones'][AIRFLOW_ENV]['dataset_id'],
        resource_id=data['geozones'][AIRFLOW_ENV].get('resource_id', None),
        resource_payload=payload
    )

    countries_file = {
        "dest_path": f"{DATADIR}/",
        "dest_name": "export_countries.json",
    }
    payload = {
        "description": f"Géozones (pays uniquement) créées à partir du [fichier de l'INSEE](https://rdf.insee.fr/geo/index.html) ({datetime.today()})",
        "filesize": os.path.getsize(os.path.join(DATADIR + '/export_countries.json')),
        "mime": "application/json",
        "title": f"Géozones pays ({datetime.today()})",
        "type": "main",
    }
    post_resource(
        api_key=DATAGOUV_SECRET_API_KEY,
        file_to_upload=countries_file,
        dataset_id=data['countries'][AIRFLOW_ENV]['dataset_id'],
        resource_id=data['countries'][AIRFLOW_ENV].get('resource_id', None),
        resource_payload=payload
    )


def notification_mattermost():
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}geozones/config/dgv.json") as fp:
        data = json.load(fp)
    message = "Données Géozones mises à jours [ici]"
    message += f"({DATAGOUV_URL}/fr/datasets/{data['geozones'][AIRFLOW_ENV]['dataset_id']})"
    send_message(message)
