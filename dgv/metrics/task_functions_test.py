def test_extract_log_info():
    from datagouvfr_data_pipelines.dgv.metrics.config import MetricsConfig
    from datagouvfr_data_pipelines.dgv.metrics.task_functions import extract_log_info

    config = MetricsConfig()

    test_logs = [
        {
            "log": "2025-01-22T00:00:52.999112+01:00 slb-03 haproxy[2021969]: 127.0.0.1:42912 [22/Jan/2025:00:00:52.847]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/datawrk-03 0/0/1/150/+151 302 +1002 - - --NN 200/164/4/0/0 0/0 "GET'
            ' /fr/datasets/r/0e644d39-d47e-49a1-85f1-83751e84768b HTTP/1.1"',
            "expected_output": (
                "0e644d39-d47e-49a1-85f1-83751e84768b",
                "resources",
                "fr",
            ),  # /fr/datasets/r/$ID (not available through a slug) + 302 status code
        },
        {
            "log": "2025-01-22T00:00:38.903679+01:00 slb-03 haproxy[2021969]: 127.0.0.1:38338 [22/Jan/2025:00:00:38.861]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-06 0/0/2/39/+41 200 +8618 - - --NN 206/169/9/2/0 0/0 "GET'
            ' /resources/indicateurs-de-lactivite-epidemique-taux-dincidence-de-lepidemie-de-covid-19-par-metropole/20211227-190730/sg-metro-opendata-2021-12-27-19h07.csv HTTP/1.1"',
            "expected_output": (
                "indicateurs-de-lactivite-epidemique-taux-dincidence-de-lepidemie-de-covid-19-par-metropole/20211227-190730/sg-metro-opendata-2021-12-27-19h07.csv",
                "resources",
                "static_resource",
            ),  # /resources/$SLUG (not available though an ID)
        },
        {
            "log": "2025-01-22T00:01:35.417564+01:00 slb-03 haproxy[2021969]: 127.0.0.1:8896 [22/Jan/2025:00:01:35.241] DATAGOUVFR_RGS~"
            ' DATAGOUVFR_NEWINFRA/dataweb-05 0/0/2/174/+176 200 +11470 - - --NN 122/84/7/2/0 0/0 "GET'
            ' https://static.data.gouv.fr/resources/liste-des-juridictions-competentes-pour-les-communes-de-france/20240327-094434/2024-competences-territoriales.csv HTTP/2.0"',
            "expected_output": (
                "liste-des-juridictions-competentes-pour-les-communes-de-france/20240327-094434/2024-competences-territoriales.csv",
                "resources",
                "static_resource",
            ),  # /resources/$SLUG (not available though an ID) + full path
        },
        {
            "log": "2025-01-22T06:50:55.046844+01:00 slb-04 haproxy[2624750]: 127.0.0.1:47438 [22/Jan/2025:06:50:54.938]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-05 0/0/0/108/+108 200 +1718 - - --NN 114/70/6/3/0 0/0 "GET'
            ' /api/1/datasets/608bbcdc3bc1b473d1e95afd/resources/ee2e9818-50ac-4a62-b0d6-6fff78958e71/ HTTP/1.1"',
            "expected_output": (
                "ee2e9818-50ac-4a62-b0d6-6fff78958e71",
                "resources",
                "api1",
            ),  # /api/1/datasets/.*/resources/$ID (not available through a slug) + 200 status code
        },
        {
            "log": "2025-01-22T00:00:39.022635+01:00 slb-03 haproxy[2021969]: 127.0.0.1:10036 [22/Jan/2025:00:00:38.891]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/datawrk-03 0/0/1/130/+131 200 +2317 - - --NN 203/165/8/0/0 0/0 "GET'
            ' https://www.data.gouv.fr/api/2/datasets/6569b3d7d193b4daf2b43edc/resources/?page=2&page_size=6&type=main&q= HTTP/2.0"',
            "expected_output": (
                "6569b3d7d193b4daf2b43edc",
                "datasets",
                "api2",
            ),  # /api/2/datasets/.*/resources/ with no ID
        },
        {
            "log": "2025-01-22T00:03:14.919798+01:00 slb-03 haproxy[2021969]: 127.0.0.1:51900 [22/Jan/2025:00:03:14.472]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-06 0/0/1/445/+446 200 +14163 - - --NR 120/78/3/1/0 0/0 "GET'
            ' https://www.data.gouv.fr/en/datasets/etat-civil-prenoms-des-nouveau-nes/?hbjkwxz=hbjkwxz HTTP/2.0"',
            "expected_output": (
                "etat-civil-prenoms-des-nouveau-nes",
                "datasets",
                "en",
            ),  # /en/datasets/$SLUG
        },
        {
            "log": "2025-01-22T00:03:20.860506+01:00 slb-03 haproxy[2021969]: 127.0.0.1:6080 [22/Jan/2025:00:03:20.010]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/datawrk-03 0/0/2/848/+850 200 +1365 - - --NR 123/76/3/1/0 0/0 "GET'
            ' /fr/datasets/6322e99e12175f7eb26ff465/ HTTP/1.1"',
            "expected_output": (
                "6322e99e12175f7eb26ff465",
                "datasets",
                "fr",
            ),  # /fr/datasets/$ID
        },
        {
            "log": "2025-01-22T00:02:27.081346+01:00 slb-03 haproxy[2021969]: 127.0.0.1:51723 [22/Jan/2025:00:02:26.607]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-06 0/0/1/472/+473 200 +4262 - - --VN 154/114/4/4/0 0/0 "GET'
            ' https://www.data.gouv.fr/fr/datasets/?q=ancv HTTP/2.0"',
            "expected_output": (None, None, None),  # /fr/datasets/ with no ID
        },
        {
            "log": "2025-01-22T00:04:13.851643+01:00 slb-03 haproxy[2021969]: 127.0.0.1:11480 [22/Jan/2025:00:04:13.551]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-05 0/0/3/296/+299 200 +7157 - - --NR 132/95/4/3/0 0/0 "GET'
            ' /es/organizations/commune-de-rosenau/ HTTP/1.1"',
            "expected_output": (
                "commune-de-rosenau",
                "organizations",
                "es",
            ),  # /es/organizations/$SLUG
        },
        {
            "log": "2025-01-22T06:59:08.378845+01:00 slb-04 haproxy[2624750]: 127.0.0.1:27337 [22/Jan/2025:06:59:07.709]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-05 0/0/1/667/+668 200 +14397 - - --NR 102/79/3/2/0 0/0 "GET'
            ' https://www.data.gouv.fr/fr/organizations/534fff94a3a7292c64a77fc1/ HTTP/2.0"',
            "expected_output": (
                "534fff94a3a7292c64a77fc1",
                "organizations",
                "fr",
            ),  # /fr/organizations/$ID
        },
        {
            "log": "./haproxy-22012025-04.log:397163:2025-01-22T05:02:48.999363+01:00 slb-04 haproxy[2624750]: 127.0.0.1:43312 [22/Jan/2025:05:02:48.961]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/datawrk-03 0/0/2/36/+38 200 +14164 - - --NN 114/102/5/2/0 0/0 "GET'
            ' /api/1/organizations/580e1e1388ee385e8e13e4cb/datasets/ HTTP/1.1"',
            "expected_output": (
                "580e1e1388ee385e8e13e4cb",
                "organizations",
                "api1",
            ),  # /api/1/organizations/$ID
        },
        {
            "log": "2025-01-22T00:00:40.313481+01:00 slb-03 haproxy[2021969]: 127.0.0.1:10036 [22/Jan/2025:00:00:28.072]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-05 0/0/14/12226/+12240 200 +7150 - - --NN 195/157/7/4/0 0/0 "GET'
            ' https://www.data.gouv.fr/api/1/reuses/?dataset=6569b3d7d193b4daf2b43edc HTTP/2.0"',
            "expected_output": (None, None, None),  # /api/1/reuses/ with no ID
        },
        {
            "log": "2025-01-22T18:27:00.933196+01:00 slb-04 haproxy[2624750]: 127.0.0.1:36634 [22/Jan/2025:18:27:00.818]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-05 0/0/1/112/+113 200 +7108 - - --VN 330/253/5/3/0 0/0 "GET'
            ' https://www.data.gouv.fr/api/1/reuses/679128cb1868f6d8ecfe266a/?lang=fr&_=1737566114249 HTTP/2.0"',
            "expected_output": (
                "679128cb1868f6d8ecfe266a",
                "reuses",
                "api1",
            ),  # /api/1/reuses/$ID
        },
        {
            "log": "2025-01-22T06:15:07.618874+01:00 slb-04 haproxy[2624750]: 127.0.0.1:59144 [22/Jan/2025:06:15:07.221]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-05 0/0/1/396/+397 200 +14398 - - --NN 126/108/4/3/0 0/0 "GET'
            ' /api/1/reuses/consultez-toutes-les-ventes-sur-une-carte/ HTTP/1.1"',
            "expected_output": (
                "consultez-toutes-les-ventes-sur-une-carte",
                "reuses",
                "api1",
            ),  # /api/1/reuses/$SLUG
        },
        {
            "log": "2025-01-22T11:26:48.392745+01:00 slb-03 haproxy[2021969]: 127.0.0.1:57341 [22/Jan/2025:11:26:48.206]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-05 0/0/3/182/+185 200 +12950 - - --VN 459/376/5/1/0 0/0 "GET'
            ' https://www.data.gouv.fr/fr/dataservices/672dcfd4fb13e93799d97e68/ HTTP/2.0"',
            "expected_output": (
                "672dcfd4fb13e93799d97e68",
                "dataservices",
                "fr",
            ),  # /fr/dataservices/$ID
        },
        {
            "log": "2025-01-22T00:03:24.845468+01:00 slb-03 haproxy[2021969]: 127.0.0.1:64764 [22/Jan/2025:00:03:24.646]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-06 0/0/1/197/+198 200 +2813 - - --NR 124/84/3/2/0 0/0 "GET'
            ' /fr/dataservices/api-recherche-dentreprises/ HTTP/1.1"',
            "expected_output": (
                "api-recherche-dentreprises",
                "dataservices",
                "fr",
            ),  # /fr/dataservices/$SLUG
        },
        {
            "log": "2025-01-22T00:00:54.934149+01:00 slb-03 haproxy[2021969]: 127.0.0.1:21363 [22/Jan/2025:00:00:54.859]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/datawrk-03 0/0/1/73/+74 200 +1172 - - --VN 202/166/4/0/0 0/0 "GET'
            ' https://www.data.gouv.fr/api/2/dataservices/search/?organization=5620c13fc751df08e3cdbb48 HTTP/2.0"',
            "expected_output": (
                "search",
                "dataservices",
                "api2",
            ),  # /api/2/dataservices/ with no ID + search
        },
        {
            "log": "2025-01-22T00:00:44.458398+01:00 slb-03 haproxy[2021969]: 127.0.0.1:39546 [22/Jan/2025:00:00:44.447]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/datawrk-03 0/0/1/9/+10 200 +2814 - - --NN 191/155/3/1/0 0/0 "GET'
            ' /api/1/posts/impact-des-donnees-ouvertes-6-6-1/ HTTP/1.1"',
            "expected_output": (None, None, None),  # No URI we are interested in
        },
        {
            "log": "2025-01-22T00:00:38.768179+01:00 slb-03 haproxy[2021969]: 127.0.0.1:38732 [22/Jan/2025:00:00:38.742]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/dataweb-05 0/0/2/23/+25 404 +671 - - --NN 201/164/9/6/0 0/0 "GET'
            ' /resources/prix-des-carburants-en-france/20181117-111538/active-stations.csv HTTP/1.1"',
            "expected_output": (None, None, None),  # No 202 or 302 status code
        },
        {
            "log": "2025-01-22T00:00:59.397051+01:00 slb-03 haproxy[2021969]: 127.0.0.1:54438 [22/Jan/2025:00:00:59.318]"
            ' DATAGOUVFR_RGS~ DATAGOUVFR_NEWINFRA/datawrk-03 0/0/0/77/+77 200 +1027 - - --NR 200/159/2/0/0 0/0 "PUT'
            ' /api/2/datasets/612780152c7513b4140e69c4/resources/4629e371-191e-497e-b363-66d7fd12872a/extras/ HTTP/1.1"',
            "expected_output": (None, None, None),  # No GET method
        },
        {
            "log": "2025-01-22T00:00:38.785656+01:00 slb-03 haproxy[2021969]: 127.0.0.1:54220 [22/Jan/2025:00:00:38.640]"
            " WILDCARD~ METRIC-API-DATAGOUVFR/metric-api2 0/0/2/142/+144 200 +713 - - ---- 201/37/1/0/0 0/0"
            ' "GET /api/datasets/data/?metric_month__sort=desc&dataset_id__exact=67373d86495f49af65c40b59 HTTP/1.1"',
            "expected_output": (
                None,
                None,
                None,
            ),  # No DATAGOUVFR_RGS which identifies production data.gouv.fr
        },
    ]

    for log in test_logs:
        assert (
            extract_log_info(log["log"], config.logs_config) == log["expected_output"]
        )


def test_get_catalog_id_mapping():
    import pandas as pd
    from datagouvfr_data_pipelines.dgv.metrics.task_functions import (
        get_catalog_id_mapping,
    )

    df = pd.DataFrame(
        {
            "id": [
                "679af967b5ff53d3c639a389",
                "679af9477001c9a6fe6fd893",
                "679af0d533b1468d0b6fd895",
                "679af0d433b1468d0b6fd894",
                "679aefcc8ffc9c88376fd893",
                "679aee11159adf9ce839a388",
                "679aebe509e40722956fd894",
                "679ade2f29d4d58a8739a388",
                "679ad76edd7b3ca9796fd894",
                "679ac40b65ded23d8d6fd8a9",
                "679ac143d16efacf806fd893",
            ],
            "slug": [
                "equipements-sportifs-de-proximite-angers-stadium",
                "lidar-hd-ign-2022",
                "loi-eau-67-2024",
                "prairies-permanentes-2024",
                "structure-a-competence-gestion-des-milieux-aquatiques-hautes-pyrenees-2",
                "limites-des-nouvelles-communes-concernees-par-un-ou-plusieurs-risque-s-majeur-s-dans-le-departement-de-lorne-1",
                "inondations-2025-routes-barrees",
                "espaces-proches-rivage-epr-limites-au-titre-du-l-146-6",
                "commune-030",
                "pprn-de-culoz-beon",
                "elections-partielles",
            ],
            "organization_id": [
                "538346d6a3a72906c7ec5c36",
                "544e1b0888ee38289ae1ed05",
                "5617abdc88ee386b8f628ef9",
                "5617abdc88ee386b8f628ef9",
                "58aeee1688ee382134fb5212",
                "558e9b15c751df2a26a453b9",
                "580735be88ee3814295ff490",
                "558ea2d6c751df3646a453cf",
                "58aedb7c88ee3801ec1afb7e",
                "5883648088ee38358d9b81a4",
                "5733038988ee38472ed1b934",
            ],
        }
    )

    expected_output = {
        "equipements-sportifs-de-proximite-angers-stadium": "679af967b5ff53d3c639a389",
        "679af967b5ff53d3c639a389": "679af967b5ff53d3c639a389",
        "lidar-hd-ign-2022": "679af9477001c9a6fe6fd893",
        "679af9477001c9a6fe6fd893": "679af9477001c9a6fe6fd893",
        "loi-eau-67-2024": "679af0d533b1468d0b6fd895",
        "679af0d533b1468d0b6fd895": "679af0d533b1468d0b6fd895",
        "prairies-permanentes-2024": "679af0d433b1468d0b6fd894",
        "679af0d433b1468d0b6fd894": "679af0d433b1468d0b6fd894",
        "structure-a-competence-gestion-des-milieux-aquatiques-hautes-pyrenees-2": "679aefcc8ffc9c88376fd893",
        "679aefcc8ffc9c88376fd893": "679aefcc8ffc9c88376fd893",
        "limites-des-nouvelles-communes-concernees-par-un-ou-plusieurs-risque-s-majeur-s-dans-le-departement-de-lorne-1": "679aee11159adf9ce839a388",
        "679aee11159adf9ce839a388": "679aee11159adf9ce839a388",
        "inondations-2025-routes-barrees": "679aebe509e40722956fd894",
        "679aebe509e40722956fd894": "679aebe509e40722956fd894",
        "espaces-proches-rivage-epr-limites-au-titre-du-l-146-6": "679ade2f29d4d58a8739a388",
        "679ade2f29d4d58a8739a388": "679ade2f29d4d58a8739a388",
        "commune-030": "679ad76edd7b3ca9796fd894",
        "679ad76edd7b3ca9796fd894": "679ad76edd7b3ca9796fd894",
        "pprn-de-culoz-beon": "679ac40b65ded23d8d6fd8a9",
        "679ac40b65ded23d8d6fd8a9": "679ac40b65ded23d8d6fd8a9",
        "elections-partielles": "679ac143d16efacf806fd893",
        "679ac143d16efacf806fd893": "679ac143d16efacf806fd893",
    }

    assert get_catalog_id_mapping(df, "slug") == expected_output


def test_get_matomo_outlinks():
    from datagouvfr_data_pipelines.dgv.metrics.task_functions import get_matomo_outlinks

    # This is the most consulted reuse of our catalog
    result = get_matomo_outlinks(
        "reuses", "deces-en-france", "https://www.deces-en-france.fr", "yesterday"
    )

    assert isinstance(result, int)
    assert result > 0
