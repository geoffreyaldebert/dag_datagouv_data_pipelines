from airflow.models import Variable
import json

# Global
AIRFLOW_DAG_HOME = Variable.get("AIRFLOW_DAG_HOME", "/opt/airflow/dags/")
AIRFLOW_DAG_TMP = Variable.get("AIRFLOW_DAG_TMP", "/tmp/")
AIRFLOW_ENV = Variable.get("AIRFLOW_ENV", "dev")
AIRFLOW_URL = Variable.get("AIRFLOW_URL", "")

# Datagouv
DATAGOUV_URL = "https://www.data.gouv.fr"
if AIRFLOW_ENV == "dev":
    DATAGOUV_URL = "https://demo.data.gouv.fr"
DATAGOUV_SECRET_API_KEY = Variable.get("DATAGOUV_SECRET_API_KEY", "")
FILES_BASE_URL = Variable.get("FILES_BASE_URL", "")

# Mattermost
MATTERMOST_DATAGOUV_DATAENG = Variable.get("MATTERMOST_DATAGOUV_DATAENG", "")
MATTERMOST_DATAGOUV_DATAENG_TEST = Variable.get("MATTERMOST_DATAGOUV_DATAENG_TEST", "")
MATTERMOST_DATAGOUV_EDITO = Variable.get("MATTERMOST_DATAGOUV_EDITO", "")
MATTERMOST_DATAGOUV_CURATION = Variable.get("MATTERMOST_DATAGOUV_CURATION", "")
MATTERMOST_DATAGOUV_MOISSONNAGE = Variable.get("MATTERMOST_DATAGOUV_MOISSONNAGE", "")
MATTERMOST_DATAGOUV_ACTIVITES = Variable.get("MATTERMOST_DATAGOUV_ACTIVITES", "")
MATTERMOST_DATAGOUV_REPORTING = Variable.get("MATTERMOST_DATAGOUV_REPORTING", "")
MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE = Variable.get(
    "MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE", ""
)
MATTERMOST_MODERATION_NOUVEAUTES = Variable.get("MATTERMOST_MODERATION_NOUVEAUTES", "")
MATTERMOST_DATAGOUV_TEAM = Variable.get(
    "MATTERMOST_DATAGOUV_TEAM", ""
)

# Minio
MINIO_URL = Variable.get("MINIO_URL", "object.files.data.gouv.fr")
MINIO_BUCKET_DATA_PIPELINE = Variable.get("MINIO_BUCKET_DATA_PIPELINE", "")
MINIO_BUCKET_DATA_PIPELINE_OPEN = Variable.get("MINIO_BUCKET_DATA_PIPELINE_OPEN", "")
MINIO_BUCKET_INFRA = Variable.get("MINIO_BUCKET_INFRA", "")
SECRET_MINIO_DATA_PIPELINE_USER = Variable.get("SECRET_MINIO_DATA_PIPELINE_USER", "")
SECRET_MINIO_DATA_PIPELINE_PASSWORD = Variable.get(
    "SECRET_MINIO_DATA_PIPELINE_PASSWORD", ""
)

# INSEE
INSEE_BASE_URL = Variable.get("INSEE_BASE_URL", "")
SECRET_INSEE_LOGIN = Variable.get("SECRET_INSEE_LOGIN", "")
SECRET_INSEE_PASSWORD = Variable.get("SECRET_INSEE_PASSWORD", "")
SECRET_INSEE_BEARER = Variable.get("SECRET_INSEE_BEARER", "")

# INPI
SECRET_INPI_USER = Variable.get("SECRET_INPI_USER", "")
SECRET_INPI_PASSWORD = Variable.get("SECRET_INPI_PASSWORD", "")

# RNE
AUTH_RNE = json.loads(Variable.get("AUTH_RNE", []))
RNE_FTP_URL = Variable.get("RNE_FTP_URL", "")

# Twitter
TWITTER_CONSUMER_KEY = Variable.get("TWITTER_CONSUMER_KEY", "")
TWITTER_CONSUMER_KEY_SECRET = Variable.get("TWITTER_CONSUMER_KEY_SECRET", "")
TWITTER_ACCESS_TOKEN = Variable.get("TWITTER_ACCESS_TOKEN", "")
TWITTER_SECRET_TOKEN = Variable.get("TWITTER_SECRET_TOKEN", "")

# emails
SECRET_MAIL_DATAGOUV_BOT_USER = Variable.get("SECRET_MAIL_DATAGOUV_BOT_USER", "")
SECRET_MAIL_DATAGOUV_BOT_PASSWORD = Variable.get(
    "SECRET_MAIL_DATAGOUV_BOT_PASSWORD", ""
)
SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD = Variable.get(
    "SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD", ""
).split(",")
