from datetime import datetime, timedelta
import pathlib
import pandas as pd
import json

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.decorators import dag, task

# Default Args which are used in DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True,
    "start_date": days_ago(1),
    "end_date": datetime.today(),
}

# Global Variables
BUCKET_NAME = Variable.get("DATA_GOOGLE_CLOUD_STORAGE")
SOURCE_BUCKET_NAME = f"{BUCKET_NAME}/01_raw"
TARGET_BUCKET_NAME = f"{BUCKET_NAME}/02_intermediate"
GCP_CONN_ID = Variable.get("GCP_GUARDIAN_MINING_CONN_ID")
ROOT_DIR = pathlib.Path().cwd()
TMP_DIR = ROOT_DIR / "tmp"
START_DATE_STRING = str(default_args["start_date"].strftime("%Y-%m-%d"))


@task
def list_file_paths_in_gcs_bucket(delimiter=".json"):
    """Return Filepaths of Files in Google Cloud Storage Bucket"""
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    file_paths = gcs_hook.list(
        bucket_name=BUCKET_NAME,
        delimiter=delimiter,
    )
    filtered_file_paths = [fp for fp in file_paths if START_DATE_STRING in fp]
    return filtered_file_paths


@task
def load_json_from_gcs_bucket(file_paths):
    """Return JSON Files from Google Cloud Storage Bucket"""
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    list_of_dicts = []
    for fp in file_paths[0:5]:
        byte_data = gcs_hook.download(bucket_name=BUCKET_NAME, object_name=fp)
        dict_data = json.loads(byte_data)
        list_of_dicts.append(dict_data)

    return list_of_dicts


def _json_to_csv(list_of_dicts):
    df = pd.json_normalize(data=list_of_dicts, sep="_")
    return df


@task
def store_csv_in_gcs_bucket(list_of_dicts):
    df = _json_to_csv(list_of_dicts)
    bytes_data = df.to_parquet()

    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    object_name = f"02_intermediate/guardian_content_{START_DATE_STRING}.csv"
    gcs_hook.upload(bucket_name=BUCKET_NAME, data=bytes_data, object_name=object_name)


@dag(
    dag_id="transform_guardian_content",
    description="Transfrom data from Guardian Content which is stored as JSON Files in GCS Bucket",
    schedule_interval="@daily",
    start_date=days_ago(1),
    end_date=datetime.today(),
    catchup=False,
    default_args=default_args,
)
def transform():

    store_csv_in_gcs_bucket(load_json_from_gcs_bucket(list_file_paths_in_gcs_bucket()))


dag = transform()
