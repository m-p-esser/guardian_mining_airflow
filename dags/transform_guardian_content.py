from datetime import datetime, timedelta
import pathlib
import pandas as pd
import json

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from pandas_profiling import ProfileReport

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
RAW_DIR = "01_raw"
INTERMEDIATE_DIR = "02_intermediate"
GCP_CONN_ID = Variable.get("GCP_GUARDIAN_MINING_CONN_ID")
START_DATE_STRING = str(default_args["start_date"].strftime("%Y-%m-%d"))


@dag(
    dag_id="transform_guardian_content",
    description="Transfrom data from Guardian Content as parquet in Google Cloud Storage",
    schedule_interval="@daily",
    start_date=days_ago(1),
    end_date=datetime.today(),
    catchup=False,
    default_args=default_args,
)
def transform():

    # Task Group to convert file type
    with TaskGroup(group_id="convert_file_type") as convert_file_type_tg:

        def convert_file_type():
            @task(task_id="list_file_paths_in_gcs_bucket")
            def list_file_paths_in_gcs_bucket(delimiter):
                """Return Filepaths of Files in Google Cloud Storage Bucket"""
                gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
                file_paths = gcs_hook.list(
                    bucket_name=BUCKET_NAME,
                    delimiter=delimiter,
                )
                filtered_file_paths = [
                    fp for fp in file_paths if START_DATE_STRING in fp
                ]
                return filtered_file_paths

            @task(task_id="load_json_from_gcs_bucket")
            def load_json_from_gcs_bucket(file_paths):
                """Return JSON Files from Google Cloud Storage Bucket"""
                gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

                list_of_dicts = []
                for fp in file_paths:
                    byte_data = gcs_hook.download(
                        bucket_name=BUCKET_NAME, object_name=fp
                    )
                    dict_data = json.loads(byte_data)
                    list_of_dicts.append(dict_data)

                return list_of_dicts

            def _json_to_parquet(list_of_dicts, **parameter):
                """Normalize List of Dictionary to Pandas Dataframe"""
                df = pd.json_normalize(data=list_of_dicts, **parameter)
                return df

            @task(task_id="store_parquet_in_gcs_bucket")
            def store_parquet_in_gcs_bucket(
                list_of_dicts, output_file_prefix, **parameter
            ):
                """Save parquet in Google Cloud Storage"""

                # Normalize JSON to Dataframe
                df = _json_to_parquet(list_of_dicts, **parameter)
                bytes_data = df.to_parquet()

                # Upload to GCS Bucket
                gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
                object_name = f"{INTERMEDIATE_DIR}/{output_file_prefix}_{START_DATE_STRING}.parquet"
                gcs_hook.upload(
                    bucket_name=BUCKET_NAME, data=bytes_data, object_name=object_name
                )

            file_paths = list_file_paths_in_gcs_bucket(delimiter=".json")
            list_of_dicts = load_json_from_gcs_bucket(file_paths)
            store_parquet_in_gcs_bucket(
                list_of_dicts=list_of_dicts,
                output_file_prefix="guardian_content",
                **{"sep": "_"},
            )
            store_parquet_in_gcs_bucket(
                list_of_dicts=list_of_dicts,
                output_file_prefix="guardian_content_tags",
                **{
                    "sep": "_",
                    "record_path": "tags",
                    "record_prefix": "tags_",
                    "meta": "id",
                    "meta_prefix": "content_",
                },
            )

        convert_file_type()

    # Task to create and upload Pandas Profile
    with TaskGroup(group_id="profiling") as profiling_tg:

        def profiling():
            def _download_parquet_file(output_file_prefix):
                """Download parquet File from GCS Bucket"""

                gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
                object_url = f"gs://{BUCKET_NAME}/{INTERMEDIATE_DIR}/{output_file_prefix}_{START_DATE_STRING}.parquet"

                with gcs_hook.provide_file(object_url=object_url) as f:
                    df = pd.read_parquet(f)
                    return df

            def _create_pandas_profile(df, output_file_prefix, **parameter):
                """Create Pandas Profile from Dataframe"""
                output_file_prefix = output_file_prefix.replace("_", " ").upper()
                title = f"{output_file_prefix} Profile Report"
                profile = ProfileReport(df, title, **parameter)
                return profile

            def _upload_pandas_profile(output_file_prefix, profile):
                """Upload Pandas Profile as HTML to Google Cloud Storage"""
                bytes_data = bytes(profile.html, encoding="utf-8")
                gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
                file_name = f"profile_{output_file_prefix}_{START_DATE_STRING}.html"
                object_name = f"{INTERMEDIATE_DIR}/{file_name}"
                gcs_hook.upload(
                    bucket_name=BUCKET_NAME,
                    data=bytes_data,
                    object_name=object_name,
                )

            @task(task_id="create_and_upload_pandas_profile")
            def create_pandas_profile(output_file_prefix, **parameter):
                """First create Pandas Profile from parquet and then upload HTML Profile to Google Cloud Storage"""
                df = _download_parquet_file(output_file_prefix)
                profile = _create_pandas_profile(df, output_file_prefix, **parameter)
                _upload_pandas_profile(output_file_prefix, profile)

            create_pandas_profile(output_file_prefix="guardian_content")
            create_pandas_profile(output_file_prefix="guardian_content_tags")

        profiling()

    convert_file_type_tg >> profiling_tg


dag = transform()
