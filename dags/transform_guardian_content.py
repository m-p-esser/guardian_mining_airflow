from datetime import datetime, timedelta
import pandas as pd
import json

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from src.profiling import create_pandas_profile
from src.utils import load_parameters

# Default Args which are used in DAG
start_date = datetime(2021, 12, 31, 6, 0, 0)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True,
    "start_date": start_date,
    "params": load_parameters("guardian_mining_params.yml"),
}


@dag(
    dag_id="transform_guardian_content",
    description="Transfrom data from Guardian Content as parquet and write to Google Cloud Storage",
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
                gcs_hook = GCSHook(
                    gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID")
                )
                file_paths = gcs_hook.list(
                    bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
                    delimiter=delimiter,
                )
                filtered_file_paths = [
                    fp
                    for fp in file_paths
                    if str(default_args["start_date"].strftime("%Y-%m-%d")) in fp
                ]
                return filtered_file_paths

            @task(task_id="load_json_from_gcs_bucket")
            def load_json_from_gcs_bucket(file_paths):
                """Return JSON Files from Google Cloud Storage Bucket"""
                gcs_hook = GCSHook(
                    gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID")
                )

                list_of_dicts = []
                for fp in file_paths:
                    byte_data = gcs_hook.download(
                        bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
                        object_name=fp,
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
                list_of_dicts, output_file_prefix, parameter
            ):
                """Save parquet in Google Cloud Storage"""

                # Normalize JSON to Dataframe
                df = _json_to_parquet(list_of_dicts, **parameter)
                bytes_data = df.to_parquet()

                # Upload to GCS Bucket
                gcs_hook = GCSHook(
                    gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID")
                )
                start_date_string = str(default_args["start_date"].strftime("%Y-%m-%d"))
                object_name = (
                    f"02_intermediate/{output_file_prefix}_{start_date_string}.parquet"
                )
                gcs_hook.upload(
                    bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
                    data=bytes_data,
                    object_name=object_name,
                )

            file_paths = list_file_paths_in_gcs_bucket(delimiter=".json")
            list_of_dicts = load_json_from_gcs_bucket(file_paths)
            store_parquet_in_gcs_bucket(
                list_of_dicts=list_of_dicts,
                output_file_prefix="guardian_content",
                parameter=default_args["params"]["guardian_content"]["json_normalize"],
            )
            store_parquet_in_gcs_bucket(
                list_of_dicts=list_of_dicts,
                output_file_prefix="guardian_content_tags",
                parameter=default_args["params"]["guardian_content_tags"][
                    "json_normalize"
                ],
            )

        convert_file_type()

    # Task to create and upload Pandas Profile
    with TaskGroup(group_id="profiling") as profiling_tg:

        def profiling():
            def _download_parquet_file(output_file_prefix):
                """Download parquet File from GCS Bucket"""

                gcs_hook = GCSHook(
                    gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID")
                )
                data_storage = Variable.get("DATA_GOOGLE_CLOUD_STORAGE")
                start_date_string = str(default_args["start_date"].strftime("%Y-%m-%d"))
                object_url = f"gs://{data_storage}/02_intermediate/{output_file_prefix}_{start_date_string}.parquet"

                with gcs_hook.provide_file(object_url=object_url) as f:
                    df = pd.read_parquet(f)
                    return df

            def _upload_profile(output_file_prefix, profile):
                """Upload Pandas Profile as HTML to Google Cloud Storage"""
                bytes_data = bytes(profile.html, encoding="utf-8")
                gcs_hook = GCSHook(
                    gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID")
                )
                start_date_string = str(default_args["start_date"].strftime("%Y-%m-%d"))
                file_name = f"profile_{output_file_prefix}_{start_date_string}.html"
                object_name = f"02_intermediate/{file_name}"
                gcs_hook.upload(
                    bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
                    data=bytes_data,
                    object_name=object_name,
                )

            @task(task_id="create_and_upload_pandas_profile")
            def create_profile(output_file_prefix, **parameter):
                """First create Pandas Profile from parquet and then upload HTML Profile to Google Cloud Storage"""
                df = _download_parquet_file(output_file_prefix)
                profile = create_pandas_profile(
                    df, output_file_prefix, **{"explorative": True}
                )
                _upload_profile(output_file_prefix, profile)

            create_profile(output_file_prefix="guardian_content")
            create_profile(output_file_prefix="guardian_content_tags")

        profiling()

    convert_file_type_tg >> profiling_tg


dag = transform()
