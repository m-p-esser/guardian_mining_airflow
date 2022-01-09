from datetime import datetime, timedelta
import pandas as pd
import json

from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.decorators import dag, task
from src.profiling import create_pandas_profile
from src.utils import (
    download_parquet_file_from_gcs,
    load_parameters,
    list_file_paths_in_gcs,
    download_json_files_from_gcs,
    upload_file_to_gcs,
)

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
    description="Transfrom data from Guardian Content to Parquet and write to Google Cloud Storage",
    schedule_interval="@daily",
    start_date=days_ago(1),
    end_date=datetime.today(),
    catchup=False,
    default_args=default_args,
)
def transform():
    @task(task_id="load_json_files")
    def load_json_files():
        filtered_file_paths = list_file_paths_in_gcs(
            gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
            start_date=default_args["start_date"],
            file_type="json",
            bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
        )

        list_of_dicts = download_json_files_from_gcs(
            gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
            file_paths=filtered_file_paths,
            bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
        )

        return list_of_dicts

    list_of_dicts = load_json_files()

    @task(task_id="normalize_guardian_content")
    def normalize_guardian_content(list_of_dicts, file_name):
        df = pd.json_normalize(
            data=list_of_dicts,
            **default_args["params"][file_name]["json_normalize"],
        )
        bytes_data = df.to_parquet()

        # Upload to Google Cloud Storage
        upload_file_to_gcs(
            gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
            start_date=default_args["start_date"],
            folder="02_intermediate",
            file_name="guardian_content",
            file_type="parquet",
            bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
            bytes_data=bytes_data,
        )

    normalized_guardian_content = normalize_guardian_content(
        list_of_dicts, "guardian_content"
    )

    @task(task_id="normalize_guardian_content_tags")
    def normalize_guardian_content_tags(list_of_dicts, file_name):
        df = pd.json_normalize(
            data=list_of_dicts,
            **default_args["params"][file_name]["json_normalize"],
        )
        bytes_data = df.to_parquet()

        # Upload to Google Cloud Storage
        upload_file_to_gcs(
            gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
            start_date=default_args["start_date"],
            folder="02_intermediate",
            file_name="guardian_content_tags",
            file_type="parquet",
            bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
            bytes_data=bytes_data,
        )

    normalized_guardian_content_tags = normalize_guardian_content_tags(
        list_of_dicts, "guardian_content_tags"
    )

    @task(task_id="profile_guardian_content")
    def create_guardian_content_profile(file_name):
        df = download_parquet_file_from_gcs(
            gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
            start_date=default_args["start_date"],
            folder="02_intermediate",
            file_name=file_name,
            bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
        )
        profile = create_pandas_profile(
            df=df, file_name=file_name, **{"explorative": True}
        )

        # Prepare for Upload of Pandas Profile
        bytes_data = bytes(profile.html, encoding="utf-8")
        file_name_profile = file_name + "_profile"

        upload_file_to_gcs(
            gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
            start_date=default_args["start_date"],
            folder="02_intermediate",
            file_name=file_name_profile,
            file_type="html",
            bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
            bytes_data=bytes_data,
        )

    guardian_content_profile = create_guardian_content_profile("guardian_content")

    @task(task_id="profile_guardian_content_tags")
    def create_guardian_content_tags_profile(file_name):
        df = download_parquet_file_from_gcs(
            gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
            start_date=default_args["start_date"],
            folder="02_intermediate",
            file_name=file_name,
            bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
        )
        profile = create_pandas_profile(
            df=df, file_name=file_name, **{"explorative": True}
        )

        # Prepare for Upload of Pandas Profile
        bytes_data = bytes(profile.html, encoding="utf-8")
        file_name_profile = file_name + "_profile"

        upload_file_to_gcs(
            gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
            start_date=default_args["start_date"],
            folder="02_intermediate",
            file_name=file_name_profile,
            file_type="html",
            bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
            bytes_data=bytes_data,
        )

    guardian_content_tags_profile = create_guardian_content_tags_profile(
        "guardian_content_tags"
    )

    list_of_dicts
    normalized_guardian_content >> guardian_content_profile
    normalized_guardian_content_tags >> guardian_content_tags_profile


dag = transform()
