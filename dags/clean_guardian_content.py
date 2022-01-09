from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.decorators import dag, task
from src.profiling import create_pandas_profile
from src.clean import clean_df
from src.utils import (
    load_parameters,
    download_parquet_file_from_gcs,
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
    dag_id="clean_guardian_content",
    description="Clean data from possible errors",
    schedule_interval="@daily",
    start_date=days_ago(1),
    end_date=datetime.today(),
    catchup=False,
    default_args=default_args,
)
def clean():
    @task(task_id="clean_guardian_content")
    def clean_guardian_content(file_name):

        df = download_parquet_file_from_gcs(
            gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
            start_date=default_args["start_date"],
            folder="02_intermediate",
            file_name=file_name,
            bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
        )

        cleaned_df = clean_df(
            df=df,
            **default_args["params"][file_name]["clean"],
        )
        bytes_data = cleaned_df.to_parquet()

        upload_file_to_gcs(
            gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
            start_date=default_args["start_date"],
            folder="03_cleaned",
            file_name=file_name,
            file_type="parquet",
            bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
            bytes_data=bytes_data,
        )

    cleaned_guardian_content = clean_guardian_content("guardian_content")

    @task(task_id="clean_guardian_content_tags")
    def clean_guardian_content_tags(file_name):

        df = download_parquet_file_from_gcs(
            gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
            start_date=default_args["start_date"],
            folder="02_intermediate",
            file_name=file_name,
            bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
        )

        cleaned_df = clean_df(
            df=df,
            **default_args["params"][file_name]["clean"],
        )
        bytes_data = cleaned_df.to_parquet()

        upload_file_to_gcs(
            gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
            start_date=default_args["start_date"],
            folder="03_cleaned",
            file_name=file_name,
            file_type="parquet",
            bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
            bytes_data=bytes_data,
        )

    cleaned_guardian_content_tags = clean_guardian_content_tags("guardian_content_tags")

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

    cleaned_guardian_content >> guardian_content_profile
    cleaned_guardian_content_tags >> guardian_content_tags_profile


dag = clean()
