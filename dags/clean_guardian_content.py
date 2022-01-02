from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from src.profiling import create_pandas_profile
from src.clean import (
    remove_html_tags_in_df,
    convert_string_to_datetime_in_df,
    camel_to_snake_in_df,
    rename_columns,
)
from src.utils import load_parameters

# Default Args which are used in DAG
start_date = datetime(2021, 12, 31)
end_date = start_date + timedelta(days=1)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True,
    "start_date": start_date,
    "end_date": end_date,
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
def clean_data():

    # Task Group to clean Dataframe
    with TaskGroup(group_id="clean_parquet") as clean_tg:

        def clean():
            def _download_parquet_file(output_file_prefix):
                """Download parquet File from GCS Bucket"""

                gcs_hook = GCSHook(
                    gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID")
                )

                # Prepare variables for download
                bucket_name = Variable.get("DATA_GOOGLE_CLOUD_STORAGE")
                start_date_string = str(default_args["start_date"].strftime("%Y-%m-%d"))
                object_url = f"gs://{bucket_name}/02_intermediate/{output_file_prefix}_{start_date_string}.parquet"

                # Download Files
                with gcs_hook.provide_file(object_url=object_url) as f:
                    df = pd.read_parquet(f)
                    return df

            def clean_parquet(
                df,
                duplicate_identifier=None,
                drop_columns=None,
                html_columns=None,
                date_columns=None,
                prefixes_to_remove=None,
                rename_columns_mapping=None,
            ):
                """Run different cleaning functions over Dataframe"""

                # Drop specific Columns
                if isinstance(drop_columns, list):
                    if len(drop_columns) > 0:
                        df = df.drop(columns=drop_columns)

                # Deduplication
                if isinstance(duplicate_identifier, str):
                    df = df.drop_duplicates(subset=duplicate_identifier)

                # Replace Missings with Empty String to do String Operations
                df = df.fillna("")

                # Remove Html tags for specific Columns
                df = remove_html_tags_in_df(df, html_columns)

                # Replace Empty String with Missing for all Columns
                df = df.replace("", np.nan)

                # Remove Whitespaces for all Columns
                df = df.apply(lambda x: x.str.strip(), axis=1)

                # Convert String Columns containg dates to Datetime
                df = convert_string_to_datetime_in_df(df, date_columns)

                # Convert Camel Cases Column Names to Snake Case for all Columns
                df = camel_to_snake_in_df(df)

                # Rename specific Columns
                rename_columns(df, prefixes_to_remove, rename_columns_mapping)

                cleaned_df = df.copy()
                return cleaned_df

            @task(task_id="store_parquet_in_gcs_bucket")
            def store_parquet_in_gcs_bucket(df, output_file_prefix, **parameter):
                """Save parquet in Google Cloud Storage"""

                bytes_data = df.to_parquet()

                # Upload to GCS Bucket
                gcs_hook = GCSHook(
                    gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID")
                )

                # Prepare variables for upload
                start_date_string = str(default_args["start_date"].strftime("%Y-%m-%d"))
                object_name = (
                    f"03_cleaned/{output_file_prefix}_{start_date_string}.parquet"
                )
                bucket_name = Variable.get("DATA_GOOGLE_CLOUD_STORAGE")

                # Upload Files
                gcs_hook.upload(
                    bucket_name=bucket_name, data=bytes_data, object_name=object_name
                )

            # Clean Guardian Content (Articles etc.)
            df = _download_parquet_file(output_file_prefix="guardian_content")
            cleaned_df = clean_parquet(
                df=df,
                **default_args["params"]["guardian_content"]["clean"],
            )
            store_parquet_in_gcs_bucket(
                df=cleaned_df, output_file_prefix="guardian_content"
            )

            # Clean Guardian Content Tags
            df = _download_parquet_file(output_file_prefix="guardian_content_tags")
            cleaned_df = clean_parquet(
                df=df,
                **default_args["params"]["guardian_content_tags"]["clean"],
            )
            store_parquet_in_gcs_bucket(
                df=cleaned_df, output_file_prefix="guardian_content_tags"
            )

        clean()

    # Task to create and upload Pandas Profile
    with TaskGroup(group_id="profiling") as profiling_tg:

        def profiling():
            def _download_parquet_file(output_file_prefix):
                """Download parquet File from GCS Bucket"""

                gcs_hook = GCSHook(
                    gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID")
                )

                # Prepare variables for download
                bucket_name = Variable.get("DATA_GOOGLE_CLOUD_STORAGE")
                start_date_string = str(default_args["start_date"].strftime("%Y-%m-%d"))
                object_url = f"gs://{bucket_name}/03_cleaned/{output_file_prefix}_{start_date_string}.parquet"

                # Download file
                with gcs_hook.provide_file(object_url=object_url) as f:
                    df = pd.read_parquet(f)
                    return df

            def _upload_profile(output_file_prefix, profile):
                """Upload Pandas Profile as HTML to Google Cloud Storage"""
                bytes_data = bytes(profile.html, encoding="utf-8")
                gcs_hook = GCSHook(
                    gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID")
                )

                # Prepare variables for Upload
                bucket_name = Variable.get("DATA_GOOGLE_CLOUD_STORAGE")
                start_date_string = str(default_args["start_date"].strftime("%Y-%m-%d"))
                file_name = f"profile_{output_file_prefix}_{start_date_string}.html"
                object_name = f"03_cleaned/{file_name}"

                # Upload Profile
                gcs_hook.upload(
                    bucket_name=bucket_name,
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

    clean_tg >> profiling_tg


dag = clean_data()
