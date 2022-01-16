from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.decorators import dag, task
from src.profiling import create_pandas_profile
from src.utils import (
    load_parameters,
    upload_file_to_gcs,
    download_parquet_file_from_gcs,
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
    dag_id="conform_guardian_content",
    description="Conform cleaned Data to Data Model so it can be extracted to SQL Database",
    schedule_interval="@daily",
    start_date=days_ago(1),
    end_date=datetime.today(),
    catchup=False,
    default_args=default_args,
)
def conform():

    for file_name_out, file_name_in in {
        "article": "guardian_content",
        "article_setting": "guardian_content",
        "article_text": "guardian_content",
        "tag_tone": "guardian_content_tags",
        "tag_series": "guardian_content_tags",
        "tag_tracking": "guardian_content_tags",
        "tag_contributor": "guardian_content_tags",
        "tag_keyword": "guardian_content_tags",
        "tag_newspaper_book": "guardian_content_tags",
        "tag_newspaper_book_section": "guardian_content_tags",
    }.items():

        task_id = f"conform_{file_name_out}"

        @task(task_id=task_id)
        def conform(file_name_out, file_name_in):

            df = download_parquet_file_from_gcs(
                gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
                start_date=default_args["start_date"],
                folder="03_cleaned",
                file_name=file_name_in,
                bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
            )

            parameters = default_args["params"][file_name_in]["conform"][file_name_out]

            if "article" in file_name_out:

                # Only keep specific columns
                keep_columns = parameters["keep_columns"]
                df_filtered = df.loc[:, keep_columns]

            if "tag" in file_name_out:

                # Only keep specific rows matching a certain value
                df_filtered = df[df["type"] == parameters["type"]]

                # Rename tag columns
                type_long = f"{parameters['type'].replace('-', '_')}_long"
                df_filtered = df_filtered.rename(
                    columns={
                        "tag_long": type_long,
                        "tag": parameters["type"].replace("-", "_"),
                        "content_id": "web_url_suffix",
                    }
                )

                # Drop specific columns
                drop_columns = parameters["drop_columns"]
                df_filtered = df_filtered.drop(columns=drop_columns)

            bytes_data = df_filtered.to_parquet(index=False)

            # Upload to Google Cloud Storage
            upload_file_to_gcs(
                gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
                start_date=default_args["start_date"],
                folder="04_conformed",
                file_name=file_name_out,
                file_type="parquet",
                bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
                bytes_data=bytes_data,
            )

        conform(file_name_out, file_name_in)

        task_id = f"profile_{file_name_out}"

        @task(task_id=task_id)
        def create_profile(file_name_out):
            df = download_parquet_file_from_gcs(
                gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
                start_date=default_args["start_date"],
                folder="04_conformed",
                file_name=file_name_out,
                bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
            )
            profile = create_pandas_profile(
                df=df, file_name=file_name_out, **{"explorative": True}
            )

            # Prepare for Upload of Pandas Profile
            bytes_data = bytes(profile.html, encoding="utf-8")
            file_name_profile = file_name_out + "_profile"

            upload_file_to_gcs(
                gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
                start_date=default_args["start_date"],
                folder="04_conformed",
                file_name=file_name_profile,
                file_type="html",
                bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
                bytes_data=bytes_data,
            )

        create_profile(file_name_out)


dag = conform()
