from datetime import datetime, timedelta
import json
import pathlib
import requests

from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.decorators import dag, task

BUCKET_NAME = Variable.get("DATA_GOOGLE_CLOUD_STORAGE")
GCP_CONN_ID = Variable.get("GCP_GUARDIAN_MINING_CONN_ID")
GUARDIAN_API_KEY = Variable.get("GUARDIAN_API_KEY")
ROOT_DIR = pathlib.Path().cwd()
TMP_DIR = ROOT_DIR / "tmp"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["emarcphilipp@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
    "provide_context": True,
}


@task
def get_guardian_content():
    """Call Guardian Search Endpoint and collect Content data"""
    guardian_content = []
    current_page = 1
    total_pages = 1  # there is expected to be a mininum of 1 page for each request
    base_url = "https://content.guardianapis.com/search"

    while current_page <= total_pages:
        params = {
            "from-date": dag.start_date.strftime("%Y-%m-%d"),
            "to-date": dag.end_date.strftime("%Y-%m-%d"),
            "order-by": "oldest",
            "show-fields": "all",
            "show-tags": "all",
            "page-size": 200,
            "api-key": GUARDIAN_API_KEY,
            "page": current_page,
        }

        response = requests.get(url=base_url, params=params)
        response.raise_for_status()

        # Store data in List of Dictionaries
        json_data = response.json()["response"]
        results = json_data["results"]
        guardian_content.extend(results)

        # Increment Page Count
        total_pages = json_data["pages"]
        current_page += 1

        # Break While Loop when current page is the last page
        if current_page > total_pages:
            break

    return guardian_content


@task
def store_guardian_content_locally(results):
    """Store Results from Guardian API as JSON"""
    for result in results[0:5]:
        unique_id = result["id"].replace("/", "-")
        file_path = TMP_DIR / f"{unique_id}.json"
        with open(file_path, "w") as fp:
            json.dump(result, fp)


@task
def clean_tmp_dir():
    """Remove all files in Temp folder"""
    tmp_filepaths = list(TMP_DIR.rglob("*"))
    [fp.unlink() for fp in tmp_filepaths]


@dag(
    dag_id="extract_guardian_content",
    description="Extract data from Guardian Content API and store it in GCS Bucket",
    schedule_interval="@daily",
    start_date=days_ago(1),
    end_date=datetime.today(),
    catchup=False,
    default_args=default_args,
)
def extract():

    guardian_content = get_guardian_content()
    store_guardian_content_locally(guardian_content)

    LocalFilesystemToGCSOperator(
        task_id="upload_local_files_to_gcs",
        src=list(TMP_DIR.rglob("*.json")),
        dst="01_raw/",
        bucket=BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID,
    )

    clean_tmp_dir()


dag = extract()
