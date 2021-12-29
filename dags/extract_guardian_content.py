from datetime import datetime, timedelta
import json
import pathlib
import requests

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
GCP_CONN_ID = Variable.get("GCP_GUARDIAN_MINING_CONN_ID")
GUARDIAN_API_KEY = Variable.get("GUARDIAN_API_KEY")
ROOT_DIR = pathlib.Path().cwd()
TMP_DIR = ROOT_DIR / "tmp"
START_DATE_STRING = str(default_args["start_date"].strftime("%Y-%m-%d"))
END_DATE_STRING = str(default_args["end_date"].strftime("%Y-%m-%d"))


@task
def get_guardian_content():
    """Call Guardian Search Endpoint and collect Content data"""
    guardian_content = []
    current_page = 1
    total_pages = 1  # there is expected to be a mininum of 1 page for each request
    base_url = "https://content.guardianapis.com/search"

    while current_page <= total_pages:
        params = {
            "from-date": START_DATE_STRING,
            "to-date": END_DATE_STRING,
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
def upload_guardian_content_to_gcs_bucket(guardian_content):
    """Store Results from Guardian API as JSON"""
    for content in guardian_content:
        unique_id = content["id"].replace("/", "-")
        bytes_data = json.dumps(content)
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        object_name = f"01_raw/{START_DATE_STRING}/{unique_id}.json"
        gcs_hook.upload(
            bucket_name=BUCKET_NAME, data=bytes_data, object_name=object_name
        )


@dag(
    dag_id="extract_guardian_content",
    description="Extract data from Guardian Content API and store it in GCS Bucket",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
)
def extract():

    upload_guardian_content_to_gcs_bucket(get_guardian_content())


dag = extract()
