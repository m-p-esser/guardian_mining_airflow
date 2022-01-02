from datetime import datetime, timedelta
import json
import requests

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.decorators import dag, task
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
    dag_id="extract_guardian_content",
    description="Extract data from Guardian Content API and store it in GCS Bucket",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
)
def extract():
    @task(task_id="get_guardian_content")
    def get_guardian_content(parameters):
        """Call Guardian Search Endpoint and collect Content data"""
        guardian_content = []
        current_page = 1
        total_pages = 1  # there is expected to be a mininum of 1 page for each request
        base_url = parameters["base_url"]

        while current_page <= total_pages:
            params = {
                "from-date": str(default_args["start_date"].strftime("%Y-%m-%d")),
                "to-date": str(default_args["end_date"].strftime("%Y-%m-%d")),
                "order-by": parameters["order_by"],
                "show-fields": parameters["show_fields"],
                "show-tags": parameters["show_tags"],
                "page-size": parameters["page_size"],
                "api-key": Variable.get("GUARDIAN_API_KEY"),
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

    @task(task_id="upload_guardian_content_to_gcs_bucket")
    def upload_guardian_content_to_gcs_bucket(guardian_content):
        """Store Results from Guardian API as JSON"""
        for content in guardian_content:

            # Convert to bytes data
            unique_id = content["id"].replace("/", "-")
            bytes_data = json.dumps(content)

            # Upload to Google Cloud Storage
            gcs_hook = GCSHook(gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"))
            start_date_string = str(default_args["start_date"].strftime("%Y-%m-%d"))
            object_name = f"01_raw/{start_date_string}/{unique_id}.json"
            gcs_hook.upload(
                bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
                data=bytes_data,
                object_name=object_name,
            )

    upload_guardian_content_to_gcs_bucket(
        guardian_content=get_guardian_content(default_args["params"]["guardian_api"])
    )


dag = extract()
