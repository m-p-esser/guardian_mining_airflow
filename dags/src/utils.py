""" Misc Utility Functions  """

import pathlib
import yaml
import datetime
from typing import Dict, List, Any
import json

import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def load_parameters(file_name: str) -> Dict[str, Any]:
    """Load parameters from .yaml file"""
    root_dir = pathlib.Path.cwd()
    dags_dir = root_dir / "dags"
    matching_filepaths = list(dags_dir.rglob(pattern=file_name))
    if len(matching_filepaths) > 1:
        raise Exception(
            f"Too many parameter files. There are {len(matching_filepaths)} \
        files matching pattern {file_name}. The following files match the pattern: {matching_filepaths}"
        )
    else:
        parameters_file_path = matching_filepaths[0]
        with open(parameters_file_path) as yaml_file:
            parameters = yaml.safe_load(yaml_file)
            return parameters


def download_json_files_from_gcs(
    gcp_conn_id: str, file_paths: str, bucket_name: str
) -> List[Dict[str, Any]]:
    """Download Files from Google Cloud Storage"""
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)

    list_of_dicts = []
    for fp in file_paths:
        byte_data = gcs_hook.download(
            bucket_name=bucket_name,
            object_name=fp,
        )
        dict_data = json.loads(byte_data)
        list_of_dicts.append(dict_data)

    return list_of_dicts


def download_parquet_file_from_gcs(
    gcp_conn_id: str,
    start_date: datetime.datetime,
    folder: str,
    file_name: str,
    bucket_name: str,
):
    """Download Parquet File from Google Cloud Storage Bucket"""

    # Initialize GCS Connection
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)

    # Prepare object url
    start_date_string = str(start_date.strftime("%Y-%m-%d"))
    object_url = f"gs://{bucket_name}/{folder}/{start_date_string}/{file_name}.parquet"

    # Finally download file
    with gcs_hook.provide_file(object_url=object_url) as file_handler:
        df = pd.read_parquet(file_handler)
        return df


def upload_file_to_gcs(
    gcp_conn_id: str,
    start_date: datetime.datetime,
    folder: str,
    file_name: str,
    file_type: str,
    bucket_name: str,
    bytes_data: bytes,
):
    """Upload File to Google Cloud Storage Bucket"""

    # Initialize GCS Connection
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)

    # Prepare object name
    start_date_string = str(start_date.strftime("%Y-%m-%d"))
    object_name = f"{folder}/{start_date_string}/{file_name}.{file_type}"

    # Finally upload file
    gcs_hook.upload(
        bucket_name=bucket_name,
        data=bytes_data,
        object_name=object_name,
    )


def list_file_paths_in_gcs(
    gcp_conn_id: str,
    start_date: datetime.datetime,
    file_type: str,
    bucket_name: str,
) -> List[str]:

    """Return Filepaths of Files stored in Google Cloud Storage Bucket"""

    # Initialize GCS Connection
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)

    # Prepare variables
    delimiter = "." + file_type
    start_date = str(start_date.strftime("%Y-%m-%d"))

    # Get Filepaths
    file_paths = gcs_hook.list(
        bucket_name=bucket_name,
        delimiter=delimiter,
    )

    # Filter according to substring in file name
    filtered_file_paths = [fp for fp in file_paths if start_date in fp]

    return filtered_file_paths


def json_to_parquet(list_of_dicts: List[Dict], **parameter: Dict[str, Any]):
    """Normalize List of Dictionary to Pandas Dataframe"""
    df = pd.json_normalize(data=list_of_dicts, **parameter)
    return df
