import datetime
import pytest
from airflow import DAG

pytest_plugins = ["helpers_namespace"]


@pytest.fixture
def test_dag():
    return DAG(
        "test_dag",
        default_args={"owner": "airflow", "start_date": datetime.datetime(2020, 1, 1)},
        schedule_interval="@once",
    )


@pytest.helpers.register
def run_task(task, dag):
    dag.clear()
    task.run(
        start_date=dag.default_args["start_date"],
        end_date=dag.default_args["start_date"],
    )
