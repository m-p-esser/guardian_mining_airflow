""" General Validation tests for DAGs """

from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import test_cycle


def test_no_import_errors():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(dag_bag.import_errors) == 0, "No Import Failures"
