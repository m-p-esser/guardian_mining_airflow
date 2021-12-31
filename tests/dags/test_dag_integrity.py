""" General Validation tests for DAGs """

from airflow.models import DagBag


def test_no_import_errors():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(dag_bag.import_errors) == 0, "No Import Failures"


# def test_all_dags_have_tags():
#     dag_bag = DagBag(dag_folder="dags/", include_examples=False)
#     for dag_id, dag in dag_bag.dags.items():
#         error_message = f"{dag_id} in {dag.full_filepath} has no tags"
#         assert dag.tags, error_message


# def test_retries_present():
#     dag_bag = DagBag(dag_folder="dags/", include_examples=False)
#     for dag in dag_bag.dags:
#         retries = dag_bag.dags[dag].default_args.get("retries", [])
#         error_msg = "Retries not set to 2 for DAG {id}".format(id=dag)
#         assert retries == 2, error_msg
