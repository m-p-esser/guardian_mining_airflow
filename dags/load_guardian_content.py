from datetime import datetime, timedelta
import pandas as pd

from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.decorators import dag, task
from src.utils import (
    load_parameters,
    download_parquet_file_from_gcs,
    upload_file_to_gcs,
)
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from sqlalchemy import engine

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
    dag_id="load_guardian_content",
    description="Load conformed data into SQL Database",
    schedule_interval="@daily",
    start_date=days_ago(1),
    end_date=datetime.today(),
    catchup=False,
    default_args=default_args,
)
def load():

    table_names = [
        "article",
        "article_setting",
        "article_text",
        "tag_contributor",
        "tag_keyword",
        "tag_tracking",
        "tag_tone",
        "tag_series",
        "tag_newspaper_book",
        "tag_newspaper_book_section",
    ]

    for table_name in table_names:

        # Download File and load into memory as Pandas Dataframe
        df = download_parquet_file_from_gcs(
            gcp_conn_id=Variable.get("GCP_GUARDIAN_MINING_CONN_ID"),
            start_date=default_args["start_date"],
            folder="04_conformed",
            file_name=table_name,
            bucket_name=Variable.get("DATA_GOOGLE_CLOUD_STORAGE"),
        )

        # Create connection to Database and Write Parquet file into Database
        mysql_hook = MySqlHook(mysql_conn_id="mysql_guardian_mining_conn_id")
        engine = mysql_hook.get_sqlalchemy_engine()

        # Retrieve Article ID
        if "tag" in table_name:

            with engine.connect() as conn:
                df_article = pd.read_sql_table(
                    table_name="article",
                    con=conn,
                    index_col=False,
                )
                df_article = df_article.loc[:, ["id", "web_url_suffix"]]
                df_article = df_article.rename(columns={"id": "article_id"})

                df = df.merge(df_article, how="inner", on="web_url_suffix")

        # Then write to Temp Table
        with engine.connect() as conn:
            df.to_sql(
                name="temp",
                con=conn,
                schema="guardian_mining",
                if_exists="replace",
                index=False,
                method=None,
            )

        # Then copy Temp Table to actual Table, this way duplicates get ignored when writing
        with engine.begin() as conn:
            insert_sql = f"""INSERT IGNORE INTO {table_name} ({', '.join(list(df.columns))}) SELECT * FROM temp"""
            conn.execute(insert_sql)


dag = load()
