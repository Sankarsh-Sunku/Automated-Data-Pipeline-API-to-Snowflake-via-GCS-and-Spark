from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from fetchdata import fetch_data_from_api
from datetime import date, timedelta, datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator
)


default_args = {
    'owner': 'sunku-sankarsh',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'news_data_to_gcs',
    default_args=default_args,
    description='Fetch news articles from Api and Save as parquet in GCS',
    schedule_interval=None,
    catchup=False,#If start date is past then it wont run for the past times also if it is false
    start_date=datetime(2024, 12, 11),
    )

#Cluster Properties for Running PySpark
CLUSTER_NAME = 'snowflake-spark-airflow-cluster'
PROJECT_ID = 'euphoric-diode-442615-u6'
REGION = 'us-central1'
CLUSTER_CONFIG = {
    "master_config" : {
        "num_instances" : 1,
        "machine_type_uri" : "n1-standard-2",
        "disk_config" : {
            "boot_disk_type" : "pd-standard",
            "boot_disk_size_gb" : 30
        }
    },
    "worker_config" : {
        "num_instances" : 2,
        "machine_type_uri" : "n1-standard-2",
        "disk_config" : {
            "boot_disk_type" : "pd-standard",
            "boot_disk_size_gb" : 30
        }
    },
    "software_config" : {
        "image_version" : "2.2.26-debian12"
    }
}



fetch_news_data = PythonOperator(
    task_id='fetch_news_from_api',
    python_callable=fetch_data_from_api,
    dag=dag
)


# Creation Of Cluster
create_dataproc_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    dag=dag
)

#Spark Submit Job
submit_spark_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_spark_job',
    main='gs://snowflake_projects_sa/news_data_analysis/pyspark-job.py',#pyspark code path where it is uploaded in GCS
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',  # ensures cluster deletion even if Spark job fails
    dag=dag,
)

snowflake_create_table = SnowflakeOperator(
    task_id="snowflake_create_table",
    sql="""CREATE TABLE IF NOT EXISTS news_api.PUBLIC.news_api_data USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(INFER_SCHEMA (
                    LOCATION => '@news_api.PUBLIC.gcs_raw_data_stage',
                    FILE_FORMAT => 'parquet_format'
                ))
            )""",
    snowflake_conn_id="snowflake_conn"
)

snowflake_copy = SnowflakeOperator(
    task_id="snowflake_copy_from_stage",
    sql="""COPY INTO news_api.PUBLIC.news_api_data 
            FROM @news_api.PUBLIC.gcs_raw_data_stage
            MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE 
            FILE_FORMAT = (FORMAT_NAME = 'parquet_format') 
            """,
    snowflake_conn_id="snowflake_conn"
)

snowflake_create_table_author = SnowflakeOperator(
    task_id="snowflake_create_table_author",
    sql="""CREATE TABLE IF NOT EXISTS news_api.PUBLIC.author_activity USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(INFER_SCHEMA (
                    LOCATION => '@news_api.PUBLIC.gcs_author_date',
                    FILE_FORMAT => 'parquet_format'
                ))
            )""",
    snowflake_conn_id="snowflake_conn"
)

snowflake_copy_author = SnowflakeOperator(
    task_id="snowflake_copy_from_stage_author",
    sql="""COPY INTO news_api.PUBLIC.author_activity 
            FROM @news_api.PUBLIC.gcs_author_date/*.parquet
            MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE 
            FILE_FORMAT = (FORMAT_NAME = 'parquet_format') 
            """,
    snowflake_conn_id="snowflake_conn"
)

news_summary_task = SnowflakeOperator(
    task_id="create_or_replace_news_summary_tb",
    sql="""
        CREATE OR REPLACE TABLE news_api.PUBLIC.summary_news AS
        SELECT
            "source" AS news_source,
            COUNT(*) AS article_count,
            MAX("timestamp") AS latest_article_date,
            MIN("timestamp") AS earliest_article_date
        FROM news_api.PUBLIC.news_api_data as tb
        GROUP BY "source"
        ORDER BY article_count DESC;
    """,
    snowflake_conn_id="snowflake_conn"
)

# author_activity_task = SnowflakeOperator(
#     task_id="create_or_replace_author_activity_tb",
#     sql="""
#         CREATE OR REPLACE TABLE news_api.PUBLIC.author_activity AS
#         SELECT
#             "author",
#             COUNT(*) AS article_count,
#             MAX("timestamp") AS latest_article_date,
#             COUNT(DISTINCT "source") AS distinct_sources
#         FROM news_api.PUBLIC.news_api_data as tb
#         WHERE "author" IS NOT NULL
#         GROUP BY "author"
#         ORDER BY article_count DESC;
#     """,
#     snowflake_conn_id="snowflake_conn"
# )


fetch_news_data >> create_dataproc_cluster >> submit_spark_job >> delete_cluster >> snowflake_create_table >> snowflake_copy >> snowflake_create_table_author >> snowflake_copy_author >> news_summary_task
