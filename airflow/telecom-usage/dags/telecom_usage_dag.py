from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_HOME = "/home/vboxuser/CDAC_Big_Data/airflow/telecom-usage"
SQL_DIR = f"{PROJECT_HOME}/sql"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
}


with DAG(
    dag_id="telecom_usage_etl",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["telecom", "hdfs", "hive", "etl"],
) as dag:

    upload_to_hdfs = BashOperator(
        task_id="upload_to_hdfs",
        bash_command=f"""
        hdfs dfs -mkdir -p /telecom/input
        hdfs dfs -put -f {PROJECT_HOME}/telecom_usage_no_header.csv /telecom/input/
        hdfs dfs -ls /telecom/input
        """,
    )

    create_hive_raw_table = BashOperator(
        task_id="create_hive_raw_table",
        bash_command=f"""
        beeline -u "jdbc:hive2://localhost:10001" \
        -f {SQL_DIR}/01_create_raw_table.sql
        """,
    )

    load_hive_raw_table = BashOperator(
        task_id="load_hive_raw_table",
        bash_command=f"""
        beeline -u "jdbc:hive2://localhost:10001" \
        -f {SQL_DIR}/02_load_raw_table.sql
        """,
    )

    create_hive_final_table = BashOperator(
        task_id="create_hive_final_table",
        bash_command=f"""
        beeline -u "jdbc:hive2://localhost:10001" \
        -f {SQL_DIR}/03_create_final_table.sql
        """,
    )

    verify_final_output = BashOperator(
        task_id="verify_final_output",
        bash_command="""
        beeline -u "jdbc:hive2://localhost:10001" \
        -e "
        SELECT COUNT(*) FROM telecom_usage_final;
        SELECT usage_category, COUNT(*)
        FROM telecom_usage_final
        GROUP BY usage_category;
        "
        """,
    )

    upload_to_hdfs >> create_hive_raw_table >> load_hive_raw_table >> create_hive_final_table >> verify_final_output
