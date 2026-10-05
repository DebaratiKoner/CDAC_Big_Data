from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_HOME = "/home/vboxuser/CDAC_Big_Data/airflow/telecom-usage"

CSV_FILE = f"{PROJECT_HOME}/telecom_usage.csv"
HDFS_DIR = "/user/vboxuser/telecom_usage/raw"

RAW_SQL = f"{PROJECT_HOME}/sql/01_create_raw.sql"
TRANSFORM_SQL = f"{PROJECT_HOME}/sql/02_transform.sql"


ENV_SETUP = """
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HIVE_HOME=/usr/local/hive
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$PATH
"""


with DAG(
    dag_id="telecom_usage_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["telecom", "hadoop", "hive", "etl"],
) as dag:

    upload_to_hdfs = BashOperator(
        task_id="upload_raw_data_to_hdfs",
        bash_command=f"""
        {ENV_SETUP}

        echo "Uploading telecom CSV to HDFS..."

        hdfs dfs -mkdir -p {HDFS_DIR}

        hdfs dfs -put -f {CSV_FILE} {HDFS_DIR}/telecom_usage.csv

        echo "HDFS contents:"
        hdfs dfs -ls {HDFS_DIR}
        """
    )


    create_raw_table = BashOperator(
        task_id="create_hive_raw_table",
        bash_command=f"""
        {ENV_SETUP}

        echo "Creating Hive raw table..."

        beeline -u jdbc:hive2://localhost:10001 \
        -f {RAW_SQL}
        """
    )


    transform_data = BashOperator(
        task_id="create_final_table",
        bash_command=f"""
        {ENV_SETUP}

        echo "Creating final transformed Hive table..."

        beeline -u jdbc:hive2://localhost:10001 \
        -f {TRANSFORM_SQL}
        """
    )


    verify_final_table = BashOperator(
        task_id="verify_final_output",
        bash_command=f"""
        {ENV_SETUP}

        echo "Final Telecom Usage Output:"

        beeline -u jdbc:hive2://localhost:10001 \
        -e "
        USE telecom_db;

        SELECT
            usage_id,
            customer_id,
            plan,
            data_gb,
            circle,
            usage_category
        FROM telecom_usage_final
        ORDER BY usage_id;

        SELECT
            usage_category,
            COUNT(*) AS total_customers
        FROM telecom_usage_final
        GROUP BY usage_category;
        "
        """
    )


    upload_to_hdfs >> create_raw_table >> transform_data >> verify_final_table
