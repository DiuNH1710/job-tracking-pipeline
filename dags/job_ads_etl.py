# /opt/airflow/dags/job_ads_etl.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, timedelta
import json
import os

from etl.transform import transform_data

# Path lưu last processed timestamp
LAST_TS_FILE = "/opt/airflow/dags/last_ts.json"


def read_last_ts():
    if os.path.exists(LAST_TS_FILE):
        with open(LAST_TS_FILE, "r") as f:
            data = json.load(f)
            return data.get("last_ts")
    return None


def save_last_ts(ts):
    with open(LAST_TS_FILE, "w") as f:
        json.dump({"last_ts": ts}, f)


def etl_job(**kwargs):
    spark = SparkSession.builder \
        .appName("JobAdsETL") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                "mysql:mysql-connector-java:8.0.33") \
        .getOrCreate()

    # Kiểm tra config
    conf = spark.sparkContext.getConf()
    print("Cassandra host:", conf.get("spark.cassandra.connection.host", "N/A"))
    print("Cassandra port:", conf.get("spark.cassandra.connection.port", "N/A"))
    print("Jars packages:", conf.get("spark.jars.packages", "N/A"))

    # Lấy last processed timestamp
    last_ts = read_last_ts()

    # Đọc dữ liệu từ Cassandra
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="tracking", keyspace="job_ads") \
        .load()

    if last_ts:
        df = df.filter(col("ts") > last_ts)

    if df.rdd.isEmpty():
        print("No new records to process.")
        spark.stop()
        return

    # Transform dữ liệu
    result = transform_data(df)

    # Ghi dữ liệu ra MySQL
    mysql_url = "jdbc:mysql://mysql:3306/ads_analytics"
    props = {"user": "root", "password": "123456", "driver": "com.mysql.cj.jdbc.Driver"}

    result["raw"].write.jdbc(mysql_url, "events", "append", props)
    result["campaign_summary"].write.jdbc(mysql_url, "campaign_summary", "append", props)
    result["publisher_summary"].write.jdbc(mysql_url, "publisher_summary", "append", props)
    result["industry_summary"].write.jdbc(mysql_url, "industry_summary", "append", props)
    result["hourly_stats"].write.jdbc(mysql_url, "hourly_stats", "append", props)
    result["daily_stats"].write.jdbc(mysql_url, "daily_stats", "append", props)

    # Lưu lại timestamp mới nhất
    max_ts = df.agg({"ts": "max"}).collect()[0][0].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    save_last_ts(max_ts)
    print(f"Processed up to ts={max_ts}")

    spark.stop()


# -----------------------
# DAG definition
# -----------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'job_ads_etl',
    default_args=default_args,
    description='ETL job ads data from Cassandra to MySQL',
    schedule='@hourly',
    catchup=False
)

run_etl = PythonOperator(
    task_id='run_etl',
    python_callable=etl_job,
    dag=dag
)
