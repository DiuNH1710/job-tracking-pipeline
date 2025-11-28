# job_ads_etl.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from etl.transform import transform_data
from sqlalchemy import create_engine, text

LAST_TS_FILE = "/opt/airflow/dags/last_ts.json"


# ----------------- Helper functions -----------------
def read_last_ts():
    if os.path.exists(LAST_TS_FILE):
        return json.load(open(LAST_TS_FILE)).get("last_ts")
    return None


def save_last_ts(ts):
    json.dump({"last_ts": ts}, open(LAST_TS_FILE, "w"))


def upsert_to_mysql(df, table_name, primary_keys, mysql_url, props, agg_columns=None):
    """
    df: pyspark DataFrame
    table_name: name of summary table
    primary_keys: list of PK columns
    agg_columns: dict, columns to aggregate by sum when duplicate
    """
    if df.rdd.isEmpty():
        return

    data_pd = df.toPandas()
    cols = df.columns

    insert_expr = ", ".join([f"`{c}`" for c in cols])
    values_expr = ", ".join([f"%({c})s" for c in cols])

    # Build update expression
    update_expr_parts = []
    for c in cols:
        if c in primary_keys:
            continue
        if agg_columns and c in agg_columns:
            # Cộng dồn giá trị
            update_expr_parts.append(f"`{c}` = `{c}` + VALUES(`{c}`)")
        else:
            # Ghi đè (ví dụ trung bình)
            update_expr_parts.append(f"`{c}` = VALUES(`{c}`)")
    update_expr = ", ".join(update_expr_parts)

    engine = create_engine(mysql_url, connect_args=props)
    with engine.begin() as conn:
        conn.execute(text(
            f"INSERT INTO {table_name} ({insert_expr}) VALUES "
            + ", ".join([f"({', '.join([repr(v) for v in row])})" for row in data_pd.values])
            + f" ON DUPLICATE KEY UPDATE {update_expr}"
        ))


# ----------------- Main ETL -----------------
def spark_etl(**kwargs):
    spark = SparkSession.builder \
        .appName("JobAdsETL") \
        .config("spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                "mysql:mysql-connector-java:8.0.33") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    last_ts = read_last_ts()

    # Read incremental data from Cassandra
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="tracking", keyspace="job_ads") \
        .load()

    if last_ts:
        df = df.filter(col("ts") > last_ts)

    if df.rdd.isEmpty():
        print("No new data.")
        spark.stop()
        return

    # Transform
    result = transform_data(df)

    # MySQL connection
    mysql_url = "mysql+pymysql://root:123456@mysql:3306/ads_analytics"
    props = {"charset": "utf8mb4"}

    # 1. Write raw events
    result["raw"].write.jdbc(
        "jdbc:mysql://mysql:3306/ads_analytics",
        "events",
        mode="append",
        properties={"user": "root", "password": "123456", "driver": "com.mysql.cj.jdbc.Driver"}
    )

    # 2. Upsert summary tables with aggregation for cumulative fields
    sum_columns = {
        "total_impressions": "sum",
        "total_clicks": "sum",
        "total_applies": "sum",
        "clicks": "sum",
        "applications": "sum",
        "impressions": "sum",
        "applies": "sum"
    }

    upsert_to_mysql(result["campaign_summary"], "campaign_summary", ["campaign_id"], mysql_url, props,
                    agg_columns=sum_columns)
    upsert_to_mysql(result["publisher_summary"], "publisher_summary", ["publisher_id"], mysql_url, props,
                    agg_columns=sum_columns)
    upsert_to_mysql(result["industry_summary"], "industry_summary", ["industry"], mysql_url, props,
                    agg_columns=sum_columns)
    upsert_to_mysql(result["hourly_stats"], "hourly_stats", ["hour"], mysql_url, props, agg_columns=sum_columns)
    upsert_to_mysql(result["daily_stats"], "daily_stats", ["day_of_week"], mysql_url, props, agg_columns=sum_columns)

    # 3. Update last_ts after insert
    try:
        max_ts_row = df.agg({"ts": "max"}).collect()
        if max_ts_row and max_ts_row[0][0]:
            max_ts = max_ts_row[0][0].isoformat()
            save_last_ts(max_ts)
    except Exception as e:
        print("Warning: failed to compute max_ts", e)
    finally:
        spark.stop()


# ----------------- DAG -----------------
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'job_ads_etl',
    default_args=default_args,
    schedule=None,
    catchup=False
)

spark_etl_task = PythonOperator(
    task_id='spark_etl',
    python_callable=spark_etl,
    dag=dag
)
