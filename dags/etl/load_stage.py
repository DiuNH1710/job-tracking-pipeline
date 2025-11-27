from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os, json

def load_stage(df_result):
    spark = SparkSession.builder \
        .appName("JobAdsStageWriter") \
        .config("spark.jars.packages",
                "mysql:mysql-connector-java:8.0.33") \
        .getOrCreate()

    mysql_url = "jdbc:mysql://mysql:3306/ads_analytics"
    props = {"user": "root", "password": "123456", "driver": "com.mysql.cj.jdbc.Driver"}

    table_map = {
        "raw": "events_stage",
        "campaign_summary": "campaign_summary_stage",
        "publisher_summary": "publisher_summary_stage",
        "industry_summary": "industry_summary_stage",
        "hourly_stats": "hourly_stats_stage",
        "daily_stats": "daily_stats_stage"
    }

    for name, df_table in df_result.items():
        stage_table = table_map[name]
        print(f"Writing → {stage_table}")
        df_table.write.jdbc(mysql_url, stage_table, "append", props)

    spark.stop()
