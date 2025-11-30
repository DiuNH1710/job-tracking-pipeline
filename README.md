# 📘 Job Ads Analytics – Incremental ETL Pipeline

## Project Overview

This project implements a full end-to-end Incremental Data Pipeline for processing Job Ads event data.
It simulates a production-style big data architecture where:

Raw events are inserted into Cassandra continuously

Airflow triggers an Incremental PySpark ETL job that:

- Reads only new data since the last run using last_ts.json

- Applies aggregations and business metrics

- Loads analytical tables into MySQL with upsert

Grafana visualizes CTR, CPC, campaign performance, publishers, industries, and patterns by hour/week

This pipeline behaves similarly to real-world ad tracking systems (e.g., Google Ads / Meta Ads logs processing).
## Data Flow Diagram

![img.png](..%2Fimages%2Fimg.png)

## 📁 Project Structure
```commandline
.
├── cql/
│   └── cql_schema.cql          # Cassandra keyspace and table schema
├── mysql/
│   └── mysql_schema.sql        # MySQL database and tables schema
├── dags/
│   ├── etl/
│   │   └── transform.py        # PySpark transformation logic
│   ├── job_ads_etl.py          # Airflow DAG for ETL
│   └── last_ts.json            # Stores last processed timestamp for incremental ETL
├── fake_generator.py           # Script to generate fake job tracking data
├── docker-compose.yml          # Docker services: Cassandra, MySQL, Grafana, Airflow
└── README.md                   # Project documentation

```
## 🚀 Core Features
### 🔹 Incremental batch processing

- Pipeline does not scan all data on every ETL run

- Reads only the new Cassandra rows where ts > last_ts.json

### 🔹 PySpark Transformation Layer

Computes:

- CTR, CPC, CPA, ROI

- Campaign-level metrics

- Publisher performance

- Industry summaries

- Hourly heatmap

- Daily heatmap

### 🔹 Airflow-Orchestrated

- Manual triggering or scheduled runs

- Handles retries, logging, and dependency management

### 🔹 MySQL Analytics Layer

Stores clean aggregated tables for BI systems.

### 🔹 Grafana Dashboards

Visualize KPIs with gauges, bar charts, heatmaps and trends.

## 🐳 Setup Instructions (Full Steps)


### Start All Containers
```commandline
docker-compose up -d --build
```


### 🗄️ Initialize Cassandra Schema
#### Enter Cassandra shell
```commandline
docker exec -it cassandra cqlsh
```

#### Select keyspace
```commandline
USE job_ads;
```

#### Apply schema
```
SOURCE '/docker-entrypoint-initdb.d/cql_schema.cql';
```


### 🧪 Generate Fake Data Into Cassandra
```commandline
python fake_generator.py
```
This script inserts realistic event logs with fields such as impressions, clicks, apply_count, cost, quality_score, device, location, etc.

![img_1.png](..%2Fimages%2Fimg_1.png)

#### tracking table in Cassandra
![img_6.png](images%2Fimg_6.png)

### 🗃️ Initialize MySQL Analytics Schema
#### Enter MySQL
```commandline
docker exec -it mysql mysql -uroot -p123456
```

#### Select DB and load schema
```commandline
USE ads_analytics;
SOURCE /docker-entrypoint-initdb.d/mysql_schema.sql;
```
![img_2.png](..%2Fimages%2Fimg_2.png)

### 🔧 Airflow ETL Pipeline (Incremental)
#### Access Airflow UI

👉 http://localhost:8000

![img_3.png](..%2Fimages%2Fimg_3.png)

#### How the incremental read works

Each ETL run does:

##### 1. Load last_ts.json
```commandline
{
  "last_processed_ts": "2025-11-28T07:21:01"
}
```


##### 2. Query Cassandra:
```commandline
WHERE ts > last_processed_ts
```

##### 3. Process only the new events
##### 4. Upsert summary tables into MySQL
##### 5. Update last_ts.json to the newest timestamp
#### 🔎 Example snippet from job_ads_etl.py

```python
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

```


#### 🔥 PySpark Transformation

Located in: dags/etl/transform.py

```python
def transform_data(df):
    # Basic metrics
    df = df.withColumn("ctr", when(col("impressions") > 0, col("clicks") / col("impressions")).otherwise(0)) \
           .withColumn("cpc", when(col("clicks") > 0, col("cost") / col("clicks")).otherwise(0)) \
           .withColumn("cpa", when(col("apply_count") > 0, col("cost") / col("apply_count")).otherwise(0)) \
           .withColumn("roi", when(col("cost") > 0, col("apply_count") / col("cost")).otherwise(0))

    # Campaign-level summary
    campaign_summary = df.groupBy("campaign_id").agg(
        sf_sum("impressions").alias("total_impressions"),
        sf_sum("clicks").alias("total_clicks"),
        sf_sum("apply_count").alias("total_applies"),
        avg("ctr").alias("avg_ctr"),
        avg("cpc").alias("avg_cpc"),
        avg("cpa").alias("avg_cpa"),
        avg("roi").alias("avg_roi")
    )

    # Publisher-level summary
    publisher_summary = df.groupBy("publisher_id", "publisher_name").agg(
        sf_sum("impressions").alias("total_impressions"),
        sf_sum("clicks").alias("total_clicks"),
        sf_sum("apply_count").alias("total_applies"),
        avg("quality_score").alias("avg_quality_score"),
        avg("ctr").alias("avg_ctr"),
        avg("cpc").alias("avg_cpc"),
        avg("cpa").alias("avg_cpa"),
        avg("roi").alias("avg_roi")
    )

    # Industry summary
    industry_summary = df.groupBy("industry").agg(
        sf_sum("clicks").alias("clicks"),
        sf_sum("apply_count").alias("applications"),
        avg("quality_score").alias("quality")
    )

    # Hourly heatmap
    hourly_stats = df.groupBy("hour").agg(
        sf_sum("impressions").alias("impressions"),
        sf_sum("clicks").alias("clicks"),
        sf_sum("apply_count").alias("applies")
    )

    # Weekday stats
    daily_stats = df.groupBy("day_of_week").agg(
        sf_sum("clicks").alias("clicks"),
        sf_sum("apply_count").alias("applies")
    )

    return {
        "raw": df,
        "campaign_summary": campaign_summary,
        "publisher_summary": publisher_summary,
        "industry_summary": industry_summary,
        "hourly_stats": hourly_stats,
        "daily_stats": daily_stats
    }
```
#### event table in Mysql
![img_5.png](images%2Fimg_5.png)

#### Transforms include:

- CTR = clicks / impressions

- CPC = cost / clicks

- CPA = cost / apply_count

- ROI = apply_count / cost

#### Aggregations for:

- Campaign summaries
![img_7.png](images%2Fimg_7.png)

- Publisher summaries
![img_8.png](images%2Fimg_8.png)

- Industry summaries
![img_9.png](images%2Fimg_9.png)


## 📊 Grafana Dashboards

### Access Dashboard:
👉 http://localhost:3000

Login: admin / admin

##### Add MySQL Datasource

Fill in:

- Field	Value
- Host	mysql:3306
- User	root
- Password	123456
- Database	ads_analytics

![img_4.png](..%2Fimages%2Fimg_4.png)
