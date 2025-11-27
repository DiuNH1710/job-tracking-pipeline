from pyspark.sql.functions import (
    col, when, lit, avg, sum as sf_sum, count as sf_count
)

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
