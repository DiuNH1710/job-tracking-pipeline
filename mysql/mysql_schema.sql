-- ========================================================
-- DATABASE
-- ========================================================
CREATE DATABASE IF NOT EXISTS ads_analytics;
USE ads_analytics;

-- ========================================================
-- RAW TABLE
-- ========================================================

DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS campaign_summary;
DROP TABLE IF EXISTS campaign_summary_stage;
DROP TABLE IF EXISTS publisher_summary;
DROP TABLE IF EXISTS publisher_summary_stage;
DROP TABLE IF EXISTS industry_summary;
DROP TABLE IF EXISTS industry_summary_stage;
DROP TABLE IF EXISTS hourly_stats;
DROP TABLE IF EXISTS hourly_stats_stage;
DROP TABLE IF EXISTS daily_stats;
DROP TABLE IF EXISTS daily_stats_stage;

CREATE TABLE events (
    event_id CHAR(36) PRIMARY KEY,
    ts DATETIME,
    publisher_id INT,
    publisher_name VARCHAR(200),
    publisher_traffic VARCHAR(100),
    publisher_quality VARCHAR(100),
    bid INT,
    job_id INT,
    campaign_id INT,
    industry VARCHAR(100),
    location VARCHAR(100),
    impressions INT,
    clicks INT,
    apply_count INT,
    cost DOUBLE,
    quality_score INT,
    device VARCHAR(50),
    hour INT,
    day_of_week INT,
    ctr DOUBLE,
    cpc DOUBLE,
    cpa DOUBLE,
    roi DOUBLE
);

-- ========================================================
-- CAMPAIGN SUMMARY
-- ========================================================
CREATE TABLE campaign_summary (
    campaign_id INT,
    total_impressions INT,
    total_clicks INT,
    total_applies INT,
    avg_ctr DOUBLE,
    avg_cpc DOUBLE,
    avg_cpa DOUBLE,
    avg_roi DOUBLE,
    PRIMARY KEY (campaign_id)
);

CREATE TABLE campaign_summary_stage LIKE campaign_summary;

-- ========================================================
-- PUBLISHER SUMMARY
-- ========================================================
CREATE TABLE publisher_summary (
    publisher_id INT,
    publisher_name VARCHAR(200),
    total_impressions INT,
    total_clicks INT,
    total_applies INT,
    avg_quality_score DOUBLE,
    avg_ctr DOUBLE,
    avg_cpc DOUBLE,
    avg_cpa DOUBLE,
    avg_roi DOUBLE,
    PRIMARY KEY (publisher_id)
);

CREATE TABLE publisher_summary_stage LIKE publisher_summary;

-- ========================================================
-- INDUSTRY SUMMARY
-- ========================================================
CREATE TABLE industry_summary (
    industry VARCHAR(200),
    clicks INT,
    applications INT,
    quality DOUBLE,
    PRIMARY KEY (industry)
);

CREATE TABLE industry_summary_stage LIKE industry_summary;

-- ========================================================
-- HOURLY STATS
-- ========================================================
CREATE TABLE hourly_stats (
    hour INT,
    impressions INT,
    clicks INT,
    applies INT,
    PRIMARY KEY (hour)
);

CREATE TABLE hourly_stats_stage LIKE hourly_stats;

-- ========================================================
-- DAILY STATS
-- ========================================================
CREATE TABLE daily_stats (
    day_of_week INT,
    clicks INT,
    applies INT,
    PRIMARY KEY (day_of_week)
);

CREATE TABLE daily_stats_stage LIKE daily_stats;
