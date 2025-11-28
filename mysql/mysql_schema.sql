-- ========================================================
-- DATABASE
-- ========================================================
CREATE DATABASE IF NOT EXISTS ads_analytics;
USE ads_analytics;

-- ========================================================
-- DROP OLD TABLES
-- ========================================================
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS campaign_summary;
DROP TABLE IF EXISTS publisher_summary;
DROP TABLE IF EXISTS industry_summary;
DROP TABLE IF EXISTS hourly_stats;
DROP TABLE IF EXISTS daily_stats;



-- ========================================================
-- RAW EVENTS TABLE (NO PRIMARY KEY!)
-- ========================================================
CREATE TABLE events (
    event_id CHAR(36),
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
-- FINAL TABLES (WITH PRIMARY KEY)
-- ========================================================

-- CAMPAIGN SUMMARY
CREATE TABLE campaign_summary (
    campaign_id INT PRIMARY KEY,
    total_impressions INT,
    total_clicks INT,
    total_applies INT,
    avg_ctr DOUBLE,
    avg_cpc DOUBLE,
    avg_cpa DOUBLE,
    avg_roi DOUBLE
);

-- PUBLISHER SUMMARY
CREATE TABLE publisher_summary (
    publisher_id INT PRIMARY KEY,
    publisher_name VARCHAR(200),
    total_impressions INT,
    total_clicks INT,
    total_applies INT,
    avg_quality_score DOUBLE,
    avg_ctr DOUBLE,
    avg_cpc DOUBLE,
    avg_cpa DOUBLE,
    avg_roi DOUBLE
);

-- INDUSTRY SUMMARY
CREATE TABLE industry_summary (
    industry VARCHAR(200) PRIMARY KEY,
    clicks INT,
    applications INT,
    quality DOUBLE
);

-- HOURLY
CREATE TABLE hourly_stats (
    hour INT PRIMARY KEY,
    impressions INT,
    clicks INT,
    applies INT
);

-- DAILY
CREATE TABLE daily_stats (
    day_of_week INT PRIMARY KEY,
    clicks INT,
    applies INT
);

