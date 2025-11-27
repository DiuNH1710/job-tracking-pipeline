CREATE KEYSPACE IF NOT EXISTS job_ads
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE job_ads.tracking (
    event_id uuid PRIMARY KEY,
    ts timestamp,
    publisher_id int,
    publisher_name text,
    publisher_traffic text,
    publisher_quality text,
    bid int,
    job_id int,
    campaign_id int,
    industry text,
    location text,
    impressions int,
    clicks int,
    apply_count int,
    cost double,
    quality_score int,
    device text,
    hour int,
    day_of_week int
);
