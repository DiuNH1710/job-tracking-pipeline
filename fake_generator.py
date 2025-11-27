#!/usr/bin/env python3
"""
fake_generator_job_ads.py
Fake event generator that writes to job_ads.tracking (Cassandra).
Run: python fake_generator_job_ads.py
"""

import random
import uuid
import time
from datetime import datetime, timezone
from cassandra.cluster import Cluster

# ---------------------------
# Publishers (10)
# ---------------------------
PUBLISHERS = {
    1: {"name": "Facebook Ads", "bid_range": (1,5),  "traffic": "HIGH", "quality": "LOW"},
    2: {"name": "Google Ads",   "bid_range": (10,30),"traffic": "HIGH", "quality": "HIGH"},
    3: {"name": "LinkedIn",     "bid_range": (20,50),"traffic": "LOW",  "quality": "HIGH"},
    4: {"name": "TikTok Ads",   "bid_range": (1,3),  "traffic": "HIGH", "quality": "LOW"},
    5: {"name": "TopCV",        "bid_range": (8,20), "traffic": "MED",  "quality": "MED"},
    6: {"name": "VietnamWorks", "bid_range": (15,35),"traffic": "MED",  "quality": "HIGH"},
    7: {"name": "CareerBuilder","bid_range": (10,25),"traffic": "LOW",  "quality": "MED"},
    8: {"name": "Indeed",       "bid_range": (5,15), "traffic": "MED",  "quality": "HIGH"},
    9: {"name": "Admicro",      "bid_range": (1,4),  "traffic": "HIGH", "quality": "LOW"},
    10:{"name": "Zalo Ads",     "bid_range": (3,8),  "traffic": "MED",  "quality": "MED"},
}

# ---------------------------
# Jobs (10)
# ---------------------------
JOBS = [
    {"job_id": 1,  "campaign_id": 101, "industry": "IT",         "location": "HCM"},
    {"job_id": 2,  "campaign_id": 102, "industry": "Finance",    "location": "HN"},
    {"job_id": 3,  "campaign_id": 103, "industry": "Marketing",  "location": "HCM"},
    {"job_id": 4,  "campaign_id": 104, "industry": "HR",         "location": "DN"},
    {"job_id": 5,  "campaign_id": 105, "industry": "Sales",      "location": "HCM"},
    {"job_id": 6,  "campaign_id": 106, "industry": "Logistics",  "location": "HP"},
    {"job_id": 7,  "campaign_id": 107, "industry": "Data",       "location": "HN"},
    {"job_id": 8,  "campaign_id": 108, "industry": "Education",  "location": "HCM"},
    {"job_id": 9,  "campaign_id": 109, "industry": "Healthcare", "location": "DN"},
    {"job_id": 10, "campaign_id": 110, "industry": "Banking",    "location": "HN"},
]

QUALITY_MAP = {"LOW": 30, "MED": 60, "HIGH": 85}  # base quality score


# ---------------------------
# Helpers
# ---------------------------
def sample_impressions(pub):
    if pub["traffic"] == "HIGH":
        return random.randint(500, 4000)
    if pub["traffic"] == "MED":
        return random.randint(200, 1200)
    return random.randint(50, 400)

def sample_clicks(impressions):
    rate = random.uniform(0.01, 0.08)
    return int(impressions * rate)

def sample_applies(clicks):
    rate = random.uniform(0.03, 0.25)
    return int(clicks * rate)

def sample_bid(pub):
    lo, hi = pub["bid_range"]
    return random.randint(lo, hi)

def compute_cost(clicks, bid):
    return float(clicks * bid)

def sample_quality_score(pub):
    base = QUALITY_MAP.get(pub["quality"], 50)
    return max(0, min(100, int(random.gauss(base, 8))))

def sample_device():
    return random.choices(["mobile","desktop"], weights=[0.75,0.25])[0]


# ---------------------------
# Cassandra connect
# ---------------------------
def connect_cassandra(hosts=['127.0.0.1'], keyspace='job_ads'):
    cluster = Cluster(hosts)
    session = cluster.connect()
    session.set_keyspace(keyspace)
    return cluster, session


# ---------------------------
# Generate event
# ---------------------------
def generate_event(pub_id, job):
    pub = PUBLISHERS[pub_id]

    impressions = sample_impressions(pub)
    clicks = sample_clicks(impressions)
    applies = sample_applies(clicks)
    bid = sample_bid(pub)
    cost = compute_cost(clicks, bid)
    quality_score = sample_quality_score(pub)

    now = datetime.utcnow().replace(tzinfo=timezone.utc)

    return {
        "event_id": uuid.uuid4(),
        "ts": now,

        # Publisher info
        "publisher_id": pub_id,
        "publisher_name": pub["name"],
        "publisher_traffic": pub["traffic"],
        "publisher_quality": pub["quality"],

        # Job info
        "job_id": job["job_id"],
        "campaign_id": job["campaign_id"],
        "industry": job["industry"],
        "location": job["location"],

        # Metrics
        "impressions": impressions,
        "clicks": clicks,
        "apply_count": applies,
        "bid": bid,
        "cost": cost,
        "quality_score": quality_score,
        "device": sample_device(),
        "hour": now.hour,
        "day_of_week": now.isoweekday()
    }


# ---------------------------
# Main
# ---------------------------
def main():
    cluster, session = connect_cassandra()

    insert_cql = session.prepare("""
        INSERT INTO tracking (
            event_id, ts,
            publisher_id, publisher_name, publisher_traffic, publisher_quality,
            job_id, campaign_id, industry, location,
            impressions, clicks, apply_count,
            bid, cost, quality_score,
            device, hour, day_of_week
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    print("Starting fake generator... Ctrl+C to stop")

    try:
        while True:
            pub_id = random.choice(list(PUBLISHERS.keys()))
            job = random.choice(JOBS)
            ev = generate_event(pub_id, job)

            params = (
                ev["event_id"], ev["ts"],
                ev["publisher_id"], ev["publisher_name"], ev["publisher_traffic"], ev["publisher_quality"],
                ev["job_id"], ev["campaign_id"], ev["industry"], ev["location"],
                ev["impressions"], ev["clicks"], ev["apply_count"],
                ev["bid"], ev["cost"], ev["quality_score"],
                ev["device"], ev["hour"], ev["day_of_week"],
            )

            session.execute(insert_cql, params)

            print(f"[{ev['ts'].isoformat()}] pub={ev['publisher_name']} job={ev['job_id']} imp={ev['impressions']} clicks={ev['clicks']} cost={ev['cost']:.2f}")

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
