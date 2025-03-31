CREATE TABLE rmt_db.daily_campaign_performance
(
    `date` Date,
    `campaign_id` Int32,
    `campaign_name` String,
    `advertiser_id` Int32,
    `advertiser_name` String,
    `impressions` UInt32,
    `clicks` UInt32,
    `ctr` Float64,
    `budget` Decimal(15, 2),
    `bid_amount` Decimal(15, 2),
    `start_date` Date,
    `end_date` Date,
    `is_active` UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, campaign_id, advertiser_id)
SETTINGS index_granularity = 8192;
