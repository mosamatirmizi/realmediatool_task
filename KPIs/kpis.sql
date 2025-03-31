-- Engine: Clickhouse
-- Note: You need to change the dates.


-- 1. Click-Through Rate (CTR) by campaign
SELECT
    date,
    campaign_id,
    campaign_name,
    sum(impressions) AS total_impressions,
    sum(clicks) AS total_clicks,
    if(sum(impressions) > 0, sum(clicks) / sum(impressions), 0) AS overall_ctr,
    advertiser_name
FROM rmt_db.daily_campaign_performance
WHERE date BETWEEN '2025-03-23' AND '2025-03-24'
GROUP BY date,campaign_id, campaign_name, advertiser_name
ORDER BY overall_ctr DESC;

-- 2. Daily impressions and clicks
SELECT
    date,
    sum(impressions) AS daily_impressions,
    sum(clicks) AS daily_clicks,
    if(sum(impressions) > 0, sum(clicks) / sum(impressions), 0) AS daily_ctr
FROM rmt_db.daily_campaign_performance
WHERE date BETWEEN '2025-03-23' AND '2025-03-24'
GROUP BY date
ORDER BY date;

-- 3. Campaign performance by advertiser
SELECT
    advertiser_id,
    advertiser_name,
    sum(impressions) AS total_impressions,
    sum(clicks) AS total_clicks,
    if(sum(impressions) > 0, sum(clicks) / sum(impressions), 0) AS overall_ctr,
    count(DISTINCT campaign_id) AS campaign_count
FROM rmt_db.daily_campaign_performance
WHERE date BETWEEN '2025-03-23' AND '2025-03-24'
GROUP BY advertiser_id, advertiser_name
ORDER BY total_impressions DESC;

-- 4. Campaign budget utilization 
-- (assuming we have cost data, which could be derived from clicks * bid_amount)
SELECT
    campaign_id,
    campaign_name,
    sum(clicks * bid_amount) AS estimated_spend,
    max(budget) AS campaign_budget,
    sum(clicks * bid_amount) / max(budget) AS budget_utilization_rate
FROM rmt_db.daily_campaign_performance
WHERE date BETWEEN '2025-03-23' AND '2025-03-24'
GROUP BY campaign_id, campaign_name
ORDER BY budget_utilization_rate DESC;