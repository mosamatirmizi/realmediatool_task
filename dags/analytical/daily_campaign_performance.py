from utils.db_utils import write_ch_df , execute_query
import logging


def __get_daily_aggregate(engine):
    query = f"""
        SELECT
            DATE(si.impression_timestamp) AS date,
            hc.campaign_id,
            sc.campaign_name,
            sc.advertiser_id,
            sa.advertiser_name,
            COUNT(DISTINCT si.impression_id) AS impressions,
            COUNT(DISTINCT scl.click_id) AS clicks,
            CASE 
                WHEN COUNT(DISTINCT si.impression_id) > 0 
                THEN COUNT(DISTINCT scl.click_id) * 1.0 / COUNT(DISTINCT si.impression_id) 
                ELSE 0 
            END AS ctr,
            sc.budget,
            sc.bid_amount,
            sc.start_date,
            sc.end_date,
            (
                DATE(sc.start_date) <= DATE(si.impression_timestamp)
                AND (
                    sc.end_date IS NULL OR DATE(sc.end_date) >= DATE(si.impression_timestamp)
                )
            ) AS is_active
        FROM curated.sat_impressions si
        LEFT JOIN curated.hub_campaign hc ON si.campaign_id = hc.campaign_id
        LEFT JOIN curated.sat_campaign sc ON hc.campaign_id = sc.campaign_id
        LEFT JOIN curated.hub_advertiser ha ON sc.advertiser_id = ha.advertiser_id
        LEFT JOIN curated.sat_advertiser sa ON ha.advertiser_id = sa.advertiser_id
        LEFT JOIN curated.sat_clicks scl 
            ON hc.campaign_id = scl.campaign_id 
            AND DATE(scl.click_timestamp) = DATE(si.impression_timestamp)
        GROUP BY
            DATE(si.impression_timestamp),
            hc.campaign_id,
            sc.campaign_name,
            sc.advertiser_id,
            sa.advertiser_name,
            sc.budget,
            sc.bid_amount,
            sc.start_date,
            sc.end_date;
    """
    logging.info("Extracting daily aggregates.")
    return execute_query(engine, query)


def populate_ch_daily_agg(engine):
    try:
        agg_data_df= __get_daily_aggregate(engine)
        if agg_data_df.empty:
            logging.warning("No advertiser data extracted.")
            return
        write_ch_df(df=agg_data_df , table_name='daily_campaign_performance',db_name='rmt_db')
        
    except Exception as e:
        logging.error(f"Failed to connect/writing to ClickHouse: {e}")
    raise

