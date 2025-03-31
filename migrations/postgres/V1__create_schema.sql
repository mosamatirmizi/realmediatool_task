CREATE TABLE IF NOT EXISTS advertiser (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS campaign (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    bid NUMERIC(10,2) NOT NULL,
    budget NUMERIC(10,2) NOT NULL,
    start_date DATE,
    end_date DATE,
    advertiser_id INTEGER REFERENCES advertiser(id),
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS impressions (
    id SERIAL PRIMARY KEY,
    campaign_id INTEGER REFERENCES campaign(id),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS clicks (
    id SERIAL PRIMARY KEY,
    campaign_id INTEGER REFERENCES campaign(id),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE SCHEMA IF NOT EXISTS curated;


CREATE TABLE IF NOT EXISTS curated.hub_advertiser (
    advertiser_id INT PRIMARY KEY,
    load_date TIMESTAMP DEFAULT NOW(),
    record_source VARCHAR(100)
);


CREATE TABLE IF NOT EXISTS curated.hub_campaign (
    campaign_id INT PRIMARY KEY,
    load_date TIMESTAMP DEFAULT NOW(),
    record_source VARCHAR(100)
);



CREATE TABLE IF NOT EXISTS curated.link_advertiser_campaign (
    advertiser_id INT REFERENCES curated.hub_advertiser(advertiser_id),
    campaign_id INT REFERENCES curated.hub_campaign(campaign_id),
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    load_date TIMESTAMP DEFAULT NOW(),
    record_source VARCHAR(100),
    PRIMARY KEY (advertiser_id, campaign_id)
);



CREATE TABLE IF NOT EXISTS curated.sat_advertiser (
    advertiser_id INT REFERENCES curated.hub_advertiser(advertiser_id),
    advertiser_name VARCHAR(255),
    created_at TIMESTAMP WITHOUT TIME ZONE,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    load_date TIMESTAMP DEFAULT NOW(),
    record_source VARCHAR(100),
    PRIMARY KEY (advertiser_id, load_date)
);


CREATE TABLE IF NOT EXISTS curated.sat_campaign (
    campaign_id INT REFERENCES curated.hub_campaign(campaign_id),
    advertiser_id INT REFERENCES curated.hub_advertiser(advertiser_id),
    campaign_name VARCHAR(255),
    budget NUMERIC(15,2),
    bid_amount NUMERIC(15,2),
    start_date DATE,
    end_date DATE,
    created_at TIMESTAMP WITHOUT TIME ZONE,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    load_date TIMESTAMP DEFAULT NOW(),
    record_source VARCHAR(100),
    PRIMARY KEY (campaign_id,advertiser_id,load_date)
);


CREATE TABLE IF NOT EXISTS curated.sat_impressions (
    impression_id SERIAL PRIMARY KEY,
    campaign_id INT REFERENCES curated.hub_campaign(campaign_id),
    impression_timestamp TIMESTAMP WITHOUT TIME ZONE,
    load_date TIMESTAMP DEFAULT NOW(),
    record_source VARCHAR(100)
);


CREATE TABLE IF NOT EXISTS curated.sat_clicks (
    click_id SERIAL PRIMARY KEY,
    campaign_id INT REFERENCES curated.hub_campaign(campaign_id),
    click_timestamp TIMESTAMP WITHOUT TIME ZONE,
    load_date TIMESTAMP DEFAULT NOW(),
    record_source VARCHAR(100)
);
