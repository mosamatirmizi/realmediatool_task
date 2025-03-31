
# realmediatool_task

# Data Engineering Task: AdTech Data Pipeline

## Overview

You are tasked with developing a data pipeline for an advertising platform. 

The source data is stored in PostgreSQL (operational database) and needs to be transformed and loaded into ClickHouse (analytical database) for efficient reporting and KPI analysis.

## Task Requirements

Your challenge is to:

1. **Design and implement a ClickHouse schema** optimized for analytical queries
2. **Create a data pipeline** to move data from PostgreSQL to ClickHouse
   - You can use any approach.
   - Your solution should be reproducible and well-documented
3. **Develop queries** to calculate key advertising KPIs
4. **Document your approach** and any assumptions made


# Solution

## Design & Tech Stack

I have tried to keep the design minimal. Applied Separation of Concern principal so each business entitiy i.e. Advertiser, Campaign 
can be mmanaged independently. I avoided unnecessary complexity hence reviiewing will be easy to understand. During the planning my main focus 
was on the key aspects of pipeline design and data model flexibility. 
Please note that it's not a production grade solution, however, I have tried my best to address all the expected requirements till some extent. 

For the tech stack I only added an orchestration layer while designing the solutino around available tools.
1. **Airfow** for orchestration
2. **Postgres** for both RAW and Curation layer. [Note: Usualy we must have separate persistant storage]
3. **Flyway** for schema initiaisation
4. **Clickhouse** for analytical model.

## Assumptions

As the data generation module was pre-provided, I assumed the data fed to the pipeline was accurate. I invested time to understand the source structure
rather then data patterns. 
In order to reproduce the solution, please ensure libraries are properly installed in docker container. I have added the commands in the docker compose file.
I didn't use custom images hene the official image would be enough.

## Out Of Scope

1. Quality Checks between Layers
2. Checkpointing
3. Caching


## Data Models

### Curation Layer

For the curation layer, I have adopted the Data Vault 2.0 (DV2) modeling approach. Given the inevitability of schema evolution, 
DV2 offers the flexibility to accommodate changes without compromising historical data integrity. 
Additionally, it provides robust support for handling late-arriving data, ensuring consistency and completeness in our data warehouse.

Detailed schema information can be found in `migrations/V1__create_schema.sql`.


### Analytical Layer

Since ClickHouse performs optimally with wide, denormalized tables and minimal joins, I have deployed a denormalized entity that 
contains daily aggregates. This design significantly improves the performance of analytical queries by reducing query complexity 
and leveraging ClickHouse’s columnar storage engine for faster data retrieval.
Additionally, it minimizes I/O overhead by storing pre-aggregated metrics.

## Deliverables

1. **ClickHouse Schema**: SQL scripts can be found in ./migrations/clickhouse 
2. **Data Pipeline**: Code can be found in ./dags/
3. **KPI Queries**: SQL queries to calculate the requested metrics can be found in ./KPIs/
   - Click-Through Rate (CTR) by campaign
   - Daily impressions and clicks
   - Few other relevant KPIs
4. **Documentation**: A README explaining your design decisions and how to run your solution


## Evaluation Criteria

- **Data modeling**: Data Vault 2.0 and Denormalised
- **Pipeline architecture**: SoC principal, Utility Functions, Resillient to Multiple Triggers, Unit tests, RnD Notebook
- **Implementation quality**: Proper Logging is implemented as per need.
- **Query performance**: Time series Queries to be executed for the latest Clickhouse server
- **Documentation**: Clear explanation of your approach, design decisions, and trade-offs
- **Innovation**: Creative solutions to the data engineering challenges presented

