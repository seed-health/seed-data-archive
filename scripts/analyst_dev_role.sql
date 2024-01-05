-- Create the role
CREATE ROLE analyst_dev;
-- Grant read/write access to PROD_DB and stage_DB
GRANT USAGE, CREATE SCHEMA ON DATABASE PROD_DB TO ROLE analyst_dev;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE PROD_DB TO ROLE analyst_dev;

GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE PROD_DB TO ROLE analyst_dev;
GRANT ALL PRIVILEGES ON ALL VIEWS IN DATABASE PROD_DB TO ROLE analyst_dev;

---------------------------------------------------------------------------
---------------------------------------------------------------------------
-- GRANT ALL PRIVILEGES ON ALL TABLES, SCHEMAS, VIEWS IN SCHEMA STAGE_DB TO ROLE analyst_dev;

GRANT USAGE, CREATE SCHEMA ON DATABASE STAGE_DB TO ROLE analyst_dev;

GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE STAGE_DB TO ROLE analyst_dev;

GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE STAGE_DB TO ROLE analyst_dev;
GRANT ALL PRIVILEGES ON ALL VIEWS IN DATABASE STAGE_DB TO ROLE analyst_dev;

---------------------------------------------------------------------------
---------------------------------------------------------------------------
-- Grant read-only access to MARKETING_DATABASE, SEGMENT_EVENTS
GRANT USAGE ON DATABASE MARKETING_DATABASE TO ROLE analyst_dev;
GRANT SELECT ON ALL TABLES IN DATABASE MARKETING_DATABASE TO ROLE analyst_dev;

---------------------------------------------------------------------------
---------------------------------------------------------------------------
-- Grant import privileges for IO06230_RECURLY_SEED_SHARE, ITERABLE_EVENT_DATA
GRANT IMPORTED PRIVILEGES ON DATABASE IO06230_RECURLY_SEED_SHARE TO ROLE analyst_dev;
GRANT IMPORTED PRIVILEGES ON DATABASE ITERABLE_EVENT_DATA TO ROLE analyst_dev;

---------------------------------------------------------------------------
---------------------------------------------------------------------------

GRANT USAGE ON DATABASE AMPLITUDE_EVENT_DATA TO ROLE analyst_dev;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE AMPLITUDE_EVENT_DATA TO ROLE analyst_dev;

GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE AMPLITUDE_EVENT_DATA TO ROLE analyst_dev;
GRANT ALL PRIVILEGES ON ALL VIEWS IN DATABASE AMPLITUDE_EVENT_DATA TO ROLE analyst_dev;

---------------------------------------------------------------------------
---------------------------------------------------------------------------

GRANT USAGE ON DATABASE SEGMENT_EVENTS TO ROLE analyst_dev;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE SEGMENT_EVENTS TO ROLE analyst_dev;

GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE SEGMENT_EVENTS TO ROLE analyst_dev;
GRANT ALL PRIVILEGES ON ALL VIEWS IN DATABASE SEGMENT_EVENTS TO ROLE analyst_dev;

---------------------------------------------------------------------------
---------------------------------------------------------------------------
-- Grant usage of warehouse_execution privileges
GRANT USAGE ON WAREHOUSE Query_execution TO ROLE analyst_dev;
---------------------------------------------------------------------------
---------------------------------------------------------------------------
-- Revoke drop table privilege
REVOKE DROP TABLE ON DATABASE PROD_DB FROM ROLE analyst_dev;
-- REVOKE DROP TABLE ON DATABASE STAGE_DB FROM ROLE analyst_dev;
REVOKE DROP TABLE ON DATABASE MARKETING_DATABASE FROM ROLE analyst_dev;
REVOKE DROP TABLE ON DATABASE IO06230_RECURLY_SEED_SHARE FROM ROLE analyst_dev;
REVOKE DROP TABLE ON DATABASE AMPLITUDE_EVENT_DATA FROM ROLE analyst_dev;
REVOKE DROP TABLE ON DATABASE SEGMENT_EVENTS FROM ROLE analyst_dev;

-- Set default database and warehouse
-- ALTER USER analyst_dev SET DEFAULT_ROLE = analyst_dev;
-- ALTER USER analyst_dev SET DEFAULT_WAREHOUSE = Query_execution;

-- Assume you have already created the role 'analyst_dev' and user 'your_username'

-- Grant the role to the user
GRANT ROLE analyst_dev TO USER SALMA;
