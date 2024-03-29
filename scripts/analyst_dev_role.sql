/*-- analyst_dev Role:

-- Purpose: The analyst_dev role seems to be designed for analysts or developers who need various privileges related to reading, writing, and managing data in specific databases and schemas.
-- Privileges:
-- Usage and creation of schemas in PROD_DB and STAGE_DB.
-- All privileges on all schemas, tables, and views in PROD_DB and STAGE_DB.
-- Read-only access to MARKETING_DATABASE.
-- Import privileges for specific databases (IO06230_RECURLY_SEED_SHARE and ITERABLE_EVENT_DATA).
-- Usage and all privileges on all schemas, tables, and views in AMPLITUDE_EVENT_DATA and SEGMENT_EVENTS.
-- Usage of the Query_execution warehouse.*/

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
GRANT USAGE ON DATABASE MARKETING_DATABASE TO ROLE BUSINESS;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE MARKETING_DATABASE TO ROLE BUSINESS;

GRANT SELECT ON ALL TABLES IN DATABASE MARKETING_DATABASE TO ROLE BUSINESS;


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

-- Set default database and warehouse
-- ALTER USER analyst_dev SET DEFAULT_ROLE = analyst_dev;
-- ALTER USER analyst_dev SET DEFAULT_WAREHOUSE = Query_execution;

-- Assume you have already created the role 'analyst_dev' and user 'your_username'

-- Grant the role to the user
GRANT ROLE BUSINESS TO USER DEJUAN;
-- ALTER USER ROLE analyst_dev SET DEFAULT_WAREHOUSE = QUERY_EXECUTION;

-- Grant view task privilege on the account
-- REVOKE MONITOR EXECUTION ON ACCOUNT FROM ROLE analyst_dev;

GRANT ROLE ANALYST_DEV TO ROLE TASKADMIN;

DROP ROLE DEVELOPER;

-- -----------------------------------------
-- -----------------------------------------
-- -- only task owner can run the task!!!
-- GRANT USAGE ON WAREHOUSE TASK_EXECUTION TO ROLE TASKADMIN;

-- GRANT USAGE ON DATABASE STAGE_DB TO ROLE TASKADMIN;
-- GRANT CREATE SCHEMA ON DATABASE STAGE_DB TO ROLE TASKADMIN;
-- GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE STAGE_DB TO ROLE TASKADMIN;


-- GRANT USAGE ON DATABASE PROD_DB TO ROLE TASKADMIN;
-- GRANT CREATE SCHEMA ON DATABASE PROD_DB TO ROLE TASKADMIN;
-- GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE PROD_DB TO ROLE TASKADMIN;

-- GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE TASKADMIN;
-- GRANT USAGE ON SCHEMA "TASKS" TO ROLE TASKADMIN;
-- -- Grant USAGE on schema to TASKADMIN
-- DROP ROLE TASKADMIN;



SHOW SCHEMAS

GRANT ROLE ANALYST_DEV TO USER CHARLESPICH;

GRANT ROLE ANALYST_DEV TO USER CHRISMCMULLEN;


GRANT ROLE ANALYST_DEV TO USER PARAMVIR;

GRANT ROLE ANALYST_DEV TO USER PRITHEESH;

GRANT ROLE ANALYST_DEV TO USER SURAJ;

--- make default warehouse query_execution 
ALTER USER CHARLESPICH SET DEFAULT_WAREHOUSE = QUERY_EXECUTION;

ALTER USER CHRISMCMULLEN SET DEFAULT_WAREHOUSE = QUERY_EXECUTION;


ALTER USER PARAMVIR SET DEFAULT_WAREHOUSE = QUERY_EXECUTION;

ALTER USER PRITHEESH SET DEFAULT_WAREHOUSE = QUERY_EXECUTION;

ALTER USER SURAJ SET DEFAULT_WAREHOUSE = QUERY_EXECUTION;