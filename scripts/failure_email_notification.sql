SELECT CURRENT_DATABASE();

CREATE OR REPLACE TASK "TASKS"."TASK_MONTHLY_REBILL_ELIGIBLE"
  WAREHOUSE=TASK_EXECUTION
  SCHEDULE='USING CRON 0 9 * * * America/New_York'
AS
BEGIN
  -- Check if it's the last day of the month
  IF DATEADD('DAY', 1, CURRENT_DATE()) = DATE_TRUNC('MONTH', CURRENT_DATE()) THEN
    -- The ETL logic here
    CREATE OR REPLACE TABLE FINANCE.MONTHLY_REBILL_ELIGIBLE_WATERFALL_RESCHEDULE_SNAPSHOT AS
    SELECT
      *,
      CONVERT_TIMEZONE('America/New_York', CURRENT_TIMESTAMP()) AS latest_refresh_time
    FROM PROD_DB.FINANCE.V_REBILL_ELIGIBLE_WATERFALL_RESCHEDULE;

    -- Insert into the TASK_UPDATE_LOG table
    INSERT INTO REFERENCE.TASK_UPDATE_LOG (
      task_name,
      table_or_view_name,
      update_timestamp,
      row_count,
      ref_column,
      latest_reference_date,
      task_next_refresh
    )
    SELECT
      'TASK_SUBSCRIPTION' AS task_name,
      'FINANCE' AS table_or_view_name,
      CURRENT_TIMESTAMP() AS update_timestamp,
      (SELECT COUNT(*) FROM FINANCE.MONTHLY_REBILL_ELIGIBLE_WATERFALL_RESCHEDULE_SNAPSHOT) AS row_count,
      'CREATED_AT' AS REF_COLUMN,
      (SELECT TO_TIMESTAMP_NTZ(MAX(CAST("CREATED_AT" AS TIMESTAMP_NTZ(9)))) AS LATEST_REFERENCE_DATE
       FROM FINANCE.MONTHLY_REBILL_ELIGIBLE_WATERFALL_RESCHEDULE),
      DATEADD('MONTH', 24, CURRENT_TIMESTAMP()) AS task_next_refresh;
  ELSE
    -- Log that today is not the last day of the month
    INSERT INTO YOUR_LOG_TABLE (log_message)
    VALUES ('Today is not the last day of the month for TASK_MONTHLY_REBILL_ELIGIBLE');
  END IF;
END;
