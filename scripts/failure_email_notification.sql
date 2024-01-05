select current_database();
-- Create or replace the task
CREATE OR REPLACE TASK "TASKS".TASK_CANCELLATION_TRANSACTION_HISTORY_test
  WAREHOUSE = TASK_EXECUTION
  SCHEDULE = 'USING CRON 0 7 * * * America/New_York'
AS
      -- Execute your main task here
  CREATE OR REPLACE TABLE "RETENTION".CANCELLATION_TRANSACTION_HISTORY AS 
    SELECT *, CONVERT_TIMEZONE('America/New_York', CURRENT_TIMESTAMP()) AS latest_refresh_time  
    FROM "RETENTION"."V_CANCELLATION_TRANSACTION_HISTORY_2";
  DECLARE
    taskName STRING;
    taskState STRING;
    taskErrorMessage STRING;
    emailBody STRING;
    destintionTable STRING;
  BEGIN
    -- Fetch the data
    SELECT name, state, error_message 
    INTO taskName, taskState, taskErrorMessage
    FROM "SNOWFLAKE"."ACCOUNT_USAGE"."TASK_HISTORY"
    WHERE database_name = current_database()
      AND state = 'FAILED'
    ORDER BY completed_time DESC 
    LIMIT 1;

      -- Email notification body with HTML formatting
    destintionTable := '"RETENTION"."V_CANCELLATION_TRANSACTION_HISTORY_2"';
    emailBody := CONCAT('<p><strong>Task failed! Please check the logs for more details.</strong></p>',
                    '<p><strong>Tasl name:</strong> ', :taskName, '</p>',
                    '<p><strong>Error:</strong> ', :taskErrorMessage,'</p>',
                    '<p><strong>Tables affected:</strong> ', :destintionTable,'</p>');

    CALL SYSTEM$SEND_EMAIL(
      'email_integration',
      'salma@seed.com',
      'Failure Notification - ' || :taskName,
      :emailBody,
      'text/html'
  );

  -- Insert into the TASK_UPDATE_LOG table
  INSERT INTO REFERENCE.TASK_UPDATE_LOG (
    task_name, 
    table_or_view_name, 
    update_timestamp, 
    row_count, 
    ref_column,
    latest_reference_date,
    task_next_refresh)
  SELECT 
    'TASK_CANCELLATION_TRANSACTION_HISTORY_test' AS task_name,
    'CANCELLATION_TRANSACTION_HISTORY_2' AS table_or_view_name,
    CURRENT_TIMESTAMP() AS update_timestamp,
    (SELECT COUNT(*) FROM "RETENTION".CANCELLATION_TRANSACTION_HISTORY) AS row_count,
    'SUB_CANCELED_AT' AS REF_COLUMN, 
    (SELECT
      TO_TIMESTAMP_NTZ(MAX(CAST(SUBSCRIPTION_CANCELED_AT AS TIMESTAMP_NTZ(9)))) AS LATEST_REFERENCE_DATE
    FROM
      "RETENTION".CANCELLATION_TRANSACTION_HISTORY),
    DATEADD('HOUR', 24, CURRENT_TIMESTAMP()) AS task_next_refresh;


END;

execute task "TASKS".TASK_CANCELLATION_TRANSACTION_HISTORY_test