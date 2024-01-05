-- Assuming the task is inside a specific schema, modify as needed
CREATE OR REPLACE TASK "TASKS".TASK_MONTHLY_REBILL_ELIGIBLE
    WAREHOUSE=TASK_EXECUTION
    SCHEDULE='USING CRON 0 9 * * * America/New_York'
AS
DECLARE 
    todays_date DATE;
    last_day_of_month DATE;
    taskName STRING;
    taskState STRING;
    taskErrorMessage STRING;
    emailBody STRING;
    destintionTable STRING;
    log_message STRING;
    condition_met BOOLEAN;
BEGIN
    WITH date_info AS (
        SELECT
            DATEADD('DAY', -1, DATEADD('MONTH', 1, DATE_TRUNC('MONTH', CURRENT_DATE()))) AS sys_last_day_of_month,
            CURRENT_DATE() AS sys_todays_date,
            'Today is not the last day of the month for TASK_MONTHLY_REBILL_ELIGIBLE' AS sys_log_message,
            true AS sys_condition_met
    )

    -- Assign values directly to variables
    SELECT sys_last_day_of_month, sys_todays_date, sys_log_message, sys_condition_met
    INTO :last_day_of_month, :todays_date, :log_message, :condition_met
    FROM date_info;

    -- Set the condition_met variable based on the comparison
    condition_met := :last_day_of_month = :todays_date;

    CASE
        WHEN :condition_met THEN 
            -- Your existing logic for when the condition is met
            CREATE OR REPLACE TABLE FINANCE.MONTHLY_REBILL_ELIGIBLE_WATERFALL_RESCHEDULE_SNAPSHOT AS
                SELECT
                    *,
                    CONVERT_TIMEZONE('America/New_York', CURRENT_TIMESTAMP()) AS latest_refresh_time
                FROM FINANCE.V_REBILL_ELIGIBLE_WATERFALL_RESCHEDULE;

            -- Fetch the task data execution related 
            SELECT name, state, error_message 
            INTO taskName, taskState, taskErrorMessage
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."TASK_HISTORY"
            WHERE database_name = current_database()
            AND state = 'FAILED'
            ORDER BY completed_time DESC 
            LIMIT 1;

            -- Email notification body with HTML formatting
            destintionTable := '"FINANCE"."MONTHLY_REBILL_ELIGIBLE_WATERFALL_RESCHEDULE_SNAPSHOT"';
            emailBody := CONCAT('<p>Task failed! Please check the logs for more details.</strong></p>',
                            '<p><strong>Task name:</strong> ', :taskName, '</p>',
                            '<p><strong>Tables affected:</strong> ', :destintionTable,'</p>');

            CALL SYSTEM$SEND_EMAIL(
                'email_integration',
                'salma@seed.com, chris.mcmullen@seed.com',
                'FAILED Notification - ' || :taskName,
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
                task_next_refresh
            )
            SELECT
                'MONTHLY_REBILL_ELIGIBLE_WATERFALL_RESCHEDULE_SNAPSHOT' AS task_name,
                'FINANCE' AS table_or_view_name,
                CURRENT_TIMESTAMP() AS update_timestamp,
                (SELECT COUNT(*) FROM FINANCE.MONTHLY_REBILL_ELIGIBLE_WATERFALL_RESCHEDULE_SNAPSHOT) AS row_count,
                'CREATED_AT' AS REF_COLUMN,
                TO_TIMESTAMP_NTZ(MAX(CAST("CREATED_AT" AS TIMESTAMP_NTZ(9)))) AS LATEST_REFERENCE_DATE,
                :last_day_of_month AS task_next_refresh;
        ELSE
            -- Your existing logic for when the condition is not met
            -- Fetch the task data execution related 
            SELECT name, state, error_message 
            INTO taskName, taskState, taskErrorMessage
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."TASK_HISTORY"
            WHERE database_name = current_database()
            ORDER BY completed_time DESC 
            LIMIT 1;

            -- Email notification body with HTML formatting
            emailBody := CONCAT('<p><strong>Task not supposed to run today!</strong></p>',
                            '<p><strong>TasK name:</strong> ', :taskName, '</p>',
                            '<p><strong>Message:</strong> ', :log_message,'</p>');

            CALL SYSTEM$SEND_EMAIL(
                'email_integration',
                'salma@seed.com, chris.mcmullen@seed.com',
                'Out of schedule Notification - ' || :taskName,
                :emailBody,
                'text/html'
            );

            -- Insert additional logic or log messages as needed
    END CASE;
END;
