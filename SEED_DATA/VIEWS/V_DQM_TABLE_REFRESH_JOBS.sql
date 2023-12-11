create or replace view SEED_DATA.DEV.V_DQM_TABLE_REFRESH_JOBS as 

with dqm_build as (
select 
'SEED_DATA.DEV.ORDER_HISTORY' as source,
'SEED_DATA_TASK_ORDER_HISTORY' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 7, 00, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.ORDER_HISTORY

union all

select 
'SEED_DATA.DEV.SUBSCRIPTION_MASTER' as source,
'SEED_DATA_TASK_SUBSCRIPTION_MASTER' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 7, 15, 00, 0, 'America/New_York')) ) as expected_time
from
 SEED_DATA.DEV.SUBSCRIPTION_MASTER

union all

select 
'SEED_DATA.DEV.SEED_ACQUISITION_ALL' as source,
'SEED_DATA_TASK_SEED_ACQUISITION_ALL' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 7, 30, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.SEED_ACQUISITION_ALL

union all

select 
'SEED_DATA.DEV.SEED_ACQUISITION_VERT' as source,
'SEED_DATA_TASK_SEED_ACQUISITION_VERT' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 7, 45, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.SEED_ACQUISITION_VERT

union all

select 
'SEED_DATA.DEV.CANCELLATION_TRANSACTION_HISTORY' as source,
'SEED_DATA_TASK_CANCELLATION_TRANSACTION_HISTORY' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 7, 00, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.CANCELLATION_TRANSACTION_HISTORY

union all

select 
'SEED_DATA.DEV.EVENTS_ORDER_COMPLETE' as source,
'SEED_DATA_TASK_EVENTS_ORDER_COMPLETE' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 6, 00, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.EVENTS_ORDER_COMPLETE

union all

select 
'SEED_DATA.DEV.SUBSCRIPTION_PAUSE_HISTORY' as source,
'SEED_DATA_TASK_PAUSE_HISTORY' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 6, 00, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.SUBSCRIPTION_PAUSE_HISTORY

union all

select 
'SEED_DATA.DEV.SHIPMENT_HISTORY' as source,
'SEED_DATA_TASK_SHIPMENT_HISTORY' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 6, 00, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.SHIPMENT_HISTORY

union all

select 
'SEED_DATA.DEV.SUBSCRIPTION' as source,
'SEED_DATA_TASK_SUBSCRIPTION' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 6, 00, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.SUBSCRIPTION

union all

select 
'SEED_DATA.DEV.SUBSCRIPTION_STATUS_HISTORY' as source,
'SEED_DATA_TASKSUBSCRIPTION_STATUS_HISTORY' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 6, 00, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.SUBSCRIPTION_STATUS_HISTORY

union all

select 
'SEED_DATA.DEV.SUBSCRIPTION_NEXT_BILL_DATE' as source,
'SEED_DATA_TASK_NEXT_BILL_DATE' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 6, 00, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.SUBSCRIPTION_NEXT_BILL_DATE

union all

select 
'SEED_DATA.DEV.REHCARGE_TRANSACTION_HISTORY' as source,
'SEED_DATA_TASK_REHCARGE_TRANSACTION_HISTORY' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 6, 00, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.REHCARGE_TRANSACTION_HISTORY

/*union all

select 
'SEED_DATA.DEV.RECURLY_TRANSACTION_HISTORY' as source,
'SEED_DATA_TASK_RECURLY_TRANSACTION_HISTORY' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 6, 15, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.RECURLY_TRANSACTION_HISTORY*/

union all

select 
'SEED_DATA.DEV.SUBSCRIPTION_TRANSACTION_HISTORY' as source,
'SEED_DATA_TASK_SUBSCRIPTION_TRANSACTION_HISTORY' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 6, 30, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.SUBSCRIPTION_TRANSACTION_HISTORY

union all

select 
'SEED_DATA.DEV.SUBSCRIPTION_ACTIVATION_LTA' as source,
'SEED_DATA_TASK_SUBSCRIPTION_ACTIVATION_LTA' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 8, 00, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.SUBSCRIPTION_ACTIVATION_LTA

union all

select 
'SEED_DATA.DEV.FINANCIAL_MODEL' as source,
'SEED_DATA_TASK_FINANCIAL_MODEL' as task_name,
count(*) as row_count,
max(latest_refresh_time) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 7, 30, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.FINANCIAL_MODEL

union all

select 
'SEED_DATA.DEV.REBILL_ELIGIBLE_SNAPSHOT' as source,
'SEED_DATA_TASK_REBILL_ELIGIBLE_SNAPSHOT' as task_name,
count(*) as row_count,
max(snapshot_date) as latest_refresh_time,
max( (timestamp_tz_from_parts(year(current_date), month(current_date), day(current_date), 3, 00, 00, 0, 'America/New_York')) ) as expected_time
from
SEED_DATA.DEV.REBILL_ELIGIBLE_SNAPSHOT


)
select
source,
task_name,
row_count,
latest_refresh_time,
CONVERT_TIMEZONE('America/New_York',TO_TIMESTAMP_TZ(expected_time)) as expected_time,
datediff('sec', expected_time, latest_refresh_time) as run_time
from dqm_build
order by row_count desc


