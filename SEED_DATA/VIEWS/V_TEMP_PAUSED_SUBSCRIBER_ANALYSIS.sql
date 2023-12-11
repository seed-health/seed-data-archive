create or replace view SEED_DATA.DEV.V_TEMP_PAUSED_SUBSCRIBER_ANALYSIS as 
--- select * from SEED_DATA.DEV.V_TEMP_PAUSED_SUBSCRIBER_ANALYSIS

with pause_rank as
(
select *,
row_number() over(partition by subscription_uuid order by version_started_at) as pause_number,
datediff(day,version_started_at,coalesce(version_ended_at_clean,current_date)) as days_on_pause,
date_trunc(year,subscription_activated_date) as activation_year,
date_trunc(month,subscription_activated_date) as activation_month_year,
floor(datediff(day,subscription_activated_date,version_started_at)/30) as months_since_active,
case when months_since_active >=0 and months_since_active < 4 then '0-3'
when months_since_active >=4 and months_since_active <= 12 then '4-12'
when months_since_active >=13 and months_since_active <= 24 then '13-24'
else '24+' end as months_active_category,
case when activation_year = '2018-01-01' then '2018'
when activation_year = '2019-01-01' then '2019'
when activation_year = '2020-01-01' then '2020'
when activation_year = '2021-01-01' then '2021'
when activation_year = '2022-01-01' then '2022'
when activation_year = '2023-01-01' and activation_month_year = '2023-01-01' then '2023-01'
when activation_year = '2023-01-01' and activation_month_year = '2023-02-01' then '2023-02'
when activation_year = '2023-01-01' and activation_month_year = '2023-03-01' then '2023-03'
when activation_year = '2023-01-01' and activation_month_year = '2023-04-01' then '2023-04'
when activation_year = '2023-01-01' and activation_month_year = '2023-05-01' then '2023-05'
when activation_year = '2023-01-01' and activation_month_year = '2023-06-01' then '2023-06'
when activation_year = '2023-01-01' and activation_month_year = '2023-07-01' then '2023-07'
when activation_year = '2023-01-01' and activation_month_year = '2023-08-01' then '2023-08' end as activation_flag,
case when updated_plan_code ilike '%syn%' then 'DS-01' else 'PDS-08' end as product,
datediff(day,version_started_at,current_date) as days_since_pause,
case when days_since_pause >=0 and days_since_pause <= 10 then 'pause-0-10'
when days_since_pause >=11 and days_since_pause <= 20 then 'pause-11-20'
when days_since_pause >=21 and days_since_pause <= 30 then 'pause-21-30'
when days_since_pause >=31 and days_since_pause <= 40 then 'pause-31-40'
when days_since_pause >=41 and days_since_pause <= 50 then 'pause-41-50'
when days_since_pause >=51 and days_since_pause <= 60 then 'pause-51-60'
when days_since_pause >=60 then 'pause-60+'
end as pause_days_cat

from SEED_DATA.DEV.V_SUBSCRIPTION_PAUSE_HISTORY
where version_started_at >= '2023-04-01' and version_started_At < '2023-09-01'
)

select distinct subscription_uuid, date_trunc(month,version_started_at) as pause_month 
from pause_rank
where pause_number = 1
and version_ended_at_clean is not null
and (version_ended_at_clean != subscription_expires_date or subscription_expires_date is null)
and date_trunc(month,version_started_at) >= '2023-04-01'
and product = 'DS-01'
