create or replace view SEED_DATA.DEV.V_SUBSCRIPTION_ACTIVATION_LTA(
	ACTIVATED_DATE,
	ACTIVATED_DAY_OF_WEEK,
	ACTIVATED_MONTH_DATE,
	ACTIVATED_WEEK_DATE,
	CHANNEL_GROUPING,
	CHANNEL_PLATFORM,
	TOTAL_SUB_QTY,
    TOTAL_SUB_IDS,
	NEW_SUB_QTY,
    NEW_SUB_IDS,
	REACTIVATION_SUB_QTY,
    REACTIVATION_SUB_IDS,
	DS01_TOTAL_SUB_QTY,
    DS01_TOTAL_SUB_IDS,
	DS01_NEW_SUB_QTY,
    DS01_NEW_SUB_IDS,
	DS01_REACTIVATION_SUB_QTY,
    DS01_REACTIVATION_SUB_IDS,
	PDS08_TOTAL_SUB_QTY,
    PDS08_TOTAL_SUB_IDS,
	PDS08_NEW_SUB_QTY,
    PDS08_NEW_SUB_IDS,
	PDS08_REACTIVATION_SUB_QTY,
    PDS08_REACTIVATION_SUB_IDS
) as 

with customer_attribution_event_date as 
(
select
  email_user_prop as customer_email,
  event_date,
  channel_grouping,
  channel_platform,
  utm_source,
  row_number() over (partition by email_user_prop order by event_date desc) as row_number
from SEED_DATA.DEV.EVENTS_ORDER_COMPLETE
order by 1
)

, customer_attribution_cleanup_event_date as 
(
select *
from customer_attribution_event_date
where row_number = 1
)

, customer_attribution_event_monthdate as 
(
select
  email_user_prop as customer_email,
  event_monthdate,
  channel_grouping as channel_grouping_op2,
  channel_platform as channel_platform_op2,
  utm_source,
  row_number() over (partition by email_user_prop order by event_monthdate desc) as row_number
from SEED_DATA.DEV.EVENTS_ORDER_COMPLETE
order by 1
)

, customer_attribution_cleanup_event_monthdate as 
(
select *
from customer_attribution_event_monthdate
where row_number = 1
)

, subscription_master as (
select 
  sm.* exclude (channel_grouping, channel_platform)
  
from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER" as sm 
)


, subscription_master_attribution as (
select 
  sm.*
, ifnull(cae.channel_grouping,null) as channel_grouping
, ifnull(cae.channel_platform,null) as channel_platform

from subscription_master as sm 
---- by event_date
left join customer_attribution_cleanup_event_date as cae
on lower(sm.customer_email) = lower(cae.customer_email)
and to_date(sm.activated_at) = cae.event_date
)

, subscription_master_attribution_null as (
select  
  sm.* exclude (channel_grouping, channel_platform)
, ifnull(cae.channel_grouping_op2,null) as channel_grouping
, ifnull(cae.channel_platform_op2,null) as channel_platform

from subscription_master_attribution as sm 
---- by event_date
left join customer_attribution_cleanup_event_monthdate as cae
on lower(sm.customer_email) = lower(cae.customer_email)
and date_trunc('month',to_date(sm.activated_at)) = cae.event_monthdate

where sm.channel_grouping is null 
)

, subscription_master_attribution_null_plus as (
select  
  sm.* exclude (channel_grouping, channel_platform)
, ifnull(cae.channel_grouping_op2,null) as channel_grouping
, ifnull(cae.channel_platform_op2,null) as channel_platform

from subscription_master_attribution_null as sm 
---- by event_date
left join customer_attribution_cleanup_event_monthdate as cae
on lower(sm.customer_email) = lower(cae.customer_email)

where sm.channel_grouping is null 
)

, subscription_master_attribution_combined as (
select * from subscription_master_attribution where channel_grouping is not null 
union all
select * from subscription_master_attribution_null where channel_grouping is not null 
union all
select * from subscription_master_attribution_null_plus
) 

, build as (
select 
  to_date(sma.activated_at) as activated_date
, CASE WHEN DAYNAME(to_date(sma.activated_at)) = 'Sun' then 1 
       WHEN DAYNAME(to_date(sma.activated_at)) = 'Mon' then 2
       WHEN DAYNAME(to_date(sma.activated_at)) = 'Tue' then 3 
       WHEN DAYNAME(to_date(sma.activated_at)) = 'Wed' then 4 
       WHEN DAYNAME(to_date(sma.activated_at)) = 'Thu' then 5 
       WHEN DAYNAME(to_date(sma.activated_at)) = 'Fri' then 6 
       WHEN DAYNAME(to_date(sma.activated_at)) = 'Sat' then 7 
       end as activated_day_of_week
, date_trunc('month',to_date(sma.activated_at)) as activated_month_date
, date_trunc('week',to_date(sma.activated_at))-1 as activated_week_date ---- minus 1 day to start on Sunday
, ifnull(sma.channel_grouping,'None') as channel_grouping
, ifnull(sma.channel_platform,'None') as channel_platform
, ifnull(sum(sma.quantity),0) as total_sub_qty
, ifnull(count(distinct sma.RECURLY_SUBSCRIPTION_ID),0) as total_sub_ids
, ifnull(sum(case when sma.reactivation_flag = 0 then sma.quantity end),0) as new_sub_qty
, ifnull(count(distinct case when sma.reactivation_flag = 0 then sma.RECURLY_SUBSCRIPTION_ID end),0) as new_sub_ids
, ifnull(sum(case when sma.reactivation_flag = 1 then sma.quantity end),0) as reactivation_sub_qty
, ifnull(count(distinct case when sma.reactivation_flag = 1 then sma.RECURLY_SUBSCRIPTION_ID end),0) as reactivation_sub_ids
--- DS-01
, ifnull(sum(case when sma.first_product = 'DS-01' then sma.quantity end),0) as ds01_total_sub_qty
, ifnull(count(distinct case when sma.first_product = 'DS-01' then sma.RECURLY_SUBSCRIPTION_ID end),0) as ds01_total_sub_ids
, ifnull(sum(case when sma.first_product = 'DS-01' and sma.reactivation_flag = 0 then sma.quantity end),0) as ds01_new_sub_qty
, ifnull(count(distinct case when sma.first_product = 'DS-01' and sma.reactivation_flag = 0 then sma.RECURLY_SUBSCRIPTION_ID end),0) as ds01_new_sub_ids
, ifnull(sum(case when sma.first_product = 'DS-01' and sma.reactivation_flag = 1 then sma.quantity end),0) as ds01_reactivation_sub_qty
, ifnull(count(distinct case when sma.first_product = 'DS-01' and sma.reactivation_flag = 1 then sma.RECURLY_SUBSCRIPTION_ID end),0) as ds01_reactivation_sub_ids
--- PDS-08
, ifnull(sum(case when sma.first_product = 'PDS-08' then sma.quantity end),0) as pds08_total_sub_qty
, ifnull(count(distinct case when sma.first_product = 'PDS-08' then sma.RECURLY_SUBSCRIPTION_ID end),0) as pds08_total_sub_ids
, ifnull(sum(case when sma.first_product = 'PDS-08' and sma.reactivation_flag = 0 then sma.quantity end),0) as pds08_new_sub_qty
, ifnull(count(distinct case when sma.first_product = 'PDS-08' and sma.reactivation_flag = 0 then sma.RECURLY_SUBSCRIPTION_ID end),0) as pds08_new_sub_ids
, ifnull(sum(case when sma.first_product = 'PDS-08' and sma.reactivation_flag = 1 then sma.quantity end),0) as pds08_reactivation_sub_qty
, ifnull(count(distinct case when sma.first_product = 'PDS-08' and sma.reactivation_flag = 1 then sma.RECURLY_SUBSCRIPTION_ID end),0) as pds08_reactivation_sub_ids
from subscription_master_attribution_combined as sma
where 
to_date(sma.activated_at) <= to_date(current_date()-1)
group by 1,2,3,4,5,6
)

select * from build

--select 
--sum(total_sub_qty)
--from build where activated_date = '2023-07-18'
;