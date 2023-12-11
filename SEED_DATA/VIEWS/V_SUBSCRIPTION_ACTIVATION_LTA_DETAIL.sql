create or replace view SEED_DATA.DEV.V_SUBSCRIPTION_ACTIVATION_LTA_DETAIL as 
with customer_attribution_event_date as 
(
select
  email_user_prop as customer_email,
  event_date,
  channel_grouping,
  channel_platform,
  channel_bucket,
  channel,
  utm_source,
  row_number() over (partition by email_user_prop order by event_date desc) as row_number
from PROD_DB.GROWTH.V_EVENTS_ORDER_COMPLETE
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
  channel_bucket as channel_bucket_op2,
  channel as channel_op2,
  utm_source,
  row_number() over (partition by email_user_prop order by event_monthdate desc) as row_number
from PROD_DB.GROWTH.V_EVENTS_ORDER_COMPLETE
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
  sm.* exclude (channel_grouping, channel_platform, channel_bucket, channel)
  
from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER" as sm 
)

, subscription_master_attribution as (
select 
  sm.*
, ifnull(cae.channel_grouping,null) as channel_grouping
, ifnull(cae.channel_platform,null) as channel_platform
, ifnull(cae.channel_bucket,null) as channel_bucket
, ifnull(cae.channel,null) as channel

from subscription_master as sm 
---- by event_date
left join customer_attribution_cleanup_event_date as cae
on lower(sm.customer_email) = lower(cae.customer_email)
and to_date(sm.activated_at) = cae.event_date
)

, subscription_master_attribution_null as (
select  
  sm.* exclude (channel_grouping, channel_platform, channel_bucket, channel)
, ifnull(cae.channel_grouping_op2,null) as channel_grouping
, ifnull(cae.channel_platform_op2,null) as channel_platform
, ifnull(cae.channel_bucket_op2,null) as channel_bucket
, ifnull(cae.channel_op2,null) as channel

from subscription_master_attribution as sm 
---- by event_date
left join customer_attribution_cleanup_event_monthdate as cae
on lower(sm.customer_email) = lower(cae.customer_email)
and date_trunc('month',to_date(sm.activated_at)) = cae.event_monthdate

where sm.channel_grouping is null 
)

, subscription_master_attribution_null_plus as (
select  
  sm.* exclude (channel_grouping, channel_platform, channel_bucket, channel)
, ifnull(cae.channel_grouping_op2,null) as channel_grouping
, ifnull(cae.channel_platform_op2,null) as channel_platform
, ifnull(cae.channel_bucket_op2,null) as channel_bucket
, ifnull(cae.channel_op2,null) as channel

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

select * from 
subscription_master_attribution_combined;