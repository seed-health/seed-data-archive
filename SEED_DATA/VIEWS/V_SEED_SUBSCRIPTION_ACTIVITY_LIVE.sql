create or replace view SEED_DATA.DEV.V_SEED_SUBSCRIPTION_ACTIVITY_LIVE as 

--- using the date spin to pin all metrics to the date
with date_spine as (
    select 
    *
    from seed_data.dev.dim_date
    where date between '2020-01-01' and to_date(current_date())
)

, cancellation_transaction_history as (
select 
  to_date(cancelation_form_created_at) as date
, reason_group
, ifnull(count(distinct subscription_id),0) as sub_cancellation_cnt
from SEED_DATA.DEV.V_CANCELLATION_TRANSACTION_HISTORY
where 
to_date(cancelation_form_created_at) between '2020-01-01' and to_date(current_date())
group by 1,2
)

----- add quantity
, pause as 
(
 select 
   to_date(pause_start_date) as date
 , count(distinct subscription_uuid) as sub_pause_cnt
 from (
 ----- pause build
    select subscription_uuid,
        version_started_at as pause_start_date, 
        version_ended_at_clean as pause_end_date,
        row_number() over(partition by subscription_uuid order by version_started_at desc) as pause_rank
    from "SEED_DATA"."DEV"."V_SUBSCRIPTION_PAUSE_HISTORY"
    qualify pause_rank = 1 
   ) 
 where pause_start_date is not null
 group by 1
 --order by 1 desc
)

, subscription as (
select 
  to_date(created_at) as date
, ifnull(sum(quantity),0) as sub_created_qty
from SEED_DATA.DEV.V_SUBSCRIPTION
where 
to_date(created_at) between '2020-01-01' and to_date(current_date())
group by 1
--order by 1 desc
)

, reactivation as (
select 
  to_date(created_at) as date
, ifnull(sum(quantity),0) as sub_reactivation_qty
from SEED_DATA.DEV.V_REACTIVATION_HISTORY
where 
to_date(created_at) between '2020-01-01' and to_date(current_date())
and reactivation_flag = 1
group by 1
--order by 1 desc
)

, new_sub as (
select 
  to_date(created_at) as date
, ifnull(sum(quantity),0) as sub_new_qty
from SEED_DATA.DEV.V_REACTIVATION_HISTORY
where 
to_date(created_at) between '2020-01-01' and to_date(current_date())
and reactivation_flag = 0
group by 1
--order by 1 desc
)

, orders as (
select 
  to_date(INVOICE_DATE) as date
, ifnull(sum(quantity),0) as orders
, ifnull(sum(TOTAL_AMOUNT_PAID),0) as revenue
from SEED_DATA.DEV.V_ORDER_HISTORY
where 
to_date(INVOICE_DATE) between '2020-01-01' and to_date(current_date())
group by 1
--order by 1 desc
)


select 
  ds.date
, DATE_TRUNC('MONTH', ds.date) AS month_date
, sum( case when cth.reason_group = 'Price' then cth.sub_cancellation_cnt end ) as price_sub_cancellation_cnt
, sum( case when cth.reason_group = 'Compliance' then cth.sub_cancellation_cnt end ) as compliance_sub_cancellation_cnt
, sum( case when cth.reason_group = 'No improvement' then cth.sub_cancellation_cnt end ) as noimprove_sub_cancellation_cnt
, sum( case when cth.reason_group = 'Discomfort' then cth.sub_cancellation_cnt end ) as discomfort_sub_cancellation_cnt
, sum( case when cth.reason_group = 'Subscription aversion' then cth.sub_cancellation_cnt end ) as subaver_sub_cancellation_cnt
, sum( case when cth.reason_group = 'Stopped taking probiotics' then cth.sub_cancellation_cnt end ) as stopped_sub_cancellation_cnt
, sum( case when cth.reason_group = 'Switched probiotics' then cth.sub_cancellation_cnt end ) as switchprob_cancellation_cnt
, sum( case when cth.reason_group = 'UX' then cth.sub_cancellation_cnt end ) as ux_sub_cancellation_cnt
, sum( case when cth.reason_group = 'Switched due to price' then cth.sub_cancellation_cnt end ) as switchprice_sub_cancellation_cnt
, sum( case when cth.reason_group = 'Experience' then cth.sub_cancellation_cnt end ) as experience_sub_cancellation_cnt
, sum( case when cth.reason_group = 'Other' then cth.sub_cancellation_cnt end ) as other_sub_cancellation_cnt
, sum(cth.sub_cancellation_cnt) as ttl_sub_cancellation_cnt
, max(sub.sub_created_qty) as sub_created_qty
, max(pau.sub_pause_cnt) as sub_pause_cnt
, max(rtv.sub_reactivation_qty) as sub_reactivation_qty
, max(nsu.sub_new_qty) as sub_new_qty
, max(ord.orders) as orders
, max(ord.revenue) as revenue

from date_spine as ds
---- join to cancellations
left join cancellation_transaction_history as cth
on ds.date = cth.date
---- join to total subs
left join subscription as sub
on ds.date = sub.date
---- join to paused subs
left join pause as pau
on ds.date = pau.date
---- join to reactivation subs
left join reactivation as rtv
on ds.date = rtv.date
---- join to new subs
left join new_sub as nsu
on ds.date = nsu.date
---- join to orders
left join orders as ord
on ds.date = ord.date

group by 1,2
order by 1 desc 

--select * from SEED_DATA.DEV.V_SEED_SUBSCRIPTION_ACTIVITY order by 1 desc