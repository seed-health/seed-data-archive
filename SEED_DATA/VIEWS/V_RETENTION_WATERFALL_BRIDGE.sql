create or replace view SEED_DATA.DEV.V_RETENTION_WATERFALL_BRIDGE as 

with date_range as								
(								
select								
date								
from seed_data.dev.dim_date								
where year in (2023) and month <= 9  and day = 1								
)								
								
								
,    all_orders as								
(								
with orders as								
(    select subscription_id,								
invoice_id,								
invoice_date,								
quantity as invoiced_quantity,								
case when sku ilike '%syn%' or sku ilike 'ds01%' then 'DS-01'								
when sku ilike '%pds%' then 'PDS-08'								
else null end as product,								
case when sku ilike '%wk' then 'Welcome Kit'								
when sku ilike '%wk-3mo%' then 'Welcome kit - 3 Months'								
when sku ilike '%wk-6mo%' then 'Welcome kit - 6 Months'								
when sku ilike '%rf' then 'Refill'								
when sku ilike '%2mo%' then 'Refill - 2 Months'								
when sku ilike '%3mo%' then 'Refill - 3 Months'								
when SKU ilike '%6mo%' then 'Refill - 6 Months'								
else null end as sku_clean,								
'Billed' as bill_flag,								
case when bill_flag = 'Billed' and sku_clean ilike '%2 Months' then dateadd(day,60,invoice_date)								
when bill_flag = 'Billed' and sku_clean ilike '%3 Months' then dateadd(day,90,invoice_date)								
when bill_flag = 'Billed' and sku_clean ilike '%6 Months' then dateadd(day,180,invoice_date)								
when bill_flag = 'Billed' then dateadd(day,30,invoice_date)								
else null end as potential_bill_date								
from SEED_DATA.DEV.ORDER_HISTORY								
where sku_clean is not null -- removing non mainstream sku								
--and base_price <> 0 and total_amount_paid <> 0  -- removing 0$ value and paid 0$ transactions								
and subscription_id is not null -- removing invoice with no subscription mapping								
),								
								
recursive_months AS (								
SELECT								
subscription_id,								
invoice_id,								
DATEADD(day,1*30,invoice_date),								
invoiced_quantity,								
product,								
sku_clean,								
'Proxy' as bill_flag,								
'2099-01-01' as potential_bill_date								
FROM orders								
WHERE (sku_clean ilike '%2 Months%' or sku_clean ilike '%3 Months%' or sku_clean ilike '%6 Months%') and (DATEADD(day,1*30,invoice_date) <= current_date())								
								
UNION ALL								
								
SELECT								
subscription_id,								
invoice_id,								
DATEADD(day,2*30,invoice_date),								
invoiced_quantity,								
product,								
sku_clean,								
'Proxy' as bill_flag,								
'2099-01-01' as potential_bill_date								
FROM orders								
WHERE (sku_clean ilike '%3 Months%' or sku_clean ilike '%6 Months%') and (DATEADD(day,2*30,invoice_date) <= current_date())								
								
UNION ALL								
								
SELECT								
subscription_id,								
invoice_id,								
DATEADD(day,3*30,invoice_date),								
invoiced_quantity,								
product,								
sku_clean,								
'Proxy' as bill_flag,								
'2099-01-01' as potential_bill_date								
FROM orders								
WHERE sku_clean ilike '%6 Months%' and (DATEADD(day,3*30,invoice_date) <= current_date())								
								
UNION ALL								
								
SELECT								
subscription_id,								
invoice_id,								
DATEADD(day,4*30,invoice_date),								
invoiced_quantity,								
product,								
sku_clean,								
'Proxy' as bill_flag,								
'2099-01-01' as potential_bill_date								
FROM orders								
WHERE sku_clean ilike '%6 Months%' and (DATEADD(day,4*30,invoice_date) <= current_date())								
								
UNION ALL								
								
SELECT								
subscription_id,								
invoice_id,								
DATEADD(day,5*30,invoice_date),								
invoiced_quantity,								
product,								
sku_clean,								
'Proxy' as bill_flag,								
'2099-01-01' as potential_bill_date								
FROM orders								
WHERE sku_clean ilike '%6 Months%' and (DATEADD(day,5*30,invoice_date) <= current_date())								
)								
								
								
SELECT subscription_id,invoice_id,invoice_date,invoiced_quantity,product,sku_clean,bill_flag, potential_bill_date								
FROM orders								
UNION ALL								
select *								
from recursive_months								
)								
								
								
, transaction as								
(								
select dr.date ,								
o.subscription_id as subscription_id,								
to_date(s.activated_at) as subscription_start_date,								
reactivation_flag,								
to_date(invoice_date) as invoice_date,								
bill_flag,								
case when date_trunc('month',to_date(s.activated_at)) = date_trunc('month',to_date(invoice_date)) then 'New' else 'Recurring' end as sub_flag								
from all_orders as o								
left join seed_data.dev.subscription_master as s on o.subscription_id = s.recurly_subscription_id								
cross join date_range as dr								
where to_date(invoice_date) >= dr.date and to_date(invoice_date) < add_months(dr.date,1)								
)								
								
, pause_history as								
(								
select dr.date,								
subscription_uuid,								
case when date_trunc('month',version_started_at) = dr.date then 'new'								
when date_trunc('month',version_ended_at_clean) = dr.date then 'expired'								
else 'retained' end as pause_flag								
from seed_data.dev.subscription_pause_history								
cross join date_range as dr								
where (version_started_at < add_months(dr.date,1)) and								
(version_ended_at_clean is null or date_trunc('month',version_ended_at_clean) >= dr.date) and								
(version_ended_at_clean is null or (date_trunc('month',version_started_at) != date_trunc('month',version_ended_at_clean)))								
)								
								
, cancel as								
(								
select dr.date,								
recurly_subscription_id as sub_id,								
'cancel' as cancel_flag								
from seed_data.dev.subscription_master								
cross join date_range as dr								
where cancelled_at >= dr.date and cancelled_at < add_months(dr.date,1)								
)								
, retain_cancel as								
(								
select dr.date,								
count(distinct recurly_subscription_id) as cancel_retained_count								
from seed_data.dev.subscription_master								
cross join date_range as dr								
where cancelled_at < dr.date								
and activated_at = last_subscription_date								
group by 1								
order by 1								
)								
								
, eligible_subscribers as								
(								
with all_order_sub as								
(								
select *								
from all_orders as o								
left join seed_data.dev.subscription_master as s on o.subscription_id = s.recurly_subscription_id								
order by invoice_date desc								
)								
								
select dr.date as eligible_date,								
count(distinct case when sku_clean ilike '%2 Months' then subscription_id end) as subscriber_count_refill_2mo,								
count(distinct case when sku_clean ilike '%3 Months' then subscription_id end) as subscriber_count_refill_3mo,								
count(distinct case when sku_clean ilike '%6 Months' then subscription_id end) as subscriber_count_refill_6mo,								
count(distinct case when sku_clean ilike 'Welcome Kit' or sku_clean ilike 'Refill' then subscription_id end) as subscriber_count_refill								
from all_order_sub as es								
cross join date_range as dr								
where to_date(potential_bill_date) >= dr.date and to_date(potential_bill_date) < add_months(dr.date,1) and								
(to_date(pause_start_date) >= dr.date or pause_start_date is null) and								
(to_date(cancelled_at) >= dr.date or cancelled_at is null)								
group by 1								
order by 1								
								
)								
								
								
/*								
select t.date,bill_flag,sub_flag,reactivation_flag,pause_flag,cancel_flag,								
count(distinct case when pause_flag is null and cancel_flag is null then subscription_id								
when pause_flag is null then sub_id else subscription_uuid end) as sub_count								
from transaction as t								
full join pause_history as p on t.date = p.date and t.subscription_id = p.subscription_uuid								
full join cancel as c on c.date = t.date and c.sub_id = t.subscription_id								
group by 1,2,3,4,5,6								
order by 7 desc								
*/								
, all_agg_table as								
(								
select coalesce(t.date,p.date,c.date) as date,								
--- Billed								
count(distinct case when bill_flag = 'Billed' and sub_flag = 'Recurring' and (pause_flag = 'new' or pause_flag is null) then subscription_id end) as total_billed,								
count(distinct case when bill_flag = 'Billed' and sub_flag = 'Recurring' and pause_flag = 'new' then subscription_id end) as billed_and_paused,								
count(distinct case when bill_flag = 'Billed' and sub_flag = 'Recurring' and pause_flag is null and cancel_flag = 'cancel' then subscription_id end) as billed_and_canceled,								
--- Retained								
count(distinct case when bill_flag = 'Proxy' and sub_flag = 'Recurring' then subscription_id end) as total_retained,								
count(distinct case when bill_flag = 'Proxy' and sub_flag = 'Recurring' and pause_flag in ('new','retained') then subscription_id end) as retained_and_paused,								
count(distinct case when bill_flag = 'Proxy' and sub_flag = 'Recurring' and cancel_flag = 'cancel' then subscription_id end) as retained_and_canceled,								
--- New								
count(distinct case when bill_flag = 'Billed' and sub_flag = 'New' and reactivation_flag = 0 then subscription_id end) as total_new,								
count(distinct case when bill_flag = 'Billed' and sub_flag = 'New' and reactivation_flag = 0 and pause_flag = 'new' then subscription_id end) as new_and_paused,								
count(distinct case when bill_flag = 'Billed' and sub_flag = 'New' and reactivation_flag = 0 and cancel_flag = 'cancel' then subscription_id end) new_and_canceled,								
--- Reactivated								
count(distinct case when bill_flag = 'Billed' and sub_flag = 'New' and reactivation_flag = 1 then subscription_id end) as total_reactivated,								
count(distinct case when bill_flag = 'Billed' and sub_flag = 'New' and reactivation_flag = 1 and pause_flag = 'new' then subscription_id end) as reactivated_and_paused,								
count(distinct case when bill_flag = 'Billed' and sub_flag = 'New' and reactivation_flag = 1 and cancel_flag = 'cancel' then subscription_id end) reactivated_and_canceled,								
--- Resume								
count(distinct case when bill_flag = 'Billed' and sub_flag = 'Recurring' and pause_flag = 'expired' then subscription_id end) as total_resumed,								
count(distinct case when bill_flag = 'Billed' and sub_flag = 'Recurring' and pause_flag = 'expired' and cancel_flag = 'cancel' then subscription_id end) as resumed_and_canceled,								
								
--- Cancel								
count(distinct case when t.date is null and cancel_flag = 'cancel' then sub_id end) as canceled,								
--sum(canceled + billed_and_canceled + retained_and_canceled + new_and_canceled + reactivated_and_canceled + resumed_and_canceled) as total_canceled,								
								
--- Pause								
count(distinct case when t.date is null and pause_flag = 'retained' then subscription_uuid end) as pause_retained,								
count(distinct case when t.date is null and pause_flag = 'new' then subscription_uuid end) as pause_new								
--sum(pause_retained + pause_new + billed_and_paused + retained_and_paused + new_and_paused + reactivated_and_paused) as total_pause								
								
from transaction as t								
full join pause_history as p on t.date = p.date and t.subscription_id = p.subscription_uuid								
full join cancel as c on c.date = t.date and c.sub_id = t.subscription_id								
group by 1								
order by 1								
)								
								
select a.* , rc.cancel_retained_count, es.subscriber_count_refill_2mo,es.subscriber_count_refill_3mo,es.subscriber_count_refill_6mo,es.subscriber_count_refill								
from all_agg_table as a								
left join retain_cancel as rc on rc.date = a.date								
left join eligible_subscribers as es on es.eligible_date = a.date								
								