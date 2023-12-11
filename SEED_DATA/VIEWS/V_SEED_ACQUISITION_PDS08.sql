create or replace view SEED_DATA.DEV.V_SEED_ACQUISITION_PDS08 as 
--- using the date spin to pin all metrics to the date
with date_spine as (
    select 
    *
    from seed_data.dev.dim_date
    where date between '2018-01-01' and to_date(current_date())
),

sub_master as (
    select 
    *
    from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
    where 
    (case when sku ilike '%syn%' or sku ilike 'ds01%' then 'DS-01'
                    when sku ilike '%pds%' then 'PDS-08'
                    else null end) = 'PDS-08'
),

cancellation_transaction_history as
(
    select to_date(cancelled_at) as date,
        reason_group,
        ifnull(sum(quantity),0) as sub_cancellation_qty,
        --------- adding for sub ids on 9/21/23
        ifnull(count(distinct RECURLY_SUBSCRIPTION_ID),0) as sub_cancellation_ids
    from sub_master
    where 
    to_date(cancelled_at) between '2018-01-01' and to_date(current_date())
    group by 1,2

), 

cancellation_transaction_history_net_pp as
(
    select to_date(cancelled_at) as date,
        ifnull(sum(quantity),0) as sub_cancellation_qty,
        --------- adding for sub ids on 9/21/23
        ifnull(count(distinct RECURLY_SUBSCRIPTION_ID),0) as sub_cancellation_ids
    from sub_master
    where 
    to_date(cancelled_at) between '2018-01-01' and to_date(current_date())
    and was_previously_paused = 'N'
    group by 1

), 

cancellation_transaction_history_ds01 as
(
    select to_date(cancelled_at) as date,
        ifnull(sum(quantity),0) as sub_cancellation_qty,
        --------- adding for sub ids on 9/21/23
        ifnull(count(distinct RECURLY_SUBSCRIPTION_ID),0) as sub_cancellation_ids
    from sub_master
    where 
    to_date(cancelled_at) between '2018-01-01' and to_date(current_date())
    group by 1

), 

pause as 
(
    select to_date(pause_start_date) as date,
    ifnull(sum(quantity),0) as sub_pause_cnt,
    --------- adding for sub ids on 9/21/23
    ifnull(count(distinct RECURLY_SUBSCRIPTION_ID),0) as sub_pause_ids
    from sub_master
    where 
    to_date(pause_start_date) between '2018-01-01' and to_date(current_date())
    group by 1

),

subscription as (
select 
  to_date(activated_at) as date
, ifnull(sum(quantity),0) as sub_created_qty
--------- adding for sub ids on 9/21/23
, ifnull(count(distinct RECURLY_SUBSCRIPTION_ID),0) as sub_created_ids
, ifnull(count(distinct case when ifnull(first_discount_percentage_base_price,0) > 0 then recurly_subscription_id end),0) as discounted_sub_created_ids
from sub_master
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
group by 1
)

, reactivation as (
select 
  to_date(activated_at) as date
, ifnull(sum(quantity),0) as sub_reactivation_qty
--------- adding for sub ids on 9/21/23
, ifnull(count(distinct RECURLY_SUBSCRIPTION_ID),0) as sub_reactivation_ids
, ifnull(count(distinct case when ifnull(first_discount_percentage_base_price,0) > 0 then recurly_subscription_id end),0) as discounted_sub_reactivation_ids
from sub_master
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 1
group by 1
)

, product_subs as (
select 
  to_date(activated_at) as date,
----------------------------Adjusted away from First Qty on 9/5/23-------------------------  
--  sum(case when first_subscription_date_ds01 is null then quantity end ) as pdsonly_qty
--, sum(case when first_subscription_date_pds08 is null then quantity end ) as dsonly_qty
  sum(case when sku ilike '%syn%' or sku ilike 'ds01%' then quantity end ) as dsonly_qty
, sum(case when sku ilike '%pds%' then quantity end ) as pdsonly_qty
, sum(case when first_subscription_date_ds01 = first_subscription_date_pds08 then quantity end ) as dsandpds_qty
, sum(case when first_subscription_date_ds01 < first_subscription_date_pds08 then quantity end ) as dsafirst_qty
, sum(case when first_subscription_date_ds01 > first_subscription_date_pds08 then quantity end ) as pdsfirst_qty
--------- adding for sub ids on 9/21/23
, count(distinct case when sku ilike '%syn%' or sku ilike 'ds01%' then RECURLY_SUBSCRIPTION_ID end ) as dsonly_sub_ids
, count(distinct case when sku ilike '%pds%' then quantity end ) as pdsonly_sub_ids
, count(distinct case when first_subscription_date_ds01 = first_subscription_date_pds08 then RECURLY_SUBSCRIPTION_ID end ) as dsandpds_sub_ids
, count(distinct case when first_subscription_date_ds01 < first_subscription_date_pds08 then RECURLY_SUBSCRIPTION_ID end ) as dsafirst_sub_ids
, count(distinct case when first_subscription_date_ds01 > first_subscription_date_pds08 then RECURLY_SUBSCRIPTION_ID end ) as pdsfirst_sub_ids

from sub_master
where to_date(activated_at) between '2018-01-01' and to_date(current_date())
group by 1
)

, new_sub as (
select 
  to_date(activated_at) as date
, ifnull(sum(quantity),0) as sub_new_qty
--------- adding for sub ids on 9/21/23
, ifnull(count(distinct RECURLY_SUBSCRIPTION_ID),0) as sub_new_ids
, ifnull(count(distinct case when ifnull(first_discount_percentage_base_price,0) > 0 then recurly_subscription_id end),0) as discounted_sub_new_ids
from sub_master
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 0
group by 1
)

, orders as (
select 
  to_date(INVOICE_DATE) as date
, ifnull(sum(quantity),0) as orders
, ifnull(sum(TOTAL_AMOUNT_PAID),0) as revenue
from SEED_DATA.DEV.ORDER_HISTORY
where 
to_date(INVOICE_DATE) between '2018-01-01' and to_date(current_date())
and (case when sku ilike '%syn%' or sku ilike 'ds01%' then 'DS-01'
                    when sku ilike '%pds%' then 'PDS-08'
                    else null end) = 'PDS-08'

group by 1
)

, finance as (
select
  date
, sum(case when product = 'PDS-08' and SKU_CLEAN in ('Refill - 3 Months') then TOTAL_CREATED_SUB_IDS end) as three_month_srp_sub_created_ids
, sum(case when product = 'PDS-08' and SKU_CLEAN in ('Refill - 6 Months') then TOTAL_CREATED_SUB_IDS end) as six_month_srp_sub_created_ids
, sum(total_gross_rev) as total_gross_rev
, sum(inv_total_sub_ids) as inv_total_subs
, sum(activated_total_gross_rev) as activated_total_gross_rev
from
SEED_DATA.DEV.FINANCIAL_MODEL 
where product = 'PDS-08'
group by 1
)

, spend as (
select 
  to_date(date) as date
, sum(case when lower(channel_update) = 'tiktok' then spend end ) as  tiktok_spend
, sum(case when lower(channel_update) = 'tapjoy' then spend end ) as  tapjoy_spend
, sum(case when lower(channel_update) = 'spotify' then spend end ) as  spotify_spend
, sum(case when lower(channel_update) = 'iHeart' then spend end ) as  iheart_spend
, sum(case when lower(channel_update) = 'google nonbrand' then spend end ) as  googlenb_spend
, sum(case when lower(channel_update) = 'google brand' then spend end ) as  googlebrand_spend
, sum(case when lower(channel_update) = 'outbrain' then spend end ) as  outbrain_spend
, sum(case when lower(channel_update) = 'meta' then spend end ) as  meta_spend
, sum(case when lower(channel_update) = 'rokt' then spend end ) as  rokt_spend
, sum(case when lower(channel_update) = 'pinterest' then spend end ) as  pinterest_spend
, sum(case when lower(channel_update) = 'reddit' then spend end ) as  reddit_spend
, sum(case when lower(channel_update) = 'public radio' then spend end ) as  public_radio_spend
, sum(case when lower(channel_update) = 'bing nonbrand' then spend end ) as  bingnb_spend
, sum(case when lower(channel_update) = 'bing brand' then spend end ) as  bingbrand_spend
, sum(case when lower(channel_update) = 'tradedesk' then spend end ) as  tradedesk_spend
, sum(case when lower(channel_update) = 'snapchat' then spend end ) as  snapchat_spend
, sum(case when lower(channel_update) = 'influencer' then spend end ) as  influencer_spend
, sum(case when lower(channel_update) = 'partnership' then spend end ) as  partnership_spend
, sum(case when lower(channel_update) = 'affiliate' then spend end ) as  affiliate_spend
, sum(case when lower(channel_update) = 'liveintent' then spend end ) as  liveintent_spend
, sum(case when lower(channel_update) = 'youtube' then spend end ) as  youtube_spend
, ifnull(sum(spend),0) as total_spend
from
PROD_DB.GROWTH.V_MARKETING_CHANNEL_SPEND_ALL_V2
--SEED_DATA.DEV.V_MARKETING_CHANNEL_SPEND_ALL
where 
to_date(date) between '2018-01-01' and to_date(current_date())
and product = 'PDS-08'
group by 1
)

, final as (
select 
  ds.date
, DATE_TRUNC('MONTH', ds.date) AS month_date
, 'PDS-08' as product
, sum( case when cth.reason_group = 'Price' then cth.sub_cancellation_qty end ) as price_sub_cancellation_qty
, sum( case when cth.reason_group = 'Compliance' then cth.sub_cancellation_qty end ) as compliance_sub_cancellation_qty
, sum( case when cth.reason_group = 'No improvement' then cth.sub_cancellation_qty end ) as noimprove_sub_cancellation_qty
, sum( case when cth.reason_group = 'Discomfort' then cth.sub_cancellation_qty end ) as discomfort_sub_cancellation_qty
, sum( case when cth.reason_group = 'Subscription aversion' then cth.sub_cancellation_qty end ) as subaver_sub_cancellation_qty
, sum( case when cth.reason_group = 'Stopped taking probiotics' then cth.sub_cancellation_qty end ) as stopped_sub_cancellation_qty
, sum( case when cth.reason_group = 'Switched probiotics' then cth.sub_cancellation_qty end ) as switchprob_cancellation_cnt
, sum( case when cth.reason_group = 'UX' then cth.sub_cancellation_qty end ) as ux_sub_cancellation_qty
, sum( case when cth.reason_group = 'Switched due to price' then cth.sub_cancellation_qty end ) as switchprice_sub_cancellation_qty
, sum( case when cth.reason_group = 'Experience' then cth.sub_cancellation_qty end ) as experience_sub_cancellation_qty
, sum( case when cth.reason_group = 'Other' then cth.sub_cancellation_qty end ) as other_sub_cancellation_qty
, sum( case when cth.reason_group is null then cth.sub_cancellation_qty end ) as null_sub_cancellation_qty
, sum(cth.sub_cancellation_qty) as ttl_sub_cancellation_qty
, sum(cth.sub_cancellation_ids) as ttl_sub_cancellation_ids
, max(cthnpp.sub_cancellation_qty) as netpp_sub_cancellation_qty
, max(cthnpp.sub_cancellation_ids) as netpp_sub_cancellation_ids
, max(sub.sub_created_qty) as sub_created_qty
, max(sub.sub_created_ids) as sub_created_ids
, max(sub.discounted_sub_created_ids) as discounted_sub_created_ids
, max(psub.pdsonly_qty) as sub_created_pdsonly_qty
, max(psub.pdsonly_sub_ids) as sub_created_pdsonly_sub_ids
, max(psub.dsonly_qty) as sub_created_dsonly_qty
, max(psub.dsonly_sub_ids) as sub_created_dsonly_sub_ids
, max(psub.dsandpds_qty) as sub_created_dsandpds_qty
, max(psub.dsandpds_sub_ids) as sub_created_dsandpds_sub_ids
, max(psub.dsafirst_qty) as sub_created_dsafirst_qty
, max(psub.dsafirst_sub_ids) as sub_created_dsafirst_sub_ids
, max(psub.pdsfirst_qty) as sub_created_pdsfirst_qty
, max(psub.pdsfirst_sub_ids) as sub_created_pdsfirst_sub_ids
, max(pau.sub_pause_cnt) as sub_pause_cnt
, max(pau.sub_pause_ids) as sub_pause_ids
, max(rtv.sub_reactivation_qty) as sub_reactivation_qty
, max(rtv.sub_reactivation_ids) as sub_reactivation_ids
, max(rtv.discounted_sub_reactivation_ids) as discounted_sub_reactivation_ids
, max(nsu.sub_new_qty) as sub_new_qty
, max(nsu.sub_new_ids) as sub_new_ids
, max(nsu.discounted_sub_new_ids) as discounted_sub_new_ids
, max(ord.orders) as orders
, max(ord.revenue) as revenue
, max(fin.total_gross_rev) as total_gross_rev
, max(fin.activated_total_gross_rev) as activated_total_gross_rev
, max(fin.inv_total_subs) as inv_total_subs
, max(fin.three_month_srp_sub_created_ids) as three_month_srp_sub_created_ids
, max(fin.six_month_srp_sub_created_ids) as six_month_srp_sub_created_ids
---- spend
, max(tiktok_spend) as tiktok_spend
, max(tapjoy_spend) as tapjoy_spend
, max(spotify_spend) as spotify_spend
, max(iheart_spend) as iheart_spend
, max(googlenb_spend) as googlenb_spend
, max(googlebrand_spend) as googlebrand_spend
, max(bingnb_spend) as bingnb_spend
, max(bingbrand_spend) as bingbrand_spend
, max(outbrain_spend) as outbrain_spend
, max(meta_spend) as meta_spend
, max(rokt_spend) as rokt_spend
, max(pinterest_spend) as pinterest_spend
, max(reddit_spend) as reddit_spend
, max(public_radio_spend) as public_radio_spend
, max(tradedesk_spend) as tradedesk_spend
, max(snapchat_spend) as snapchat_spend
, max(influencer_spend) as influencer_spend
, max(partnership_spend) as partnership_spend
, max(affiliate_spend) as affiliate_spend
, max(liveintent_spend) as liveintent_spend
, max(youtube_spend) as youtube_spend
, max(total_spend) as total_spend

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
---- join to finance
left join finance as fin
on ds.date = fin.date
---- join to product subs
left join product_subs as psub
on ds.date = psub.date
---- join to spend by channel
left join spend as spd
on ds.date = spd.date
---- join to cancellations net previously paused
left join cancellation_transaction_history_net_pp as cthnpp
on ds.date = cthnpp.date

group by 1,2,3 )

select * from final order by 1 desc;