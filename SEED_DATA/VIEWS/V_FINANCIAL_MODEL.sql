create or replace view SEED_DATA.DEV.V_FINANCIAL_MODEL(
	DATE,
	MONTH_DATE,
	SKU,
	PRODUCT,
	SKU_CLEAN,
	TOTAL_CANCEL_QTY,
	TOTAL_PAUSE_QTY,
	TOTAL_CREATED_SUB_QTY,
	TOTAL_REACTIVATED_SUB_QTY,
	TOTAL_NEW_SUB_QTY,
	TOTAL_CREATED_ACCTS,
	TOTAL_CREATED_REACTIVATED_ACCTS,
	TOTAL_CREATED_NEW_ACCTS,
	TOTAL_CREATED_SUB_IDS,
	TOTAL_CREATED_NEW_SUB_ID,
	TOTAL_CREATED_REACTIVATED_SUB_ID,
	INV_TOTAL_SUB_QTY,
	INV_TOTAL_CREATED_SUB_QTY,
	INV_TOTAL_NEW_SUB_QTY,
	INV_TOTAL_REACTIVATED_SUB_QTY,
	INV_TOTAL_SUB_IDS,
	INV_TOTAL_CREATED_SUB_IDS,
	INV_TOTAL_CREATED_NEW_SUB_ID,
	INV_TOTAL_CREATED_REACTIVATED_SUB_ID,
	INV_TOTAL_ACCOUNTS,
	INV_TOTAL_CREATED_ACCTS,
	INV_TOTAL_CREATED_NEW_ACCTS,
	INV_TOTAL_CREATED_REACTIVATED_ACCTS,
	TOTAL_BASE_PRICE,
	ACTIVATED_BASE_PRICE,
	ACTIVATED_NEW_BASE_PRICE,
	ACTIVATED_REACTIVATE_BASE_PRICE,
	TOTAL_AMOUNT_PAID,
	ACTIVATED_TOTAL_AMOUNT_PAID,
	ACTIVATED_NEW_TOTAL_AMOUNT_PAID,
	ACTIVATED_REACTIVATE_TOTAL_AMOUNT_PAID,
	TOTAL_GROSS_REV,
	ACTIVATED_TOTAL_GROSS_REV,
	ACTIVATED_NEW_TOTAL_GROSS_REV,
	ACTIVATED_REACTIVATE_TOTAL_GROSS_REV,
	TOTAL_DISCOUNT,
	ACTIVATED_TOTAL_DISCOUNT,
	ACTIVATED_NEW_TOTAL_DISCOUNT,
	ACTIVATED_REACTIVATE_TOTAL_DISCOUNT,
	TOTAL_SHIPPING_COST,
	ACTIVATED_TOTAL_SHIPPING_COST,
	ACTIVATED_NEW_TOTAL_SHIPPING_COST,
	ACTIVATED_REACTIVATE_TOTAL_SHIPPING_COST,
	TOTAL_REFUNDED,
	ACTIVATED_TOTAL_REFUNDED,
	ACTIVATED_NEW_REFUNDED,
	ACTIVATED_REACTIVATE_TOTAL_REFUNDED,
	TOTAL_CREDIT,
	ACTIVATED_TOTAL_CREDIT,
	ACTIVATED_NEW_TOTAL_CREDIT,
	ACTIVATED_REACTIVATE_TOTAL_CREDIT,
	TOTAL_TAX,
	ACTIVATED_TOTAL_TAX,
	ACTIVATED_NEW_TOTAL_TAX,
	ACTIVATED_REACTIVATE_TOTAL_TAX,
	INVOICED_FROM_PRIOR_COHORT,
	UPGRADE_FROM_SYNWK,
	UPGRADE_FROM_SYNRF
) as
with date_spine as (
    select 
    date
    from seed_data.dev.dim_date
    where date between '2018-01-01' and to_date(current_date())
)
  
, products as (
   select UPPER(SKU) AS SKU
          from 
            "SEED_DATA"."DEV"."DIM_PRODUCT"
  )
  
, PRODUCT_DATE as (
   SELECT 
   DATE,
   SKU
   FROM products
   cross join date_spine 
)
  ---------added 9-14-2023----------
					
,orders as								
(    select
  TO_DATE(INVOICE_DATE) as INVOICE_DATE,
        CUSTOMER_ID,
        SUBSCRIPTION_ID as SUBSCRIPTION_ID,
        INVOICE_ID as INVOICE_ID,
        INVOICE_NUMBER AS INVOICE_NUMBER,
        TRANSACTION_ID,
        QUANTITY,
        BASE_PRICE,
        TOTAL_AMOUNT_PAID,
        TAX,
        DISCOUNT,
        TOTAL_SHIPPING_COST,
        AMOUNT_REFUNDED,
        CREDIT_APPLIED,
        UPPER(sku) AS SKU,
        'Billed' as bill_flag								
        from SEED_DATA.DEV.ORDER_HISTORY --- all orders and adjusted orders for SRP 
        where sku is not null -- removing non mainstream sku
        and subscription_id is not null 

)					
								                   		
, transaction as (
								
select 
o.*  , s.reactivation_flag,
case when to_date(s.activated_at) = to_date(invoice_date) then 'New' else 'Recurring' end as sub_flag,
        base_price
        - tax 
        - COALESCE(total_shipping_cost,0) 
        + discount 
        + COALESCE(credit_applied, 0) as gross_revenue    
from orders as o								
left join seed_data.dev.subscription_master as s on o.subscription_id = s.recurly_subscription_id								
				
)			  
  
, CANCELS as
(
    select to_date(cancelled_at) as date,
    UPPER(sku) as sku,
    ifnull(sum(quantity),0) as sub_cancellation_qty
    from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
    where 
    to_date(cancelled_at) between '2018-01-01' and to_date(current_date())
    group by 1,2

), PAUSED as 
(
    select to_date(pause_start_date) as date,
    UPPER(sku) as sku,
    ifnull(sum(quantity),0) as sub_pause_cnt
    from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
    where 
    to_date(pause_start_date) between '2018-01-01' and to_date(current_date())
    group by 1,2

)
, REACTIVATED_SUBS as (
select 
  to_date(activated_at) as date,
 UPPER(sku) as sku,
 ifnull(sum(quantity),0) as sub_reactivation_qty
from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 1
group by 1,2
)

, NEW_SUBS as (
select 
  to_date(activated_at) as date,
   UPPER(sku) as sku,
  ifnull(sum(quantity),0) as sub_new_qty
from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 0 
  --and   to_date(activated_at) = '2023-08-01'
group by 1,2
)

,ALL_SUBS as (
select 
  to_date(activated_at) as date,
  UPPER(sku) as sku,
  ifnull(sum(quantity),0) as sub_created_qty
from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
group by 1,2
)

,ALL_ACCOUNTS as (
select 
  to_date(activated_at) as date,
   UPPER(sku) as sku,
  ifnull(count(distinct customer_id),0) as accounts_all
from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
group by 1,2
)

, REACTIVATED_ACCOUNTS as (
select 
  to_date(activated_at) as date,
   UPPER(sku) as sku,
  ifnull(count(distinct customer_id),0) as account_renew
from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 1
group by 1,2
)

, NEW_ACCOUNTS as (
select 
  to_date(activated_at) as date,
 UPPER(sku) as sku,
 ifnull(count(distinct customer_id),0) as account_new
from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 0
group by 1,2
)  
  

, NEW_SUB_ID as (
select 
  to_date(activated_at) as date,
  UPPER(sku) as sku,
  ifnull(count(distinct recurly_subscription_id),0) as sub_id_new
from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER" 
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 0 and recurly_subscription_id is not null
group by 1,2
  union all
  
  select 
  to_date(activated_at) as date,
   UPPER(sku) as sku,
  ifnull(count(distinct recharge_subscription_id),0) as sub_id_new
from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER" 
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 0 and recharge_subscription_id is not null
group by 1,2
  
)

, REACTIVATED_SUB_ID as (
select 
  to_date(activated_at) as date,
   UPPER(sku) as sku,
  ifnull(count(distinct recurly_subscription_id),0) as sub_id_renew
from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 1 and recurly_subscription_id is not null
group by 1,2
  
  union all
  
  select 
  to_date(activated_at) as date,
   UPPER(sku) as sku,
  ifnull(count(distinct recharge_subscription_id),0) as sub_id_renew
from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 1 and recharge_subscription_id is not null
group by 1,2
  
)	  

,waterfall as (select 
invoice_date as date,
                 UPPER(SKU) as SKU,
       sum(invoiced_From_Prior_Cohort) as invoiced_From_Prior_Cohort,
       sum(upgrade_from_synwk) as upgrade_from_synwk,
       sum(upgrade_from_synrf) as upgrade_from_synrf
     
from "SEED_DATA"."DEV"."V_SRP_INVOICE_BUCKETS" group by 1,2 order by 1 desc
    )
    
,final as  (select
        ds.date,
        date_trunc(month,ds.date) as month_date,
        ds.sku,
        case when ds.sku ilike '%syn%' or ds.sku ilike 'ds01%' then 'DS-01'								
        when ds.sku ilike '%pds%' then 'PDS-08'								
        else null end as product,								
        case when ds.sku ilike '%wk' then 'Welcome Kit'								
             when ds.sku ilike '%wk-3mo%' then 'Welcome kit - 3 Months'								
             when ds.sku ilike '%wk-6mo%' then 'Welcome kit - 6 Months'								
             when ds.sku ilike '%rf' then 'Refill'								
             when ds.sku ilike '%2mo%' then 'Refill - 2 Months'								
             when ds.sku ilike '%3mo%' then 'Refill - 3 Months'								
             when ds.SKU ilike '%6mo%' then 'Refill - 6 Months'								
        else null end as sku_clean																						
, ifnull(max(cth.sub_cancellation_qty),0) as TOTAL_CANCEL_QTY
, ifnull(max(pau.sub_pause_cnt),0) as TOTAL_PAUSE_QTY
, ifnull(max(sub.sub_created_qty),0) as TOTAL_CREATED_SUB_QTY
, ifnull(max(rsu.sub_reactivation_qty),0) as TOTAL_REACTIVATED_SUB_QTY
, ifnull(max(nsu.sub_new_qty),0) as TOTAL_NEW_SUB_QTY
, ifnull(max(aa.accounts_all),0) as TOTAL_CREATED_ACCTS
, ifnull(max(ra.account_renew),0) as TOTAL_CREATED_REACTIVATED_ACCTS
, ifnull(max(na.account_new),0) as TOTAL_CREATED_NEW_ACCTS
, ifnull(max(nsi.sub_id_new),0)+ifnull(max(rsi.sub_id_renew),0) as TOTAL_CREATED_SUB_IDS
, ifnull(max(nsi.sub_id_new),0) as TOTAL_CREATED_NEW_SUB_ID
, ifnull(max(rsi.sub_id_renew),0) as TOTAL_CREATED_REACTIVATED_SUB_ID

,IFNULL(SUM(o.QUANTITY),0) as INV_TOTAL_SUB_QTY
,IFNULL(sum(case when (sub_flag = 'New') then o.quantity end),0) as INV_TOTAL_CREATED_SUB_QTY
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 0 then o.quantity end),0) as INV_TOTAL_NEW_SUB_QTY
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 1  then o.quantity end),0) as INV_TOTAL_REACTIVATED_SUB_QTY
      ,ifnull(count(distinct subscription_id),0) as INV_TOTAL_SUB_IDS
      ,IFNULL(COUNT(distinct case when sub_flag = 'New' then o.SUBSCRIPTION_ID end),0) as INV_TOTAL_CREATED_SUB_IDS
      ,IFNULL(COUNT(distinct case when sub_flag = 'New' and reactivation_flag = 0  then o.SUBSCRIPTION_ID end),0) as INV_TOTAL_CREATED_NEW_SUB_ID
      ,IFNULL(COUNT(distinct case when sub_flag = 'New' and reactivation_flag = 1  then o.SUBSCRIPTION_ID end),0) as INV_TOTAL_CREATED_REACTIVATED_SUB_ID
      ,ifnull(count(distinct o.customer_id),0) as INV_TOTAL_ACCOUNTS
      ,IFNULL(COUNT(distinct case when sub_flag = 'New' then o.CUSTOMER_ID end),0) as INV_TOTAL_CREATED_ACCTS
      ,IFNULL(COUNT(distinct case when sub_flag = 'New' and reactivation_flag = 0  then o.CUSTOMER_ID end),0) as INV_TOTAL_CREATED_NEW_ACCTS
      ,IFNULL(COUNT(distinct case when sub_flag = 'New' and reactivation_flag = 1 then o.CUSTOMER_ID end),0) as INV_TOTAL_CREATED_REACTIVATED_ACCTS
  --------------------------------INVOICE>ORDER REV INFO-----------------------------------  
      ,IFNULL(SUM(BASE_PRICE),0) as TOTAL_BASE_PRICE
      ,IFNULL(sum(case when  sub_flag = 'New'  then BASE_PRICE end),0) as ACTIVATED_BASE_PRICE
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 0 then BASE_PRICE end),0) as ACTIVATED_NEW_BASE_PRICE
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 1  then BASE_PRICE end),0) as ACTIVATED_REACTIVATE_BASE_PRICE
      ,IFNULL(SUM(TOTAL_AMOUNT_PAID),0) as TOTAL_AMOUNT_PAID
      ,IFNULL(sum(case when sub_flag = 'New' then TOTAL_AMOUNT_PAID end),0) as ACTIVATED_TOTAL_AMOUNT_PAID
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 0  then TOTAL_AMOUNT_PAID end),0) as ACTIVATED_NEW_TOTAL_AMOUNT_PAID
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 1 then TOTAL_AMOUNT_PAID end),0) as ACTIVATED_REACTIVATE_TOTAL_AMOUNT_PAID
      ,IFNULL(SUM(gross_revenue),0) as TOTAL_GROSS_REV
      ,IFNULL(sum(case when sub_flag = 'New'  then gross_revenue end),0) as ACTIVATED_TOTAL_GROSS_REV
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 0  then gross_revenue end),0) as ACTIVATED_NEW_TOTAL_GROSS_REV
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 1 then gross_revenue end),0) as ACTIVATED_REACTIVATE_TOTAL_GROSS_REV
      ,IFNULL(SUM(DISCOUNT),0) as TOTAL_DISCOUNT
      ,IFNULL(sum(case when sub_flag = 'New' then DISCOUNT end),0) as ACTIVATED_TOTAL_DISCOUNT
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 0  then DISCOUNT end),0) as ACTIVATED_NEW_TOTAL_DISCOUNT
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 1 then DISCOUNT end),0) as ACTIVATED_REACTIVATE_TOTAL_DISCOUNT
      ,IFNULL(SUM(TOTAL_SHIPPING_COST),0) as TOTAL_SHIPPING_COST
      ,IFNULL(sum(case when sub_flag = 'New' then TOTAL_SHIPPING_COST end),0) as ACTIVATED_TOTAL_SHIPPING_COST
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 0  then TOTAL_SHIPPING_COST end),0) as ACTIVATED_NEW_TOTAL_SHIPPING_COST
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 1 then TOTAL_SHIPPING_COST end),0) as ACTIVATED_REACTIVATE_TOTAL_SHIPPING_COST
      ,IFNULL(SUM(AMOUNT_REFUNDED),0) as TOTAL_REFUNDED
      ,IFNULL(sum(case when sub_flag = 'New'  then AMOUNT_REFUNDED end),0) as ACTIVATED_TOTAL_REFUNDED
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 0  then AMOUNT_REFUNDED end),0) as ACTIVATED_NEW_REFUNDED
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 1 then AMOUNT_REFUNDED end),0) as ACTIVATED_REACTIVATE_TOTAL_REFUNDED
      ,IFNULL(SUM(CREDIT_APPLIED),0) as TOTAL_CREDIT
      ,IFNULL(sum(case when sub_flag = 'New'  then CREDIT_APPLIED end),0) as ACTIVATED_TOTAL_CREDIT
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 0  then CREDIT_APPLIED end),0) as ACTIVATED_NEW_TOTAL_CREDIT
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 1 then CREDIT_APPLIED end),0) as ACTIVATED_REACTIVATE_TOTAL_CREDIT
      ,IFNULL(SUM(TAX),0) as TOTAL_TAX
      ,IFNULL(sum(case when sub_flag = 'New'  then TAX end),0) as ACTIVATED_TOTAL_TAX
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 0  then TAX end),0) as ACTIVATED_NEW_TOTAL_TAX
      ,IFNULL(sum(case when sub_flag = 'New' and reactivation_flag = 1 then TAX end),0) as ACTIVATED_REACTIVATE_TOTAL_TAX
  ---------added 9-14-2023----------
      ,ifnull(max(invoiced_From_Prior_Cohort),0) as invoiced_From_Prior_Cohort
      ,ifnull(max(upgrade_from_synwk),0) as upgrade_from_synwk
      ,ifnull(max(upgrade_from_synrf),0) as upgrade_from_synrf

  from PRODUCT_DATE as ds
  left join  transaction  as o
on ds.date = o.invoice_date and ds.sku = o.sku

left join CANCELS as cth
on ds.date = cth.date and ds.sku = cth.sku
---- join to paused subs
left join PAUSED as pau
on ds.date = pau.date and ds.sku = pau.sku
---- join to total subs
left join ALL_SUBS as sub
on ds.date = sub.date and ds.sku = sub.sku
---- join to reactivated subs
left join REACTIVATED_SUBS as rsu
on ds.date = rsu.date and ds.sku = rsu.sku
---------------join new subs---------
left join NEW_SUBS as nsu
on ds.date = nsu.date and ds.sku = nsu.sku
---------------join all accounts---------
left join ALL_ACCOUNTS  as aa
on ds.date = aa.date and ds.sku = aa.sku
---------------join reactivated accounts---------
left join REACTIVATED_ACCOUNTS  as ra
on ds.date = ra.date and ds.sku = ra.sku
---------------join new accounts---------
left join NEW_ACCOUNTS  as na
on ds.date = na.date and ds.sku = na.sku
---------------join new accounts---------
left join NEW_SUB_ID  as nsi
on ds.date = nsi.date and ds.sku = nsi.sku
---------------join new accounts---------
left join REACTIVATED_SUB_ID   as rsi
on ds.date = rsi.date and ds.sku = rsi.sku
left join waterfall  as wat
on o.invoice_date = wat.date and o.sku = wat.sku
group by 1,2,3,4,5
 )
select * from final;