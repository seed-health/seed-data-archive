create or replace view SEED_DATA.DEV.V_SRP_INVOICE_BUCKETS(
	INVOICE_DATE,
	SKU,
	INVOICED_FROM_PRIOR_COHORT,
	UPGRADE_FROM_SYNWK,
	UPGRADE_FROM_SYNRF
) as 
 
with other_orders as								
(						
with orders as								
(    select
  TO_DATE(INVOICE_DATE) as INVOICE_DATE,
        CUSTOMER_ID,
        SUBSCRIPTION_ID as SUBSCRIPTION_ID,
        INVOICE_ID as INVOICE_ID,
        INVOICE_NUMBER AS INVOICE_NUMBER,
        TRANSACTION_ID,
       case when sku is null then 'MISSING SKU' else sku end as sku,
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
    from SEED_DATA.DEV.ORDER_HISTORY --- all orders and adjusted orders for SRP 
         where sku_clean is not null -- removing non mainstream sku
            and subscription_id is not null 
--  where subscription_id = '63e00920fb808d3f873f1448469b2197'
   --invoice_number = '2593891' 2743296 	
),								
								
recursive_months AS (								
SELECT	DATEADD(day,1*30,invoice_date),								
        CUSTOMER_ID,
        SUBSCRIPTION_ID as SUBSCRIPTION_ID,
        INVOICE_ID as INVOICE_ID,
        INVOICE_NUMBER AS INVOICE_NUMBER,
        TRANSACTION_ID,
        case when sku is null then 'MISSING SKU' else sku end as sku,												
        sku_clean,								
        'Proxy' as bill_flag,								
        '2099-01-01' as potential_bill_date								
FROM orders								
WHERE (sku_clean ilike '%2 Months%' or sku_clean ilike '%3 Months%' or sku_clean ilike '%6 Months%') and (DATEADD(day,1*30,invoice_date) <= current_date())								
								
UNION ALL								
								
SELECT								
DATEADD(day,2*30,invoice_date),								
        CUSTOMER_ID,
        SUBSCRIPTION_ID as SUBSCRIPTION_ID,
        INVOICE_ID as INVOICE_ID,
        INVOICE_NUMBER AS INVOICE_NUMBER,
        TRANSACTION_ID,
        case when sku is null then 'MISSING SKU' else sku end as sku,								
        sku_clean,								
        'Proxy' as bill_flag,								
        '2099-01-01' as potential_bill_date								
FROM orders								
WHERE (sku_clean ilike '%3 Months%' or sku_clean ilike '%6 Months%') and (DATEADD(day,2*30,invoice_date) <= current_date())								
								
UNION ALL								
								
SELECT								
        DATEADD(day,3*30,invoice_date),								
        CUSTOMER_ID,
        SUBSCRIPTION_ID as SUBSCRIPTION_ID,
        INVOICE_ID as INVOICE_ID,
        INVOICE_NUMBER AS INVOICE_NUMBER,
        TRANSACTION_ID,
        case when sku is null then 'MISSING SKU' else sku end as sku,									
        sku_clean,								
        'Proxy' as bill_flag,								
        '2099-01-01' as potential_bill_date	
FROM orders								
WHERE sku_clean ilike '%6 Months%' and (DATEADD(day,3*30,invoice_date) <= current_date())								
								
UNION ALL								
								
SELECT								
DATEADD(day,4*30,invoice_date),								
        CUSTOMER_ID,
        SUBSCRIPTION_ID as SUBSCRIPTION_ID,
        INVOICE_ID as INVOICE_ID,
        INVOICE_NUMBER AS INVOICE_NUMBER,
        TRANSACTION_ID,
        case when sku is null then 'MISSING SKU' else sku end as sku,								
        sku_clean,								
        'Proxy' as bill_flag,								
        '2099-01-01' as potential_bill_date								
FROM orders								
WHERE sku_clean ilike '%6 Months%' and (DATEADD(day,4*30,invoice_date) <= current_date())								
								
UNION ALL								
								
SELECT								
        DATEADD(day,5*30,invoice_date),								
        CUSTOMER_ID,
        SUBSCRIPTION_ID as SUBSCRIPTION_ID,
        INVOICE_ID as INVOICE_ID,
        INVOICE_NUMBER AS INVOICE_NUMBER,
        TRANSACTION_ID,
        case when sku is null then 'MISSING SKU' else sku end as sku,
        sku_clean,								
        'Proxy' as bill_flag,								
        '2099-01-01' as potential_bill_date	
FROM orders								
WHERE sku_clean ilike '%6 Months%' and (DATEADD(day,5*30,invoice_date) <= current_date())								
)								
								
								
SELECT *								
FROM orders								
UNION ALL								
select *								
from recursive_months								
)

,other_order_final as (
select *,
       case when bill_flag = 'Billed' then dense_rank() over (partition by subscription_id, bill_flag order by invoice_date desc) end as invoice_rank
from other_orders
                    )
                    		
,transactions as (
								
select 
o.*, 
case when date_trunc('month',to_date(s.activated_at)) = date_trunc('month',to_date(invoice_date)) then 'New' else 'Recurring' end as sub_flag,
invoice_rank,  
invoice_rank + 1  as prev_invoice_rank    
from other_order_final as o										
left join seed_data.dev.subscription_master as s on o.subscription_id = s.recurly_subscription_id		  

				
)																											

, previous_transactions as (
         select
           so.subscription_id,
           so.invoice_date,
           so.sku,
           so.sku_clean,
           o.sku_clean as prev_sku
          from transactions so
          left join other_ORDER_final o on so.subscription_id = o.subscription_id
                                 and so.prev_invoice_rank = o.invoice_rank 
                    )
 ,combined_transactions as (                       

select 
       distinct t.*,
                ps.prev_sku as prev_sku_clean
                
       from transactions t
       left join previous_transactions ps
       using (subscription_id, invoice_date)

                    )


        select invoice_date
               ,upper(sku) as SKU 
       ,count(distinct case when bill_flag = 'Billed' and sub_flag = 'Recurring' and sku_clean =  prev_sku_clean then subscription_id end) as invoiced_From_Prior_Cohort
       ,count(distinct case when prev_sku_clean = 'Welcome Kit' and bill_flag = 'Billed' and sub_flag = 'Recurring' and sku_clean in ('Refill - 6 Months', 'Refill - 3 Months') then subscription_id end) as upgrade_from_synwk
       ,count(distinct case when prev_sku_clean = 'Refill' and bill_flag = 'Billed' and sub_flag = 'Recurring' and sku_clean in ('Refill - 6 Months', 'Refill - 3 Months')then subscription_id end) as upgrade_from_synrf

       from combined_transactions group by 1,2;