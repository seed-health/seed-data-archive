create or replace view SEED_DATA.DEV.V_REVENUE_HISTORY_PDS08  as 

with all_orders as 
(
    select * from SEED_DATA.DEV.ORDER_HISTORY --- all orders and adjusted orders for SRP
)

 , reactivation as (
select 
  to_date(activated_at) as date
, recharge_subscription_id as subscription_id
, quantity
, 'Y' as Flag
   from
   "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 1 and recharge_subscription_id is not null
   
   union all

   select 
  to_date(activated_at) as date
, recurly_subscription_id as subscription_id
, quantity
, 'Y' as Flag
   from
   "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 1 and Recurly_subscription_id is not null
)

 , New as (
select 
  to_date(activated_at) as date
, recharge_subscription_id as subscription_id
, 'Y' as Flag
   from
   "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 0 and Recharge_subscription_id is not null
   
   union all

   select 
  to_date(activated_at) as date
, recurly_subscription_id as subscription_id
, 'Y' as Flag
   from
   "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER"
where 
to_date(activated_at) between '2018-01-01' and to_date(current_date())
and reactivation_flag = 0 and Recurly_subscription_id is not null)

,sub_inv as 
(
    -- joining recharge invoices
    select 
        TO_DATE(O.INVOICE_DATE) as INVOICE_DATE,
        O.CUSTOMER_ID,
        O.SUBSCRIPTION_ID as SUBSCRIPTION_ID,
        O.INVOICE_ID as INVOICE_ID,
        O.INVOICE_NUMBER AS INVOICE_NUMBER,
        O.TRANSACTION_ID,
        O.SKU,
        O.SKU_DESCRIPTION,
        CASE WHEN UPPER(O.SKU) ILIKE '%PDS%' THEN 'PDS-08' 
             WHEN UPPER(O.SKU) ILIKE '%SYN%' THEN 'DS-01'
             WHEN UPPER(O.SKU) ILIKE '%DS%' THEN 'DS-01'
             ELSE 'OTHER' END AS SKU_MASTER_ID,
       CASE WHEN UPPER(O.SKU) ILIKE '%6MO%' THEN 'Y' 
            WHEN UPPER(O.SKU) ILIKE '%3MO%' THEN 'Y' 
            else 'N' END AS SRP_FLAG,
                CASE WHEN base_price > 0 then 'N'
                     WHEN base_price = 0 and SKU_DESCRIPTION ilike '%Trial%' then 'N'
                     when base_price = 0 and SKU_DESCRIPTION ilike '%Preorder%' then 'N'
                     when base_price = 0 and SKU_DESCRIPTION ilike '%Charge%' then 'N'
                     when base_price = 0 and SKU_DESCRIPTION ilike '%Gift%' then 'N'
                     else 'Y'
                     end as Replacement_Flag,
  
                  CASE WHEN base_price > 0 then 'N'
                     else 'Y'
                     end as Non_Revenue_Shipment_Flag,
        ifnull(S.FLAG,'N') as REACTIVATION_FLAG,
        ifnull(N.FLAG,'N') as NEW_FLAG,
        O.QUANTITY,
        O.BASE_PRICE,
        O.TOTAL_AMOUNT_PAID,
        O.TAX,
        O.DISCOUNT,
        O.AMOUNT_PAID_BY_TRANSACTION,
        O.TOTAL_SHIPPING_COST,
        O.SHIPPING_COST_WO_TAX,
        O.SHIPPING_COST_TAX,
        O.AMOUNT_REFUNDED,
        O.CREDIT_APPLIED
  
    from all_orders as o
    left join reactivation as s
    on o.subscription_id = s.subscription_id and TO_DATE(O.INVOICE_DATE) = s.date
    left join new as n
    on o.subscription_id = n.subscription_id and TO_DATE(O.INVOICE_DATE) = n.date
      
 )

    
  select 
      sub_inv.INVOICE_DATE,
      sub_inv.CUSTOMER_ID,
      sub_inv.SUBSCRIPTION_ID,
      sub_inv.INVOICE_NUMBER,
      sub_inv.INVOICE_ID,
      sub_inv.TRANSACTION_ID,
      sub_inv.SKU,
      sub_inv.SKU_MASTER_ID,
      REACTIVATION_FLAG,
      NEW_FLAG,
      CASE WHEN NEW_FLAG = 'N' and REACTIVATION_FLAG = 'N' then 'Y' else 'N' end as RENEWAL_FLAG,
      SRP_FLAG, 
      QUANTITY,
      BASE_PRICE,
      TOTAL_AMOUNT_PAID,
      TAX,
      DISCOUNT,
      AMOUNT_PAID_BY_TRANSACTION,
      TOTAL_SHIPPING_COST,
      SHIPPING_COST_WO_TAX,
      SHIPPING_COST_TAX,
      AMOUNT_REFUNDED,
      CREDIT_APPLIED,
    base_price
        - tax 
        - COALESCE(total_shipping_cost,0) 
        + discount 
        + COALESCE(credit_applied, 0) as gross_revenue
        
    , base_price
        - tax 
        - COALESCE(shipping_cost_tax,0) 
        + discount 
        + COALESCE(SHIPPING_COST_WO_TAX, 0) as adj_gross_revenue
        
    , base_price
        + tax 
        + COALESCE(total_shipping_cost,0) 
        - discount 
        - COALESCE(credit_applied, 0) as adj_total_paid
        
    , base_price
        + COALESCE(SHIPPING_COST_WO_TAX,0) 
        - discount 
        - COALESCE(credit_applied, 0) as adj_subtotal_paid
        
    ,gross_revenue/quantity as Net_Value
    
    from sub_inv
    where SKU_MASTER_ID = 'PDS-08';