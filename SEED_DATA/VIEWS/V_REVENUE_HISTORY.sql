create or replace view SEED_DATA.DEV.V_REVENUE_HISTORY as 

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
        O.CREDIT_APPLIED,
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
        
    ,gross_revenue/o.quantity as Net_Value
  
  
  
  
    from all_orders as o
    left join reactivation as s
    on o.subscription_id = s.subscription_id and TO_DATE(O.INVOICE_DATE) = s.date
    left join new as n
    on o.subscription_id = n.subscription_id and TO_DATE(O.INVOICE_DATE) = n.date
      
 )

    
  select 
      sub_inv.INVOICE_DATE,
      date_trunc('month',sub_inv.INVOICE_DATE) as INVOICE_MONTH,
      SKU_MASTER_ID as PRODUCT,
      SKU,
      IFNULL(SUM(QUANTITY),0) as TOTAL_SUB_QTY,
      IFNULL(sum(case when (NEW_FLAG = 'Y' or REACTIVATION_FLAG = 'Y') then quantity end),0) as ACTIVATED_SUB_QTY,
      IFNULL(sum(case when NEW_FLAG = 'Y' then quantity end),0) as ACTIVATED_NEW_SUB_QTY,
      IFNULL(sum(case when REACTIVATION_FLAG = 'Y' then quantity end),0) as ACTIVATED_REACTIVATE_SUB_QTY,
      IFNULL(COUNT(distinct SUBSCRIPTION_ID),0) as TOTAL_SUBSCRIBERS,
      IFNULL(COUNT(distinct case when (NEW_FLAG = 'Y' or REACTIVATION_FLAG = 'Y') then SUBSCRIPTION_ID end),0) as ACTIVATED_SUB_ID,
      IFNULL(COUNT(distinct case when NEW_FLAG = 'Y' then SUBSCRIPTION_ID end),0) as ACTIVATED_NEW_SUB_ID,
      IFNULL(COUNT(distinct case when REACTIVATION_FLAG = 'Y' then SUBSCRIPTION_ID end),0) as ACTIVATED_REACTIVATE_SUB_ID,       
      IFNULL(count(distinct CUSTOMER_ID),0) as TOTAL_ACCOUNTS,
      IFNULL(COUNT(distinct case when (NEW_FLAG = 'Y' or REACTIVATION_FLAG = 'Y') then CUSTOMER_ID end),0) as ACTIVATED_ACCOUNT,
      IFNULL(COUNT(distinct case when NEW_FLAG = 'Y' then CUSTOMER_ID end),0) as ACTIVATED_NEW_ACCOUNT,
      IFNULL(COUNT(distinct case when REACTIVATION_FLAG = 'Y' then CUSTOMER_ID end),0) as ACTIVATED_REACTIVATE_ACCOUNT,        
      IFNULL(SUM(TOTAL_AMOUNT_PAID),0) as TOTAL_AMOUNT_PAID,
      IFNULL(sum(case when (NEW_FLAG = 'Y' or REACTIVATION_FLAG = 'Y') then TOTAL_AMOUNT_PAID end),0) as ACTIVATED_TOTAL_AMOUNT_PAID,
      IFNULL(sum(case when NEW_FLAG = 'Y' then TOTAL_AMOUNT_PAID end),0) as ACTIVATED_NEW_TOTAL_AMOUNT_PAID,
      IFNULL(sum(case when REACTIVATION_FLAG = 'Y' then TOTAL_AMOUNT_PAID end),0) as ACTIVATED_REACTIVATE_TOTAL_AMOUNT_PAID,
      IFNULL(SUM(gross_revenue),0) as TOTAL_GROSS_REV,
      IFNULL(sum(case when (NEW_FLAG = 'Y' or REACTIVATION_FLAG = 'Y') then gross_revenue end),0) as ACTIVATED_TOTAL_GROSS_REV,
      IFNULL(sum(case when NEW_FLAG = 'Y' then gross_revenue end),0) as ACTIVATED_NEW_TOTAL_GROSS_REV,
      IFNULL(sum(case when REACTIVATION_FLAG = 'Y' then gross_revenue end),0) as ACTIVATED_REACTIVATE_TOTAL_GROSS_REV,
      IFNULL(SUM(DISCOUNT),0) as TOTAL_DISCOUNT,
      IFNULL(sum(case when (NEW_FLAG = 'Y' or REACTIVATION_FLAG = 'Y') then DISCOUNT end),0) as ACTIVATED_TOTAL_DISCOUNT,
      IFNULL(sum(case when NEW_FLAG = 'Y' then DISCOUNT end),0) as ACTIVATED_NEW_TOTAL_DISCOUNT,
      IFNULL(sum(case when REACTIVATION_FLAG = 'Y' then DISCOUNT end),0) as ACTIVATED_REACTIVATE_TOTAL_DISCOUNT,
      IFNULL(SUM(TOTAL_SHIPPING_COST),0) as TOTAL_SHIPPING_COST,
      IFNULL(sum(case when (NEW_FLAG = 'Y' or REACTIVATION_FLAG = 'Y') then TOTAL_SHIPPING_COST end),0) as ACTIVATED_TOTAL_SHIPPING_COST,
      IFNULL(sum(case when NEW_FLAG = 'Y' then TOTAL_SHIPPING_COST end),0) as ACTIVATED_NEW_TOTAL_SHIPPING_COST,
      IFNULL(sum(case when REACTIVATION_FLAG = 'Y' then TOTAL_SHIPPING_COST end),0) as ACTIVATED_REACTIVATE_TOTAL_SHIPPING_COST,
      IFNULL(SUM(AMOUNT_REFUNDED),0) as TOTAL_REFUNDED,
      IFNULL(sum(case when (NEW_FLAG = 'Y' or REACTIVATION_FLAG = 'Y') then AMOUNT_REFUNDED end),0) as ACTIVATED_TOTAL_REFUNDED,
      IFNULL(sum(case when NEW_FLAG = 'Y' then AMOUNT_REFUNDED end),0) as ACTIVATED_NEW_REFUNDED,
      IFNULL(sum(case when REACTIVATION_FLAG = 'Y' then AMOUNT_REFUNDED end),0) as ACTIVATED_REACTIVATE_TOTAL_REFUNDED,
      IFNULL(SUM(CREDIT_APPLIED),0) as TOTAL_CREDIT,
      IFNULL(sum(case when (NEW_FLAG = 'Y' or REACTIVATION_FLAG = 'Y') then CREDIT_APPLIED end),0) as ACTIVATED_TOTAL_CREDIT,
      IFNULL(sum(case when NEW_FLAG = 'Y' then CREDIT_APPLIED end),0) as ACTIVATED_NEW_TOTAL_CREDIT,
      IFNULL(sum(case when REACTIVATION_FLAG = 'Y' then CREDIT_APPLIED end),0) as ACTIVATED_REACTIVATE_TOTAL_CREDIT
          
    from sub_inv
    group by 1,2,3,4
    ;