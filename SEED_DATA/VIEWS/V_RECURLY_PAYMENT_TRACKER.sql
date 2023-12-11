CREATE OR REPLACE VIEW SEED_DATA.DEV.V_RECURLY_PAYMENT_TRACKER as 

-- using the date spin to pin all metrics to the date
with date_spine as (
    select 
    *
    from "SEED_DATA"."DEV"."DIM_DATE"
    where date between '2020-01-01' and to_date(current_date())
  )
  --------To get all transactions
,TOTAL_TRANSACTIONS as (
  
    select
      to_date(date) as date, 
      count(distinct transaction_id) as TOTAL_TRANSACTIONS
        from
        IO06230_RECURLY_SEED_SHARE.CLASSIC.TRANSACTIONS
          WHERE type = 'purchase' 
          and test = 'FALSE'
     group by 1
         )
    --------To get all only customers renewing there subscription
,RENEW as (
  
   select
     to_date(date) as date, 
     count(distinct transaction_id) as RENEW_TRANSACTIONS
       from
       IO06230_RECURLY_SEED_SHARE.CLASSIC.TRANSACTIONS
         WHERE ORIGIN = 'recurring' 
         and type = 'purchase' 
         and test = 'FALSE'
    group by 1
         )
      --------To get only customers starting there subscription
,NEW as (
  
  select
    to_date(date) as date, 
    count(distinct transaction_id) as NEW_TRANSACTIONS
      from
      IO06230_RECURLY_SEED_SHARE.CLASSIC.TRANSACTIONS
        where ORIGIN = 'api' 
        and type = 'purchase' 
        and test = 'FALSE'
   group by 1
         )
  
      --------To get all declines
,DECLINES as (
   select
     to_date(date) as date, 
     count(distinct transaction_id) as TOTAL_DECLINES
       from
       IO06230_RECURLY_SEED_SHARE.CLASSIC.TRANSACTIONS
         where type = 'purchase' 
         and status = 'declined' 
         and test = 'FALSE'
  group by 1
)
        --------To get only declines from customers renewing
,RENEW_DECLINES as (
   select
     to_date(date) as date,  
     count(distinct transaction_id) as RENEW_DECLINES
       from
       IO06230_RECURLY_SEED_SHARE.CLASSIC.TRANSACTIONS
         Where type = 'purchase' 
         and status = 'declined' 
         and test = 'FALSE'
         and ORIGIN = 'recurring' 
  group by 1
)     
          --------To get only declines from customers being billed for first time
,NEW_DECLINES as (
   select
     to_date(date) as date,  
     count(distinct transaction_id) as NEW_DECLINES
       from
       IO06230_RECURLY_SEED_SHARE.CLASSIC.TRANSACTIONS
         Where type = 'purchase' 
         and status = 'declined' 
         and test = 'FALSE'
         and ORIGIN = 'api'
    group by 1
                    )
  
    --------To get only all declines (all customers) by payment method
 ,DECLINE_PAY_TYPES as (

    select
     to_date(date) as date 
     ,count (distinct case when PAYMENT_METHOD = 'Amazon Pay' then transaction_id end) as AMAZON_PAY_DECLINES
     ,count (distinct case when PAYMENT_METHOD = 'Credit Card' then transaction_id end) as CREDIT_CARD_DECLINES
     ,count (distinct case when PAYMENT_METHOD = 'PayPal' then transaction_id end) as PAYPAL_DECLINES
     ,count (distinct case when PAYMENT_METHOD is null then transaction_id end) as MISSING_PAYMENT_METHOD_DECLINES
       from
       IO06230_RECURLY_SEED_SHARE.CLASSIC.TRANSACTIONS
         where type = 'purchase' 
         and status = 'declined' 
         and test = 'FALSE'
       --  and to_date(date) = '2023-07-09'
     group by 1
                   )     
  
    --------To get only all declines for renew customers by payment method 
   ,DECLINE_PAY_TYPES_R as (

    select
     to_date(date) as date 
     ,count (distinct case when PAYMENT_METHOD = 'Amazon Pay' then transaction_id end) as AMAZON_PAY_DECLINES_RENEW
     ,count (distinct case when PAYMENT_METHOD = 'Credit Card' then transaction_id end) as CREDIT_CARD_DECLINES_RENEW
     ,count (distinct case when PAYMENT_METHOD = 'PayPal' then transaction_id end) as PAYPAL_DECLINES_RENEW
     ,count (distinct case when PAYMENT_METHOD is null then transaction_id end) as MISSING_PAYMENT_METHOD_DECLINES_RENEW
       from
       IO06230_RECURLY_SEED_SHARE.CLASSIC.TRANSACTIONS
         where type = 'purchase' 
         and status = 'declined' 
         and test = 'FALSE'
         and ORIGIN = 'recurring'
         --and to_date(date) = '2023-07-09'
     group by 1
                   )    
      --------To get only all declines for new customers by payment method 
  
   ,DECLINE_PAY_TYPES_N as (

    select
     to_date(date) as date 
     ,count (distinct case when PAYMENT_METHOD = 'Amazon Pay' then transaction_id end) as AMAZON_PAY_DECLINES_NEW
     ,count (distinct case when PAYMENT_METHOD = 'Credit Card' then transaction_id end) as CREDIT_CARD_DECLINES_NEW
     ,count (distinct case when PAYMENT_METHOD = 'PayPal' then transaction_id end) as PAYPAL_DECLINES_NEW
     ,count (distinct case when PAYMENT_METHOD is null then transaction_id end) as MISSING_PAYMENT_METHOD_DECLINES_NEW
       from
       IO06230_RECURLY_SEED_SHARE.CLASSIC.TRANSACTIONS
         where type = 'purchase' 
         and status = 'declined' 
         and test = 'FALSE'
         and ORIGIN = 'api'
         --and to_date(date) = '2023-07-09'
     group by 1
                   )     
          ----------Decline reason codes by decline group (gsheet below has the mapping)
  ,DECLINE_REASONS as (
  select
     to_date(date) as date
     ,count (distinct case when DECLINE_GROUPING = 'INVALID CARD INFORMATION' then transaction_id end) as INVALID_CARD_INFO_DECLINE_GROUP
     ,count (distinct case when DECLINE_GROUPING = 'INSUFFICIENT FUNDS' then transaction_id end) as INSUFFICIENT_FUNDS_DECLINE_GROUP
     ,count (distinct case when DECLINE_GROUPING = 'FRAUD-CANCELLED' then transaction_id end) as FRAUD_CANCELLED_DECLINE_GROUP
     ,count (distinct case when  DECLINE_GROUPING = 'BLOCKED CARD'  then transaction_id end) as BLOCKED_CARD_DECLINE_GROUP
     ,count (distinct case when  DECLINE_GROUPING = 'BLOCKED CARD (LIFE CYCLE)' then transaction_id end) as BLOCKED_CARD_LIFECYCLE_DECLINE_GROUP
     ,count (distinct case when  DECLINE_GROUPING = 'BLOCKED CARD (POLICY)'  then transaction_id end) as BLOCKED_CARD_POLICY_DECLINE_GROUP
     ,count (distinct case when  DECLINE_GROUPING is null  then transaction_id end) as OTHER_DECLINE_GROUP
       from
        IO06230_RECURLY_SEED_SHARE.CLASSIC.TRANSACTIONS trans
          left join  "MARKETING_DATABASE"."GOOGLE_SHEETS"."CREDIT_CARD_DECLINE_GROUP_MAPPING" cc
            on trans.message = cc.message
              where type = 'purchase' 
              and status = 'declined' 
              and test = 'FALSE'
              -- and to_date(date) = '2023-07-09'
     group by 1
                   )
  
  
          ----------Decline reason codes by decline group for new (gsheet below has the mapping)
      ,DECLINE_REASONS_NEW as (
   select
     to_date(date) as date
     ,count (distinct case when DECLINE_GROUPING = 'INVALID CARD INFORMATION' then transaction_id end) as INVALID_CARD_INFO_DECLINE_GROUP_NEW
     ,count (distinct case when DECLINE_GROUPING = 'INSUFFICIENT FUNDS' then transaction_id end) as INSUFFICIENT_FUNDS_DECLINE_GROUP_NEW
     ,count (distinct case when DECLINE_GROUPING = 'FRAUD-CANCELLED' then transaction_id end) as FRAUD_CANCELLED_DECLINE_GROUP_NEW
     ,count (distinct case when  DECLINE_GROUPING = 'BLOCKED CARD'  then transaction_id end) as BLOCKED_CARD_DECLINE_GROUP_NEW
     ,count (distinct case when  DECLINE_GROUPING = 'BLOCKED CARD (LIFE CYCLE)' then transaction_id end) as BLOCKED_CARD_LIFECYCLE_DECLINE_GROUP_NEW
     ,count (distinct case when  DECLINE_GROUPING = 'BLOCKED CARD (POLICY)'  then transaction_id end) as BLOCKED_CARD_POLICY_DECLINE_GROUP_NEW
     ,count (distinct case when  DECLINE_GROUPING is null  then transaction_id end) as OTHER_DECLINE_GROUP_NEW
       from
        IO06230_RECURLY_SEED_SHARE.CLASSIC.TRANSACTIONS trans
          left join  "MARKETING_DATABASE"."GOOGLE_SHEETS"."CREDIT_CARD_DECLINE_GROUP_MAPPING" cc
            on trans.message = cc.message
              where type = 'purchase' 
              and status = 'declined' 
              and test = 'FALSE'
              and ORIGIN = 'api'
              -- and to_date(date) = '2023-07-09'
     group by 1
                   )              
          ----------Decline reason codes by decline group for new (gsheet below has the mapping)
      ,DECLINE_REASONS_RENEW as (
   select
     to_date(date) as date
     ,count (distinct case when DECLINE_GROUPING = 'INVALID CARD INFORMATION' then transaction_id end) as INVALID_CARD_INFO_DECLINE_GROUP_RENEW
     ,count (distinct case when DECLINE_GROUPING = 'INSUFFICIENT FUNDS' then transaction_id end) as INSUFFICIENT_FUNDS_DECLINE_GROUP_RENEW
     ,count (distinct case when DECLINE_GROUPING = 'FRAUD-CANCELLED' then transaction_id end) as FRAUD_CANCELLED_DECLINE_GROUP_RENEW
     ,count (distinct case when  DECLINE_GROUPING = 'BLOCKED CARD'  then transaction_id end) as BLOCKED_CARD_DECLINE_GROUP_RENEW
     ,count (distinct case when  DECLINE_GROUPING = 'BLOCKED CARD (LIFE CYCLE)' then transaction_id end) as BLOCKED_CARD_LIFECYCLE_DECLINE_GROUP_RENEW
     ,count (distinct case when  DECLINE_GROUPING = 'BLOCKED CARD (POLICY)'  then transaction_id end) as BLOCKED_CARD_POLICY_DECLINE_GROUP_RENEW
     ,count (distinct case when  DECLINE_GROUPING is null  then transaction_id end) as OTHER_DECLINE_GROUP_RENEW
       from
        IO06230_RECURLY_SEED_SHARE.CLASSIC.TRANSACTIONS trans
          left join  "MARKETING_DATABASE"."GOOGLE_SHEETS"."CREDIT_CARD_DECLINE_GROUP_MAPPING" cc
            on trans.message = cc.message
              where type = 'purchase' 
              and status = 'declined' 
              and test = 'FALSE'
              and ORIGIN = 'recurring'
              -- and to_date(date) = '2023-07-09'
     group by 1
                   )              
       
  select 
    ds.date
   ,DATE_TRUNC('MONTH', ds.date) AS month_date
   ,TOTAL_TRANSACTIONS
   ,RENEW_TRANSACTIONS
   ,NEW_TRANSACTIONS
   ,TOTAL_DECLINES
   ,RENEW_DECLINES
   ,NEW_DECLINES
   ,AMAZON_PAY_DECLINES
   ,CREDIT_CARD_DECLINES
   ,PAYPAL_DECLINES
   ,MISSING_PAYMENT_METHOD_DECLINES
   ,AMAZON_PAY_DECLINES_RENEW
   ,CREDIT_CARD_DECLINES_RENEW
   ,PAYPAL_DECLINES_RENEW
   ,MISSING_PAYMENT_METHOD_DECLINES_RENEW
   ,AMAZON_PAY_DECLINES_NEW
   ,CREDIT_CARD_DECLINES_NEW
   ,PAYPAL_DECLINES_NEW
   ,MISSING_PAYMENT_METHOD_DECLINES_NEW
   ,INVALID_CARD_INFO_DECLINE_GROUP
   ,INSUFFICIENT_FUNDS_DECLINE_GROUP
   ,FRAUD_CANCELLED_DECLINE_GROUP
   ,BLOCKED_CARD_DECLINE_GROUP
   ,BLOCKED_CARD_LIFECYCLE_DECLINE_GROUP
   ,BLOCKED_CARD_POLICY_DECLINE_GROUP
   ,OTHER_DECLINE_GROUP
   ,INVALID_CARD_INFO_DECLINE_GROUP_RENEW
   ,INSUFFICIENT_FUNDS_DECLINE_GROUP_RENEW
   ,FRAUD_CANCELLED_DECLINE_GROUP_RENEW
   ,BLOCKED_CARD_DECLINE_GROUP_RENEW
   ,BLOCKED_CARD_LIFECYCLE_DECLINE_GROUP_RENEW
   ,BLOCKED_CARD_POLICY_DECLINE_GROUP_RENEW
   ,OTHER_DECLINE_GROUP_RENEW
   ,INVALID_CARD_INFO_DECLINE_GROUP_NEW
   ,INSUFFICIENT_FUNDS_DECLINE_GROUP_NEW
   ,FRAUD_CANCELLED_DECLINE_GROUP_NEW
   ,BLOCKED_CARD_DECLINE_GROUP_NEW
   ,BLOCKED_CARD_LIFECYCLE_DECLINE_GROUP_NEW
   ,BLOCKED_CARD_POLICY_DECLINE_GROUP_NEW
   ,OTHER_DECLINE_GROUP_NEW
  
  
  
from date_spine as ds
---- join to TOTAL TRANS
left join TOTAL_TRANSACTIONS as TS
on ds.date = TS.date
---- join to RENW SUBS
left join RENEW as RN
on ds.date = RN.date
---- join to NEW
  left join NEW as NEW
on ds.date = NEW.date
---- join to total declines
left join DECLINES as DC
on ds.date = DC.date
---- join to DECLINE RENEW
left join RENEW_DECLINES as DCR
on ds.date = DCR.date
---- join to RENW SUBS
left join NEW_DECLINES as DCN
on ds.date = DCN.date
---- join to NEW
  left join DECLINE_PAY_TYPES as DPT
on ds.date = DPT.date
---- join to total subs
left join DECLINE_PAY_TYPES_R as DPTR
on ds.date = DPTR.date  
  
  left join DECLINE_PAY_TYPES_N as DPTN
on ds.date = DPTN.date
---- join to RENW SUBS
left join DECLINE_REASONS as DR
on ds.date = DR.date
---- join to NEW
  left join DECLINE_REASONS_RENEW as DRRN
on ds.date = DRRN.date
---- join to total declines
left join DECLINE_REASONS_NEW as DRN
on ds.date = DRN.date