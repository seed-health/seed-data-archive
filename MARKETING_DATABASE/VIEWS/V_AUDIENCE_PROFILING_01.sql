create or replace view MARKETING_DATABASE.PRODUCTION_VIEWS.V_AUDIENCE_PROFILING_01(
	RECURLY_SUBSCRIPTION_ID,
	CUSTOMER_EMAIL,
	FIRST_NAME,
	LAST_NAME,
	CURRENT_STATUS,
	CREATED_AT,
	ACTIVATED_AT,
	CANCELLED_AT,
	BILLING_COUNTRY,
	BILLING_CITY,
	BILLING_STATE,
	IS_INTERNATIONAL,
	SKU,
	DATE_FLAG,
	TOTAL_NO_OF_TRANSACTION,
	TOTAL_TRANSACTION_BEFORE_3MO,
	TOTAL_TRANSACTION_BEFORE_6MO,
	TOTAL_TRANSACTION_BEFORE_12MO,
	TOTAL_MONTHLY_SUPPLIES,
	TOTAL_MONTHLY_SUPPLIES_BEFORE_3MO,
	TOTAL_MONTHLY_SUPPLIES_BEFORE_6MO,
	TOTAL_MONTHLY_SUPPLIES_BEFORE_12MO,
	ACTIVE_DAYS,
	ACTIVE_MONTHS,
	ORDER_RATE,
	ORDER_RATE_BEFORE_3MO,
	ORDER_RATE_BEFORE_6MO,
	ORDER_RATE_BEFORE_12MO,
	SUPPLY_RATE,
	SUPPLY_RATE_BEFORE_3MO,
	SUPPLY_RATE_BEFORE_6MO,
	SUPPLY_RATE_BEFORE_12MO,
	SKIP_SHIPMENT,
	SKIP_SHIPMENT_BEFORE3MO,
	SKIP_SHIPMENT_BEFORE6MO,
	SKIP_SHIPMENT_BEFORE12MO,
	CHURN_FLAG,
	SRP_ADOPTATION,
    LAST_LOGIN,
    LOGIN_COUNT,
    COUPON_CODE,
    DISCOUNT_AMOUNT,
    DISCOUNT_PERCENT,
    COUPON_TYPE,
    DISCOUNT_TYPE
) as


      with DS_01_Map as 
      (
      select RECURLY_SUBSCRIPTION_ID,CUSTOMER_EMAIL,FIRST_NAME,LAST_NAME,CURRENT_STATUS,CREATED_AT,ACTIVATED_AT,CANCELLED_AT,
        BILLING_COUNTRY,BILLING_CITY,BILLING_STATE,IS_INTERNATIONAL,SKU
        from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_DS_01"
        where RECHARGE_SUBSCRIPTION_ID is null
      ),

      all_transactions as 
      (
      select s.UUID as subscription_id , t.date as transaction_date,adjustment_quantity as quantity,ADJUSTMENT_PRODUCT_CODE as SKU, activated_at as subscription_start_date, 
        case when sku ilike '%3mo' then 3 
             when sku ilike '%6mo%' then 6
             else 1 end as sku_month_count,
        case when transaction_date <= dateadd(month,3,activated_at) then 1 else 0 end as transaction_before3mo,
        case when transaction_date <= dateadd(month,6,activated_at) then 1 else 0 end as transaction_before6mo,
        case when transaction_date <= dateadd(month,12,activated_at) then 1 else 0 end as transaction_before12mo--,d.activated_at 
      from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
                              join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
                              left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s on t.subscription_id = s.uuid
                              where t.type = 'purchase'
                                  and t.status = 'success'
                                  and adjustment_description not ilike '%shipping%' 
                                  and adjustment_description not ilike '%Replacement%'
                                  and adjustment_description not ilike '%preorder%'
                                  and (s.plan_name ilike '%DS-01%' or s.plan_name ilike 'Daily Synbiotic%')
      ),

      transaction_summary as 
      (
      select subscription_id, count(*) as total_no_of_transaction, sum(TRANSACTION_BEFORE3MO) as total_transaction_before_3mo,
            sum(TRANSACTION_BEFORE6MO) as total_transaction_before_6mo,sum(TRANSACTION_BEFORE12MO) as total_transaction_before_12mo,
            sum(sku_month_count) as total_monthly_supplies, sum(sku_month_count*TRANSACTION_BEFORE3MO) as total_monthly_supplies_before_3mo,
            sum(sku_month_count*TRANSACTION_BEFORE6MO) as total_monthly_supplies_before_6mo,sum(sku_month_count*TRANSACTION_BEFORE12MO) as total_monthly_supplies_before_12mo

      from all_transactions 
      group by subscription_id
      ),

      order_rate_skip as
      (
      select d.*,ifnull(cancelled_at,current_date()) as date_flag,total_no_of_transaction, total_transaction_before_3mo,total_transaction_before_6mo,total_transaction_before_12mo,
                 total_monthly_supplies,total_monthly_supplies_before_3mo,total_monthly_supplies_before_6mo,total_monthly_supplies_before_12mo,
                 case when datediff(days,ACTIVATED_AT,date_flag) = 0 then 1
                      else datediff(days,ACTIVATED_AT,date_flag) end as active_days, 
                 ceil(active_days/30) as active_months,
                 round(TOTAL_NO_OF_TRANSACTION/active_months,2) as order_rate, case when active_months >= 3 then round(TOTAL_TRANSACTION_BEFORE_3MO/active_months,2) else null end as order_rate_before_3mo,
                  case when active_months >= 6 then round(TOTAL_TRANSACTION_BEFORE_6MO/active_months,2) else null end as order_rate_before_6mo,
                  case when active_months >= 12 then round(TOTAL_TRANSACTION_BEFORE_12MO/active_months,2) else null end as order_rate_before_12mo,
                 round(TOTAL_MONTHLY_SUPPLIES/active_months,2) as supply_rate, case when active_months >= 3 then round(TOTAL_MONTHLY_SUPPLIES_BEFORE_3MO/active_months,2) else null end as supply_rate_before_3mo,
                  case when active_months >= 6 then round(TOTAL_MONTHLY_SUPPLIES_BEFORE_6MO/active_months,2) else null end as supply_rate_before_6mo,
                  case when active_months >= 12 then round(TOTAL_MONTHLY_SUPPLIES_BEFORE_12MO/active_months,2) else null end as supply_rate_before_12mo,
                  case when supply_rate < 1 then 1 else 0 end as skip_shipment, 
                  case when supply_rate_before_3mo is not null and supply_rate_before_3mo < 1 then 1
                       when supply_rate_before_3mo is not null and supply_rate_before_3mo >= 1 then 0
                       else null end as skip_shipment_before3mo,
                  case when supply_rate_before_6mo is not null and supply_rate_before_6mo < 1 then 1
                       when supply_rate_before_6mo is not null and supply_rate_before_6mo >= 1 then 0
                       else null end as skip_shipment_before6mo,
                  case when supply_rate_before_12mo is not null and supply_rate_before_12mo < 1 then 1
                       when supply_rate_before_12mo is not null and supply_rate_before_12mo >= 1 then 0
                       else null end as skip_shipment_before12mo

      from DS_01_Map as d left join transaction_summary as trn on d.recurly_subscription_id = trn.subscription_id
      ),

      churn_before as 
      (
      select distinct customer_email,1 as churn_flag
      from ds_01_map
      where cancelled_at is not null
      ),

      srp_adoptation as 
      (
      select distinct subscription_id, 1 as SRP_adoptation
      from all_transactions 
      where sku_month_count > 1
      ),
      
      customer_login as 
      (
      select email, max(to_date(original_timestamp)) as last_login, count(original_timestamp) as login_count
      from "SEGMENT_EVENTS"."SEED_COM"."USERS" as u left join "SEGMENT_EVENTS"."SEED_COM"."LOGIN" as l on u.ID = l.user_id
      group by email
      ),
      
      coupons as 
      (
        with first_row as 
        (
        select row_number() over(partition by subscription_id order by INVOICE_BILLED_DATE asc) as row_number,
          subscription_id, invoice_billed_date,ADJUSTMENT_COUPON_CODE 
          from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
          where adjustment_description not ilike '%shipping%'
        )

        select subscription_id, adjustment_coupon_code, discount, discount_percent,coupon_type,discount_type
        from first_row as fr left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."COUPONS" as c on fr.adjustment_coupon_code = c.coupon_code
        where row_number = 1 
      )
      
      select o.*,ifnull(c.churn_flag,0) as churn_flag, ifnull(SRP_adoptation,0) as SRP_adoptation, cl.last_login, cl.login_count, 
             cp.adjustment_coupon_code as coupon_code, cp.discount as discount_amount, cp.discount_percent as discount_percent, cp.coupon_type,cp.discount_type
      from order_rate_skip as o 
          left join churn_before as c on o.customer_email = c.customer_email
          left join srp_adoptation as s on o.recurly_subscription_id = s.subscription_id
          left join customer_login as cl on o.customer_email = cl.email
          left join coupons as cp on o.recurly_subscription_id = cp.subscription_id;