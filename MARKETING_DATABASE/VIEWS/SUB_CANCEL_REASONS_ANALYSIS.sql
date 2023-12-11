create or replace view MARKETING_DATABASE.PUBLIC.SUB_CANCEL_REASONS_ANALYSIS(
	CUSTOMER_EMAIL,
    FIRST_NAME,
    LAST_NAME,
    SUBSCRIPTION_UUID,
	REASON,
	CONTEXT,
	CANCELED_FORM_SUBMITTED_DATE,
	ACTIVATED_AT,
	CANCELED_AT,
	PRODUCT,
	SHIP_ADDRESS_COUNTRY,
	STP_FLAG,
	MEDIAN_INCOME,
	MEAN_INCOME
) as

    with sub_info as (
        select customer_email,first_name,last_name,recurly_subscription_id,created_at,cancelled_at,'DS-01' as product
        from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_DS_01"
        where recurly_subscription_id is not null
        UNION 
        select customer_email,first_name,last_name,recurly_subscription_id,created_at,cancelled_at,'PDS-08' as product
        from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_PDS_08"
     ),
     cancel_reasons as(
          select distinct customer_email,first_name,last_name,subscription_uuid,reason,context,user_cancel.created as canceled_form_submitted_date,user_sub.activated_at,user_sub.canceled_at,product
          from "MARKETING_DATABASE"."SEED_CORE_PUBLIC"."SEED_ECOMMERCE_USERSUBSCRIPTIONCANCELLATIONREASON" as user_cancel 
          left join "MARKETING_DATABASE"."SEED_CORE_PUBLIC"."SEED_ECOMMERCE_CANCELLATIONREASON" as map on user_cancel.reason_id = map.ID
          left join "MARKETING_DATABASE"."SEED_CORE_PUBLIC"."SEED_ECOMMERCE_SUBSCRIPTION" as user_sub on user_sub.user_subscription_id = user_cancel.user_subscription_id
          left join sub_info on sub_info.recurly_subscription_id = user_sub.subscription_uuid
          where user_sub.canceled_at is not null and user_sub.canceled_at >= '2022-02-22' and datediff('day',canceled_form_submitted_date,user_sub.canceled_at) = 0
    ),
    last_transaction as (
          select distinct adj_last_sku.subscription_id, adjustment_product_code
         from
        (
          select max(adjustment_start_at) as adjustment_last_at, subscription_id
          from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
          where adjustment_description not ilike 'Shipping%' 
          and adjustment_type = 'charge' 
          and subscription_id is not null
          group by subscription_id
        ) as adj_latest
        join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as adj_last_sku 
            on adj_last_sku.adjustment_start_at = adj_latest.adjustment_last_at
            and adj_last_sku.subscription_id = adj_latest.subscription_id
        where adj_last_sku.adjustment_description not ilike 'Shipping%' 
        and adj_last_sku.adjustment_type = 'charge' 
        and adj_last_sku.subscription_id is not null    
        --order by adj_last_sku.adjustment_start_at, adj_last_sku.subscription_id
    ),
    
    zip_code as(
      select s.uuid as subscription_id, left(s.SHIP_ADDRESS_ZIP,5) as zip_code
        from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s 
        --left join "MARKETING_DATABASE"."GOOGLE_SHEETS"."CENSUS_2020" as cen on s.zip_code = cen.zip_code
        where s.SHIP_ADDRESS_COUNTRY = 'US'),
        
    zip_code_merge as (
      
      select zc.*,cen.ESTIMATE_HOUSEHOLDS_MEDIAN_INCOME_DOLLARS_ as median_income, cen.ESTIMATE_HOUSEHOLDS_MEAN_INCOME_DOLLARS_ as mean_income
      from zip_code as zc left join "MARKETING_DATABASE"."GOOGLE_SHEETS"."CENSUS_2020" as cen on zc.zip_code = cen.zip_code)
    
    select cancel_reasons.*,r_sub.SHIP_ADDRESS_COUNTRY, 
    CASE when adjustment_product_code ilike '%3mo%' then 'STP'
    else 'Baseline' end as STP_flag,
    median_income,
    mean_income
    from cancel_reasons 
    left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as r_sub on r_sub.uuid = cancel_reasons.subscription_uuid
    left join last_transaction on cancel_reasons.subscription_uuid = last_transaction.subscription_id
    left join zip_code_merge as zcm on zcm.subscription_id = r_sub.uuid;