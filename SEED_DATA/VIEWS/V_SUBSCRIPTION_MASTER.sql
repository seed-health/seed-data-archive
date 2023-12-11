create or replace view SEED_DATA.DEV.V_SUBSCRIPTION_MASTER as 
    with orders as 
    (
        with orders_clean as 
        (
            select subscription_id, 
                invoice_date, 
                lower(sku) as sku,
                case when sku ilike '%syn%' or sku ilike 'ds01%' then 'DS-01'
                    when sku ilike '%pds%' then 'PDS-08'
                    else null end as product,
                case when product = 'DS-01' and sku ilike '%wk%' then 'DS-01 Welcome Kit'
                    when product = 'DS-01' and sku ilike '%wk-3mo%' then 'DS-01 Welcome kit - 3 Months'
                    when product = 'DS-01' and sku ilike '%wk-6mo%' then 'DS-01 Welcome kit - 6 Months'
                    when product = 'DS-01' and sku ilike '%rf' then 'DS-01 Refill'
                    when product = 'DS-01' and sku ilike '%2mo%' then 'DS-01 Refill - 2 Months'
                    when product = 'DS-01' and sku ilike '%3mo%' then 'DS-01 SRP Refill - 3 Months'
                    when product = 'DS-01' and sku ilike '%6mo%' then 'DS-01 SRP Refill - 6 Months'
                    when product = 'DS-01' and sku ilike '%trial%' then 'DS-01 Trial'
                    when product = 'PDS-08' and sku ilike '%wk%' then 'PDS-08 Welcome Kit'
                    when product = 'PDS-08' and sku ilike '%rf' then 'PDS-08 Refill'
                    when product = 'PDS-08' and sku ilike '%2mo%' then 'PDS-08 Refill - 2 Months'
                    when product = 'PDS-08' and sku ilike '%3mo%' then 'PDS-08 SRP Refill - 3 Months'
                    when product = 'PDS-08' and sku ilike '%6mo%' then 'PDS-08 SRP Refill - 6 Months'
                    when product = 'PDS-08' and sku ilike '%trial%' then 'PDS-08 Trial'
                    else null                    
                    end as clean_sku, 
                case when clean_sku ilike '%Trial%' then 1 
                    when clean_sku ilike '%Welcome Kit%' then 2 
                    when clean_sku ilike '%Refill - 2 Months' then 3
                    when clean_sku ilike '%Refill' then 4
                    when clean_sku ilike '%Refill - 3 Months' then 5
                    when clean_sku ilike '%Refill - 6 Months' then 6 
                    end as sku_ranking,
                row_number() over(partition by subscription_id,product order by invoice_date,sku_ranking) as first_invoice_product_rank,
                row_number() over (partition by subscription_id,product order by invoice_date desc ,sku_ranking desc) as last_invoice_product_rank,
                row_number() over (partition by subscription_id order by invoice_date,sku_ranking ) as first_invoice_rank,
                row_number() over (partition by subscription_id order by invoice_date desc, sku_ranking desc) as last_invoice_rank,
                case when product = 'DS-01' and first_invoice_product_rank = 1 then invoice_date
                    else null end as first_order_date_ds01,
                case when product = 'PDS-08' and first_invoice_product_rank = 1 then invoice_date
                    else null end as first_order_date_pds08,
                case when first_invoice_rank = 1 then invoice_date 
                    else null end as first_order_date,
                case when product = 'DS-01' and last_invoice_product_rank = 1 then invoice_date
                    else null end as last_order_date_ds01,
                case when product = 'PDS-08' and last_invoice_product_rank = 1 then invoice_date
                    else null end as last_order_date_pds08,
                case when last_invoice_rank = 1 then invoice_date 
                    else null end as last_order_date,
                case when first_invoice_rank = 1 then product
                    else null end as first_product,
                case when first_invoice_rank = 1 then clean_sku
                    else null end as first_sku,
                case when first_invoice_rank = 1 then quantity
                    else null end as first_quantity,
                case when first_invoice_rank = 1 then 
                round(div0((discount*100),(total_amount_paid - tax - total_shipping_cost + discount)),0) else null end as first_discount_percentage,
                ----- adding as part of a test for base price discount
                case when first_invoice_rank = 1 then 
                round(div0((discount*100),(base_price)),0) else null end as first_discount_percentage_base_price,
                case when first_invoice_rank = 1 then promotion_code end as first_promotion_code,
                case when last_invoice_rank = 1 then product 
                    else null end as last_product,
                case when last_invoice_rank = 1 then clean_sku
                    else null end as last_sku,
                case when last_invoice_rank = 1 then quantity
                    else null end as last_quantity,
                case when last_invoice_rank = 1 then round(div0((discount*100),(total_amount_paid - tax - total_shipping_cost +discount)),0) else null end as last_discount_percentage,
                case when last_invoice_rank = 1 then promotion_code end as last_promotion_code,
                case when first_invoice_rank = 1 then (base_price - tax - COALESCE(total_shipping_cost,0) + discount + COALESCE(credit_applied, 0))
                    else null end as first_order_gross_revenue,
                case when first_invoice_rank = 1 then (base_price)
                    else null end as first_order_total_base_price,
                case when first_invoice_rank = 1 then (Discount)
                    else null end as first_order_total_discount
          
      from "SEED_DATA"."DEV"."V_ORDER_HISTORY" as o 
            where subscription_id is not null 
        ),
    
        order_summary as
        (
            select
                subscription_id, 
                min(first_order_date_ds01) as first_order_date_ds01,
                min(first_order_date_pds08) as first_order_date_pds08,
                min(first_order_date) as first_order_date,
                min(first_product) as first_product,
                min(first_sku) as first_sku,
                min(first_quantity) as first_quantity,
                min(first_discount_percentage) as first_discount_percentage,
                min(first_discount_percentage_base_price) as first_discount_percentage_base_price,
                min(first_promotion_code) as first_promotion_code,
                max(last_order_date_ds01) as last_order_date_ds01,
                max(last_order_date_pds08) as last_order_date_pds08,
                max(last_order_date) as last_order_date,
                max(last_product) as last_product,
                max(last_sku) as last_sku,
                max(last_quantity) as last_quantity,
                max(last_discount_percentage) as last_discount_percentage,
                max(last_promotion_code) as last_promotion_code,
                min(first_order_gross_revenue) as first_order_gross_revenue,
                min(first_order_total_base_price) as first_order_total_base_price,
                min(first_order_total_discount) as first_order_total_discount
            from orders_clean    
            group by subscription_id
        )

        
    
        select recharge_subscription_id,
            recurly_subscription_id,
            min(coalesce(order_recharge.first_order_date_ds01,order_recurly.first_order_date_ds01)) as first_order_date_ds01,
            min(coalesce(order_recharge.first_order_date_pds08,order_recurly.first_order_date_pds08)) as first_order_date_pds08,
            min(coalesce(order_recharge.first_order_date,order_recurly.first_order_date)) as first_order_date,
            min(coalesce(order_recharge.first_product,order_recurly.first_product)) as first_product,
            min(coalesce(order_recharge.first_sku,order_recurly.first_sku)) as first_sku,
            min(coalesce(order_recharge.first_quantity,order_recurly.first_quantity)) as first_quantity,
            min(coalesce(order_recharge.first_discount_percentage,order_recurly.first_discount_percentage)) as first_discount_percentage,
            min(coalesce(order_recharge.first_discount_percentage_base_price,order_recurly.first_discount_percentage_base_price)) as first_discount_percentage_base_price,
            min(coalesce(order_recharge.first_promotion_code,order_recurly.first_promotion_code)) as first_promotion_code,
            max(coalesce(order_recurly.last_order_date_ds01,order_recharge.last_order_date_ds01)) as last_order_date_ds01,
            max(coalesce(order_recurly.last_order_date_pds08,order_recharge.last_order_date_pds08)) as last_order_date_pds08,
            max(coalesce(order_recurly.last_order_date,order_recharge.last_order_date)) as last_order_date,
            max(coalesce(order_recurly.last_product,order_recharge.last_product)) as last_product,
            max(coalesce(order_recurly.last_sku,order_recharge.last_sku)) as last_sku,
            max(coalesce(order_recurly.last_quantity,order_recharge.last_quantity)) as last_quantity,
            max(coalesce(order_recurly.last_discount_percentage,order_recharge.last_discount_percentage)) as last_discount_percentage,
            max(coalesce(order_recurly.last_promotion_code,order_recharge.last_promotion_code)) as last_promotion_code,
            min(coalesce(order_recurly.first_order_gross_revenue,order_recharge.first_order_gross_revenue)) as first_order_gross_revenue,
            min(coalesce(order_recurly.first_order_total_base_price,order_recharge.first_order_total_base_price)) as first_order_total_base_price,
            min(coalesce(order_recurly.first_order_total_discount,order_recharge.first_order_total_discount)) as first_order_total_discount
          
        from "SEED_DATA"."DEV"."V_SUBSCRIPTION" as s 
            left join order_summary as order_recharge on order_recharge.subscription_id = s.recharge_subscription_id
            left join order_summary as order_recurly on order_recurly.subscription_id = s.recurly_subscription_id
        group by recharge_subscription_id,recurly_subscription_id
       
    ),
    
    customer_sub_info as 
    (
        with subscription_product as 
        (
            select customer_id, activated_at,cancelled_at,
                case when sku ilike '%syn%' or sku ilike 'ds01%' then 'DS-01'
                    when sku ilike '%pds%' then 'PDS-08'
                    when sku is null and is_recharge_native = 1 and is_imported = 0 then 'DS-01'
                    else null end as product,
                datediff(month,activated_at,coalesce(cancelled_at,current_date()))+1 as months_active
        from "SEED_DATA"."DEV"."V_SUBSCRIPTION"
        )

    select customer_id,
        min(case when product = 'DS-01' then activated_at else null end) as first_subscription_date_ds01,
        min(case when product = 'PDS-08' then activated_at else null end) as first_subscription_date_pds08,
        min(activated_at) as first_subscription_date,
        max(case when product = 'DS-01' then activated_at else null end) as last_subscription_date_ds01,
        max(case when product = 'PDS-08' then activated_at else null end) as last_subscription_date_pds08,
        max(activated_at) as last_subscription_date,
        min(case when product = 'DS-01' then cancelled_at else null end) as first_cancel_date_ds01,
        min(case when product = 'PDS-08' then cancelled_at else null end) as first_cancel_date_pds08,
        min(cancelled_at) as first_cancel_date,
        max(case when product = 'DS-01' then cancelled_at else null end) as last_cancel_date_ds01,
        max(case when product = 'PDS-08' then cancelled_at else null end) as last_cancel_date_pds08,
        max(cancelled_at) as last_cancel_date,
        sum(months_active) as months_active_customer -- overlaps are considered as multiple 
    from subscription_product
    group by customer_id
    )
       
    ,

    cancellation as 
    (
        select recurly_subscription_id,
            to_date(subscription_canceled_at) as cancelled_date,
            primary_reason as cancelled_primary_reason, 
            secondary_reason as cancelled_secondary_reason,
            reason_group
        from "SEED_DATA"."DEV"."V_CANCELLATION_TRANSACTION_HISTORY"
    ),
    
    pause as 
    (
        select subscription_uuid as subscription_id,
            paused_at_clean as pause_start_date, 
            paused_at_clean_ts as pause_start_timestamp,
            version_ended_at_clean as pause_end_date,
            case when pause_end_date is null then 'Y' else 'N' end as Is_Paused,
            row_number() over(partition by subscription_uuid order by version_started_at desc) as pause_rank
        from "SEED_DATA"."DEV"."V_SUBSCRIPTION_PAUSE_HISTORY"
        qualify pause_rank = 1
    
    ),
    
    next_bill as 
    (
        select subscription_id, next_bill_date
        from "SEED_DATA"."DEV"."V_SUBSCRIPTION_NEXT_BILL_DATE"
    ),
    
    sku_history as 
    (
        select subscription_uuid as subscription_id, 
            min(case when updated_plan_code ilike '%3mo%' then version_started_at else null end) as first_enroll_date_srp_3mo,
            max(case when updated_plan_code ilike '%3mo%' then version_started_at else null end) as last_enroll_date_srp_3mo,
            min(case when updated_plan_code ilike '%6mo%' then version_started_at else null end) as first_enroll_date_srp_6mo,
            min(case when updated_plan_code ilike '%6mo%' then version_started_at else null end) as last_enroll_date_srp_6mo,
            case when first_enroll_date_srp_3mo is not null then 1 else 0 end as SRP_3mo_ever_flag,
            case when first_enroll_date_srp_6mo is not null then 1 else 0 end as SRP_6mo_ever_flag
        from "SEED_DATA"."DEV"."V_SUBSCRIPTION_STATUS_HISTORY"
        group by subscription_uuid
    )
 ---------CHECKOUT SURVEY----------
    ,CO_Survey as
      
      (        
select 
  to_date(timestamp) as date
, date_trunc('month',to_date(timestamp)) as month_date
, upper(email) as email
, question
, response
, count(distinct user_id) as user_count
from SEGMENT_EVENTS.SEED_COM.SURVEY_QUESTION_ANSWERED
where question = 'How did you learn about Seed?'
group by 1,2,3,4,5
        )
     , LTA as 
     
     (
       select distinct 
                recurly_subscription_id
              , recharge_subscription_id
              , ifnull(channel,null) as channel
              , ifnull(bucket,null) as bucket
              , ifnull(agency,null) as agency
              , ifnull(spend_type,null) as spend_type
              , ifnull(platform,null) as platform
              , ifnull(erewhon_flag,null) as erewhon_flag
             from PROD_DB.GROWTH.V_SUBSCRIPTION_LTA_MAPPING

     )
 
 -------------
 ,MX_TEAM as (
    select Customer_id, 
           MAX(CONTACTED_MX_TEAM) as CONTACTED_MX_TEAM,
           MAX(SPOKE_W_SCI_CARE) as CONTACTED_SCI_CARE,
           MAX(SPOKE_W_CARE_TEAM) as CONTACTED_CARE
    from SEED_DATA.DEV.V_KUSTOMER_SUBSCRIBER_DETAIL
    where customer_id is not null
    group by 1
   
             )
             
 ----------------------Customer was moved from Pause to Cancel when we removed pause as an option
  ,prev_pause as (
    select
	ses.subscription_uuid,
	case when ses.was_previously_paused = 'TRUE' then 'Y' else 'N' end as was_previously_paused
from MARKETING_DATABASE.SEED_CORE_PUBLIC.SEED_ECOMMERCE_SUBSCRIPTION as ses
where was_previously_paused = True)


------------------------
, marketing_consent as (

select 
       distinct
       email, 
       max(case when consent is not null then 'Y' else 'N' end) as consent_flag
       from SEGMENT_EVENTS.SEED_COM.IDENTIFIES
where not (consent is null)
group by 1 

                        )
                        
, accepts_marketing as (

select 
       distinct
       email, 
       max(case when accepts_marketing ilike '%TRUE%' then 'Y' else 'N' end) as accepts_marketing_flag
       from SEGMENT_EVENTS.SEED_COM.IDENTIFIES
where not (accepts_marketing is null)
group by 1 

                        )                 
                        
, m_and_c_consent as (

select 
       distinct
       email, 
       max(case when accepts_marketing ilike '%TRUE%' or consent is not null then 'Y' else 'N' end) as m_and_c_flag
       from SEGMENT_EVENTS.SEED_COM.IDENTIFIES
where not (consent is null and accepts_marketing is null)
group by 1 
                       )
                       
, coupon_mapping as (
select * from "MARKETING_DATABASE"."GOOGLE_SHEETS"."COUPON_MAPPING"
                    )                             
    
    select s.recharge_subscription_id,
        s.recurly_subscription_id,
        s.customer_id,
        s.customer_email,
        s.first_name,
        s.last_name,
        upper(s.CURRENT_STATUS) as current_sub_status,
        s.activated_at,
        s.quantity,
        s.cancelled_at,
        s.sku,
        s.full_address as address,
        s.billing_country as country,			
        s.billing_CITY as city,			
        s.billing_STATE as state,	
        cancelled_primary_reason,
        cancelled_secondary_reason,
        reason_group,
        case when is_recharge_native = 1 then 'Recharge'
            else 'Recurly'
            end as origination_platform,
        cus_sub.first_subscription_date_ds01,
        cus_sub.first_subscription_date_pds08,
        cus_sub.first_subscription_date,
        date_trunc(month,cus_sub.first_subscription_date) as first_subscription_month,
        cus_sub.last_subscription_date_ds01,
        cus_sub.last_subscription_date_pds08,
        cus_sub.last_subscription_date,
        date_trunc(month,cus_sub.last_subscription_date) as last_subscription_month,
        cus_sub.first_cancel_date_ds01,
        cus_sub.first_cancel_date_pds08,
        cus_sub.first_cancel_date,
        cus_sub.last_cancel_date_ds01,
        cus_sub.last_cancel_date_pds08,
        cus_sub.last_cancel_date,
        o.first_order_date_ds01,
        o.first_order_date_pds08,
        o.first_order_date,
        o.first_product,
        o.first_sku,
        o.first_quantity,
        o.first_discount_percentage,
        o.first_discount_percentage_base_price,
        o.first_promotion_code,
        o.last_order_date_ds01,
        o.last_order_date_pds08,
        o.last_order_date,
        o.last_product,
        o.last_sku,
        o.last_quantity,
        o.last_discount_percentage,
        o.last_promotion_code,
        o.first_order_gross_revenue,
        o.first_order_total_base_price,
        o.first_order_total_discount,
        (datediff(month,activated_at,coalesce(cancelled_at,current_date()))+1) as months_active,
        cus_sub.months_active_customer,
        p.pause_start_date,
        p.pause_start_timestamp,
        p.pause_end_date,
        nb.next_bill_date,
        sku.first_enroll_date_srp_3mo,
        sku.last_enroll_date_srp_3mo,
        sku.first_enroll_date_srp_6mo,
        sku.last_enroll_date_srp_6mo,
        sku.SRP_3mo_ever_flag,
        sku.SRP_6mo_ever_flag,
        case when sku ilike '%3mo%' then 1 else 0 end as SRP_3mo_currently,
        case when sku ilike '%6mo%' then 1 else 0 end as SRP_6mo_currently,
        case when s.activated_at > first_cancel_date then 1
            else 0 end as reactivation_flag,
        ifnull(Is_Paused,'N') as Is_Paused,
        case when p.pause_start_date is not null then 'Y' else 'N' end as Has_Paused,
        case when current_sub_status = 'failed' then 'Y' else 'N' end as In_Payment_Failure,
        case when (current_sub_status in ('ACTIVE','PENDING','FAILED') and Is_Paused is null)  then 'Y' else 'N' end as Is_Active,
        months_active_customer as customer_age,
        cs.date as checkout_survey_date,
        cs.month_date as checkout_survey_month,
        cs.RESPONSE as checkout_survey_response,
        --lta.CHANNEL_PLATFORM, 
        --lta.CHANNEL_GROUPING, 
        --lta.channel_bucket,
        --lta.channel,
        lta.channel,
        lta.bucket,
        lta.agency,
        lta.spend_type,
        lta.platform,
        ifnull(lta.erewhon_flag,'N') as erewhon_flag,
        case when cm.channel = 'INFLUENCER' then 'Influencer' else lta.channel end as channel_modified,
        case when cm.channel = 'INFLUENCER' then 'KOL/Partner' else lta.bucket end as bucket_modified,
        case when cm.channel = 'INFLUENCER' then 'HQ' else lta.agency end as agency_modified,     
        case when cm.channel = 'INFLUENCER' then 'Media' else lta.SPEND_TYPE end as spend_type_modified,  
        case when cm.channel = 'INFLUENCER' then lta.platform end as platform_modified,  

        CONTACTED_MX_TEAM as HAS_CONTACTED_MX,
        CONTACTED_SCI_CARE as HAS_CONTACTED_SCI_CARE,
        CONTACTED_CARE as HAS_CONTACTED_CARE,
        ifnull(was_previously_paused,'N') as was_previously_paused,
        ifnull(consent_flag,'N') as consent_flag,
        ifnull (accepts_marketing_flag,'N') as accepts_marketing_flag,
        ifnull (M_and_C_flag,'N') as MARKETING_AND_CONSENT_FLAG,
        case when (Is_Active = 'Y' and (sku ilike '%3mo%' or sku ilike '%6mo%')) then 'Y' else 'N' end as Is_Active_SRP,
        datediff(day,cus_sub.first_subscription_date,coalesce(cus_sub.last_cancel_date,current_date()))+1 as days_from_first_subscription_date_last_cancel_date,
        datediff(day,pause_start_date,current_date) as days_since_last_pause_date,
        datediff(day,cus_sub.last_cancel_date,current_date())+1 as days_since_last_cancel_date
     
        from "SEED_DATA"."DEV"."V_SUBSCRIPTION" as s
        left join cancellation as cancel on s.recurly_subscription_id = cancel.recurly_subscription_id
        left join customer_sub_info as cus_sub on cus_sub.customer_id = s.customer_id
        left join orders as o on coalesce(s.recurly_subscription_id,'') = coalesce(o.recurly_subscription_id,'') and coalesce(s.recharge_subscription_id,'') = coalesce(o.recharge_subscription_id,'')
        left join pause as p on s.recurly_subscription_id = p.subscription_id
        left join next_bill as nb on s.recurly_subscription_id = nb.subscription_id
        left join sku_history as sku on sku.subscription_id = s.recurly_subscription_id
        left join co_survey as cs on upper(s.customer_email) = upper(cs.email)
        left join lta on coalesce(s.recurly_subscription_id,'') = coalesce(lta.recurly_subscription_id,'') and coalesce(s.recharge_subscription_id,'') = coalesce(lta.recharge_subscription_id,'')
        left join MX_TEAM as cmt on cmt.customer_id = s.customer_id
        left join prev_pause as pp on s.recurly_subscription_id = pp.subscription_uuid
        left join marketing_consent as mc on s.customer_email = mc.email
        left join m_and_c_consent as mcc on s.customer_email = mcc.email
        left join accepts_marketing as am on s.customer_email = am.email
        left join coupon_mapping as cm on upper(o.first_promotion_code) = upper(cm.coupon_code)
        ;