create or replace view SEED_DATA.DEV.V_CUSTOMER_ATTRIBUTES as 
with customer as 
(select
        recharge_subscription_id,
        recurly_subscription_id,
        customer_id,
        UPPER(customer_email) as customer_email,
        first_name,
        last_name
        from 
        "SEED_DATA"."DEV"."SUBSCRIPTION"
 )
   
, orders as 
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
                case when first_invoice_rank = 1 then invoice_date 
                    else null end as first_order_date,
                case when last_invoice_rank = 1 then invoice_date 
                    else null end as last_order_date,
                case when first_invoice_rank = 1 then clean_sku
                    else null end as first_sku,
                case when first_invoice_rank = 1 then 
                round(div0((discount*100),(total_amount_paid - tax - total_shipping_cost + discount)),0) else null end as first_discount_percentage,
                ----- adding as part of a test for base price discount
                case when first_invoice_rank = 1 then 
                round(div0((discount*100),(base_price)),0) else null end as first_discount_percentage_base_price,
                case when first_invoice_rank = 1 then promotion_code end as first_promotion_code,
                case when last_invoice_rank = 1 then clean_sku
                    else null end as last_sku,
                case when last_invoice_rank = 1 then 
                round(div0((discount*100),(total_amount_paid - tax - total_shipping_cost +discount)),0)                       else null end as last_discount_percentage,
                case when last_invoice_rank = 1 then promotion_code end as last_promotion_code
            from "SEED_DATA"."DEV"."ORDER_HISTORY" as o 
            where subscription_id is not null
        ),
    
        order_summary as
        (
            select
                subscription_id, 
                min(first_order_date) as first_order_date,
                min(first_sku) as first_sku,
                min(first_discount_percentage) as first_discount_percentage,
                min(first_promotion_code) as first_promotion_code,
                max(last_order_date) as last_order_date,
                max(last_sku) as last_sku,
                max(last_discount_percentage) as last_discount_percentage,
                max(last_promotion_code) as last_promotion_code
            from orders_clean    
            group by subscription_id
        )

        
    
        select recharge_subscription_id,
            recurly_subscription_id,
            min(coalesce(order_recharge.first_order_date,order_recurly.first_order_date)) as first_order_date,
            min(coalesce(order_recharge.first_sku,order_recurly.first_sku)) as first_sku,
            min(coalesce(order_recharge.first_discount_percentage,order_recurly.first_discount_percentage)) as first_discount_percentage,
            min(coalesce(order_recharge.first_promotion_code,order_recurly.first_promotion_code)) as first_promotion_code,
            max(coalesce(order_recurly.last_order_date,order_recharge.last_order_date)) as last_order_date,
            max(coalesce(order_recurly.last_sku,order_recharge.last_sku)) as last_sku,
            max(coalesce(order_recurly.last_discount_percentage,order_recharge.last_discount_percentage)) as last_discount_percentage,
            max(coalesce(order_recurly.last_promotion_code,order_recharge.last_promotion_code)) as last_promotion_code
        from "SEED_DATA"."DEV"."SUBSCRIPTION" as s 
            left join order_summary as order_recharge on order_recharge.subscription_id = s.recharge_subscription_id
            left join order_summary as order_recurly on order_recurly.subscription_id = s.recurly_subscription_id
        group by recharge_subscription_id,recurly_subscription_id
       
      )
       
    , customer_sub_info as 
    (
        with subscription_product as 
        (
            select customer_id, 
                   activated_at,
                   cancelled_at,
                   datediff(month,activated_at,coalesce(cancelled_at,current_date()))+1 as months_active
        from "SEED_DATA"."DEV"."SUBSCRIPTION"
        )

    select customer_id,
        min(activated_at) as first_subscription_date,
        max(activated_at) as last_subscription_date,
        min(cancelled_at) as first_cancel_date,
        max(cancelled_at) as last_cancel_date,
        sum(months_active) as months_active_customer -- overlaps are considered as multiple 
    from subscription_product
    group by customer_id
    )
     
   ,pause as 
    (
        select subscription_uuid as subscription_id,
            version_started_at as Last_pause_start_date, 
            version_ended_at_clean as Last_pause_end_date,
            row_number() over(partition by subscription_uuid order by version_started_at desc) as pause_rank
        from "SEED_DATA"."DEV"."SUBSCRIPTION_PAUSE_HISTORY"
        qualify pause_rank = 1 
    
    ),
    
    next_bill as 
    (
        select subscription_id, next_bill_date
        from "SEED_DATA"."DEV"."SUBSCRIPTION_NEXT_BILL_DATE"
    ),
    
    sku_history as 
    (
        select subscription_uuid as subscription_id, 
            min(case when (updated_plan_code ilike '%3mo%' or updated_plan_code ilike '%6mo%') then version_started_at else null end) as first_enroll_SRP_Date,
            max(case when (updated_plan_code ilike '%3mo%' or updated_plan_code ilike '%6mo%') then version_started_at else null end) as last_enroll_date_srp,
            case when first_enroll_srp_date is not null then 'Y' else 'N' end as Has_Enrolled_SRP

        from "SEED_DATA"."DEV"."SUBSCRIPTION_STATUS_HISTORY"
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
       select recurly_subscription_id,
              recharge_subscription_id,
              CHANNEL_PLATFORM, 
              CHANNEL_GROUPING 
              from 
              SEED_DATA.DEV.V_SUBSCRIPTION_ACTIVATION_LTA_DETAIL      
     )
        

    
    select 
        s.recharge_subscription_id,
        s.recurly_subscription_id,
        s.customer_id,
        s.customer_email,
        s.first_name,
        s.last_name,
        cus_sub.first_subscription_date,
        date_trunc(month,cus_sub.first_subscription_date) as first_subscription_month,
        cus_sub.last_subscription_date,
        date_trunc(month,cus_sub.last_subscription_date) as last_subscription_month,
        cus_sub.first_cancel_date,
        cus_sub.last_cancel_date,
        case when cus_sub.first_cancel_date is not null then 'Y' else 'N' end as Has_Cancelled,
        o.first_order_date,
        o.first_sku,
        o.first_discount_percentage,
        o.first_promotion_code,
        o.last_order_date,
        o.last_sku,
        o.last_discount_percentage,
        o.last_promotion_code,
        p.Last_pause_start_date,
        case when p.Last_pause_start_date is not null then 'Y' else 'N' end as Has_Paused,
        date_trunc(month, p.Last_pause_start_date) as Last_pause_month,
        p.Last_pause_end_date,
        case when (Has_Cancelled = 'Y' or has_paused = 'Y') then 'Y' else 'N' end as Has_Paused_or_Cancelled,
        first_enroll_SRP_Date,
        last_enroll_date_srp,
        Has_Enrolled_SRP,
        nb.next_bill_date,
        date_trunc(month,nb.next_bill_date) as next_bill_month,
        months_active_customer as customer_age,
        cs.date as checkout_survey_date,
        cs.month_date as checkout_survey_month,
        cs.RESPONSE as checkout_survey_response,
        lta.CHANNEL_PLATFORM, 
        lta.CHANNEL_GROUPING, 
        datediff(day,cus_sub.first_subscription_date,coalesce(cus_sub.last_cancel_date,current_date()))+1 as days_from_first_subscription_date_last_cancel_date,
        datediff(day,Last_pause_start_date,coalesce(p.Last_pause_end_date,current_date()))+1 as days_since_last_pause_date,
        datediff(day,cus_sub.last_cancel_date,current_date())+1 as days_since_last_cancel_date
        
    from customer s
        left join customer_sub_info as cus_sub on cus_sub.customer_id = s.customer_id
        left join orders as o on coalesce(s.recurly_subscription_id,'') = coalesce(o.recurly_subscription_id,'') and coalesce(s.recharge_subscription_id,'') = coalesce(o.recharge_subscription_id,'')
        left join pause as p on s.recurly_subscription_id = p.subscription_id
        left join next_bill as nb on s.recurly_subscription_id = nb.subscription_id
        left join sku_history as sku on sku.subscription_id = s.recurly_subscription_id
        left join co_survey as cs on s.customer_email = cs.email
        left join lta on coalesce(s.recurly_subscription_id,'') = coalesce(lta.recurly_subscription_id,'') and coalesce(s.recharge_subscription_id,'') = coalesce(lta.recharge_subscription_id,'')
       -- where s.recurly_subscription_id = '6bbccb88514f948d0250cb46f1b8eeb4'
        
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37