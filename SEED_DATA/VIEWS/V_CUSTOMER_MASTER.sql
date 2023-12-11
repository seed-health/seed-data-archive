create or replace view SEED_DATA.DEV.V_CUSTOMER_MASTER as 
with customer as 
(select
        customer_id,
        UPPER(customer_email) as customer_email,
        first_name,
        last_name,
        Full_address as Address,
        billing_country as Country,			
        billing_CITY as City,			
        billing_STATE as State	
        from 
        "SEED_DATA"."DEV"."SUBSCRIPTION"
 
 )
 
 ,accounts as (
   select
         to_varchar(ACCOUNT_CODE) as customer_id,
         Case when ACCOUNT_STATUS_OPEN = 'TRUE' then 'Y' else 'N' end as HAS_OPEN_ACCOUNT,
         Case when ACCOUNT_STATUS_ACTIVE_SUBSCRIBERS = 'TRUE' then 'Y' else 'N' end as HAS_ACTIVE_SUB,
         Case when ACCOUNT_STATUS_PAST_DUE = 'TRUE' then 'Y' else 'N' end as HAS_SUB_IN_PAY_FAIL
   from   "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ACCOUNTS"     
 
 )
 
, orders as 
    (
        with orders_clean as 
        (
            select 
                
                Customer_id, 
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
                row_number() over(partition by customer_id,product order by invoice_date,sku_ranking) as first_invoice_product_rank,
                row_number() over (partition by customer_id,product order by invoice_date desc ,sku_ranking desc) as last_invoice_product_rank,
                row_number() over (partition by customer_id order by invoice_date,sku_ranking ) as first_invoice_rank,
                row_number() over (partition by customer_id order by invoice_date desc, sku_ranking desc) as last_invoice_rank,
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
        )
    
            select
                customer_id, 
                min(first_order_date) as first_order_date,
                min(first_sku) as first_sku,
                min(first_discount_percentage) as first_discount_percentage,
                min(first_promotion_code) as first_promotion_code,
                max(last_order_date) as last_order_date,
                max(last_sku) as last_sku,
                max(last_discount_percentage) as last_discount_percentage,
                max(last_promotion_code) as last_promotion_code
            from orders_clean    
            group by customer_id
                    
      )
       
    , customer_sub_info as 
    (
        with subscription_product as 
        (
            select customer_id, 
                   activated_at,
                   cancelled_at,
                   datediff(month,min(activated_at),coalesce(max(cancelled_at),current_date()))+1 as months_active
        from "SEED_DATA"."DEV"."SUBSCRIPTION"
          group by 1,2,3
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
     ,
    
    pause as 
    (
        select customer_id,
            paused_at_clean as pause_start_date, 
            paused_at_clean_ts as pause_start_timestamp,
            version_ended_at_clean as pause_end_date,
            case when pause_end_date is null then 'Y' else 'N' end as Is_Paused,
            row_number() over(partition by customer_id order by version_started_at desc) as pause_rank
        from "SEED_DATA"."DEV"."V_SUBSCRIPTION_PAUSE_HISTORY" sph
       left join "SEED_DATA"."DEV"."SUBSCRIPTION" s on s.recurly_subscription_id = sph.subscription_uuid
        qualify pause_rank = 1
    )
   
    ,next_bill as 
    (
        select customer_id, min(next_bill_date) as next_bill_date
        from "SEED_DATA"."DEV"."SUBSCRIPTION_NEXT_BILL_DATE" nbd
        left join "SEED_DATA"."DEV"."SUBSCRIPTION" s on s.recurly_subscription_id = nbd.subscription_id
        group by 1
    ),
    
    sku_history as 
    (
        select CUSTOMER_ID, 
            min(case when (updated_plan_code ilike '%3mo%' or updated_plan_code ilike '%6mo%') then version_started_at else null end) as first_enroll_SRP_Date,
            max(case when (updated_plan_code ilike '%3mo%' or updated_plan_code ilike '%6mo%') then version_started_at else null end) as last_enroll_date_srp,
            case when first_enroll_srp_date is not null then 'Y' else 'N' end as Has_Enrolled_In_SRP

        from "SEED_DATA"."DEV"."SUBSCRIPTION_STATUS_HISTORY" nbd
        left join "SEED_DATA"."DEV"."SUBSCRIPTION" s on s.recurly_subscription_id = nbd.subscription_uuid
        group by 1
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

-------------
 ,MX_TEAM as (
    select Customer_id, 
           CONTACTED_MX_TEAM as has_contacted_mx
           from SEED_DATA.DEV.V_KUSTOMER_SUBSCRIBER_DETAIL
             )
      
       ----------------------Customer was moved from Pause to Cancel when we removed pause as an option
  ,prev_pause as (
    select
	sau.email,
	case when ses.was_previously_paused = 'TRUE' then 'Y' else 'N' end as was_previously_paused
from MARKETING_DATABASE.SEED_CORE_PUBLIC.seed_ecommerce_subscription ses
	inner join MARKETING_DATABASE.SEED_CORE_PUBLIC.seed_ecommerce_gatewayaccount seg 
		on ses.account_id = seg.id 
	inner join MARKETING_DATABASE.SEED_CORE_PUBLIC.seed_account_user sau 
		on sau.id = seg.user_id 
where ses.was_previously_paused)


------------------------
, marketing_consent as (

select email, 
       max(case when accepts_marketing ilike '%TRUE%' or consent is not null then 'Y' else 'N' end) as consent_flag
       from SEGMENT_EVENTS.SEED_COM.IDENTIFIES
where not (consent is null and accepts_marketing is null)
group by 1 
         )
    
    select 

        s.customer_id,
        s.customer_email,
        s.first_name,
        s.last_name,
        s.Address,
        s.Country,
        s.City,
        s.State,
        a.HAS_OPEN_ACCOUNT,
        a.HAS_ACTIVE_SUB,
        a.HAS_SUB_IN_PAY_FAIL,
        o.first_order_date,
        o.first_sku,
        o.first_discount_percentage,
        o.first_promotion_code,
        o.last_order_date,
        o.last_sku,
        o.last_discount_percentage,
        o.last_promotion_code,
        cus_sub.first_subscription_date,
        date_trunc(month,cus_sub.first_subscription_date) as first_subscription_month,
        cus_sub.last_subscription_date,
        date_trunc(month,cus_sub.last_subscription_date) as last_subscription_month,
        case when first_cancel_date is not null then 'Y' else 'N' end as Has_Cancelled_a_sub,
        nb.next_bill_date,
        sh.Has_Enrolled_In_SRP,
        sh.first_enroll_SRP_Date,
        sh.last_enroll_date_srp,
        months_active_customer as customer_age,
        cs.date as checkout_survey_date,
        cs.month_date as checkout_survey_month,
        cs.RESPONSE as checkout_survey_response,
        ifnull(HAS_CONTACTED_MX,'N') as HAS_CONTACTED_MX,
        ifnull(IS_PAUSED,'N') as Has_Paused_Sub,
        ifnull(was_previously_paused,'N') as was_previously_paused,
        ifnull(consent_flag,'N') as consent_flag,
        datediff(day,cus_sub.first_subscription_date,coalesce(cus_sub.last_cancel_date,current_date()))+1 as days_from_first_subscription_date_last_cancel_date,
        datediff(day,pause_start_date,coalesce(p.pause_end_date,current_date()))+1 as days_since_last_pause_date,
        datediff(day,cus_sub.last_cancel_date,current_date())+1 as days_since_last_cancel_date
     
    from customer s
        left join accounts as a on a.customer_id = s.customer_id
        left join customer_sub_info as cus_sub on cus_sub.customer_id = s.customer_id
        left join orders as o on o.customer_id = s.customer_id
        left join pause as p on p.customer_id = s.customer_id
        left join next_bill as nb on nb.customer_id = s.customer_id
        left join sku_history as sh on sh.customer_id = s.customer_id
        left join co_survey as cs on s.customer_email = cs.email
        left join MX_TEAM as cmt on cmt.customer_id = s.customer_id
        left join prev_pause as pp on upper(s.customer_email) = upper(pp.email)
        left join marketing_consent as mc on upper(s.customer_email) = upper(mc.email)