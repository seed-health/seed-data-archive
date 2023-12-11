create or replace view SEED_DATA.DEV.V_Subscription_reactivation as 

    with subscription_reactivation as 
    (select subscription_uuid,USER_SUBSCRIPTION_ID,
        was_previously_paused,
        row_number() over(partition by user_subscription_id order by activated_at) as sub_rank,
        case when sub_rank > 1 then 'Subscription Reactivation' else 'New Subscription' end as Reactivation_flag
    from marketing_database.seed_core_public.seed_ecommerce_subscription 
    )
    -- This data is only for users from recurly
    
    ,sub_master_reactivation as 
    (
    select sm.customer_email,sm.first_name,sm.last_name,USER_SUBSCRIPTION_ID,
        max(case when sub_rank = 1 then recurly_subscription_id end) as first_sub_subscription_id,
        max(case when sub_rank = 1 then activated_at end) as first_sub_activated_at,
        max(case when sub_rank = 1 then cancelled_at end) as first_sub_cancelled_at,
        max(case when sub_rank = 1 then first_product end) as first_sub_product,
        max(case when sub_rank = 1 then FIRST_DISCOUNT_PERCENTAGE_BASE_PRICE end) as first_sub_discount_percentage,
        max(case when sub_rank = 1 then FIRST_PROMOTION_CODE end) as first_sub_promo_code,
        max(case when sub_rank = 1 then checkout_survey_response end) as first_sub_checkout_survey,
        max(case when sub_rank = 1 then LAST_SKU end) as first_sub_canceled_sku,
        max(case when sub_rank = 1 then CANCELLED_PRIMARY_REASON end) as first_sub_canceled_primary_reason,
        max(case when sub_rank = 1 then CANCELLED_SECONDARY_REASON end) as first_sub_canceled_secondary_reason,
        max(case when sub_rank = 1 then reason_group end) as first_sub_canceled_reason_group,
        max(case when sub_rank = 2 then recurly_subscription_id end) as first_reactivated_sub_subscription_id,
        max(case when sub_rank = 2 then activated_at end) as first_reactivated_sub_activated_at,
        max(case when sub_rank = 2 then cancelled_at end) as first_reactivated_sub_cancelled_at,
        max(case when sub_rank = 2 then first_product end) as first_reactivated_sub_product,
        max(case when sub_rank = 2 then FIRST_DISCOUNT_PERCENTAGE_BASE_PRICE end) as first_reactivated_sub_discount_percentage,
        max(case when sub_rank = 2 then FIRST_PROMOTION_CODE end) as first_reactivated_sub_promo_code,
        max(case when sub_rank = 2 then checkout_survey_response end) as first_reactivated_sub_checkout_survey,
        max(case when sub_rank = 2 then first_sku end) as first_reactivated_sub_activated_sku,
        max(case when sub_rank = 2 then CANCELLED_PRIMARY_REASON end) as first_reactivated_sub_canceled_primary_reason,
        max(case when sub_rank = 2 then CANCELLED_SECONDARY_REASON end) as first_reactivated_sub_canceled_secondary_reason,
        max(case when sub_rank = 2 then reason_group end) as first_reactivated_sub_canceled_reason_group
    from seed_data.dev.subscription_master as sm 
        left join subscription_reactivation as sr 
        on sm.recurly_subscription_id = sr.subscription_uuid
    where recurly_subscription_id is not null
    group by 1,2,3,4
    ),
    
    active_sub_cancel as 
    (
        select sm.customer_email,
            max(case when to_date(activated_at) < to_date(first_sub_cancelled_at) and (cancelled_at is null or to_date(cancelled_at) > to_date(first_sub_cancelled_at)) then 1 else 0 end) as active_sub_at_cancellation_flag
        from seed_data.dev.subscription_master as sm
        left join sub_master_reactivation as re 
        on sm.customer_email = re.customer_email
        where re.first_sub_cancelled_at is not null
        group by 1
        having active_sub_at_cancellation_flag = 1
    ),

    final_raw_data as 
    (
    
    select smr.*,case when (to_date(first_sub_cancelled_at) = '2023-09-06' and sr.was_previously_paused = True) then True else False end as was_previously_paused,
    datediff(day,first_sub_cancelled_at,first_reactivated_sub_activated_at) as first_reactivated_sub_days_since_cancel,
    floor(datediff(day,first_sub_cancelled_at,first_reactivated_sub_activated_at)/30) as first_reactivated_sub_cycle_since_cancel,
    coalesce(asca.ACTIVE_SUB_AT_CANCELLATION_FLAG,0) as active_sub_at_cancel_flag
    from sub_master_reactivation as smr
    left join active_sub_cancel as asca on smr.customer_email = asca.customer_email
    left join subscription_reactivation as sr on smr.first_sub_subscription_id = sr.subscription_uuid
)


    --- Need to clean this cohrt or exclude them

  /*  select to_date(first_sub_cancelled_at),count(*)
    from SEED_DATA.DEV.V_Subscription_reactivation 
    where to_date(first_sub_cancelled_at) >= '2023-09-01'
    group by 1 
    order by 1

select *  
from SEED_DATA.DEV.V_Subscription_reactivation
where ((first_reactivated_sub_days_since_cancel >= 0) or (first_reactivated_sub_days_since_cancel is null)) and first_sub_cancelled_at is not null and date_trunc("month",to_date(first_sub_cancelled_at)) = '2023-09-01' and was_previously_paused = False
*/
    
select date_trunc("month",to_date(first_sub_cancelled_at)) as cancelled_month_year,
    was_previously_paused,
    first_sub_product,
    first_reactivated_sub_product,
    first_sub_canceled_reason_group,
    first_sub_discount_percentage,
    first_reactivated_sub_discount_percentage,
    first_sub_canceled_sku,
    first_reactivated_sub_activated_sku,
    first_sub_checkout_survey,
    first_reactivated_sub_checkout_survey,
    floor(datediff('days',to_date(first_sub_activated_at),to_date(first_sub_cancelled_at))/30) as customer_age_months,
    first_reactivated_sub_cycle_since_cancel as reactivated_cycle,
    count(distinct user_subscription_id) as churn_subscrption_count,
    count(distinct case when first_reactivated_sub_cycle_since_cancel = 0 then user_subscription_id end) as winback_0_30_days,
    count(distinct case when first_reactivated_sub_cycle_since_cancel = 1 then user_subscription_id end) as winback_30_60_days,
    count(distinct case when first_reactivated_sub_cycle_since_cancel = 2 then user_subscription_id end) as winback_60_90_days,
    count(distinct case when first_reactivated_sub_cycle_since_cancel = 3 then user_subscription_id end) as winback_90_120_days,
    count(distinct case when first_reactivated_sub_cycle_since_cancel = 4 then user_subscription_id end) as winback_120_150_days,
    count(distinct case when first_reactivated_sub_cycle_since_cancel = 5 then user_subscription_id end) as winback_150_180_days,
    count(distinct case when first_reactivated_sub_cycle_since_cancel = 6 then user_subscription_id end) as winback_180_210_days,
    count(distinct case when first_reactivated_sub_cycle_since_cancel = 7 then user_subscription_id end) as winback_210_240_days,
    count(distinct case when first_reactivated_sub_cycle_since_cancel = 8 then user_subscription_id end) as winback_240_270_days,
    count(distinct case when first_reactivated_sub_cycle_since_cancel = 9 then user_subscription_id end) as winback_270_300_days,
    count(distinct case when first_reactivated_sub_cycle_since_cancel = 10 then user_subscription_id end) as winback_300_330_days,
    count(distinct case when first_reactivated_sub_cycle_since_cancel = 11 then user_subscription_id end) as winback_330_360_days,
    count(distinct case when first_reactivated_sub_cycle_since_cancel is not null then user_subscription_id end) as winback_ever

from final_raw_data
where ((first_reactivated_sub_days_since_cancel >= 0) or (first_reactivated_sub_days_since_cancel is null)) and first_sub_cancelled_at is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
order by 1,2 desc