create or replace view SEED_DATA.DEV.V_PAUSED_SUBSCRIBER_ANALYSIS as 

with pause_rank as 
(
select sub.recurly_subscription_id as subscription_uuid,
    CUSTOMER_ID,
    to_date(sub.activated_at) as activation_date,
    sub.first_product as product,
    sub.first_discount_percentage as first_discount_percentage,
    to_date(sub.cancelled_at) as cancelled_date, 
    p.version_started_at as pause_start_date,
    p.version_ended_at_clean as pause_end_date,
    datediff(month,pause_start_date,pause_end_date),
    datediff(day,pause_start_date,pause_end_date),
    ceil(datediff(day,pause_start_date,pause_end_date)/30,0) as pause_period,
    row_number() over(partition by subscription_uuid order by version_started_at) as pause_number,
    date_trunc(year,activation_date) as activation_year,
    date_trunc(month,activation_date) as activation_month_year,
    date_trunc(quarter,activation_date) as activation_quarter,
    case when activation_year = '2018-01-01' then '2018'
        when activation_year = '2019-01-01' then '2019'
        when activation_year = '2020-01-01' then '2020'
        when activation_year = '2021-01-01' and activation_quarter = '2021-01-01' then '2021 Q1'
        when activation_year = '2021-01-01' and activation_quarter = '2021-04-01' then '2021 Q2'
        when activation_year = '2021-01-01' and activation_quarter = '2021-07-01' then '2021 Q3'
        when activation_year = '2021-01-01' and activation_quarter = '2021-10-01' then '2021 Q4'
        when activation_year = '2022-01-01' and activation_quarter = '2022-01-01' then '2022 Q1'
        when activation_year = '2022-01-01' and activation_quarter = '2022-04-01' then '2022 Q2'
        when activation_year = '2022-01-01' and activation_quarter = '2022-07-01' then '2022 Q3'
        when activation_year = '2022-01-01' and activation_quarter = '2022-10-01' then '2022 Q4'
        when activation_year = '2023-01-01' and activation_month_year = '2023-01-01' then '2023-01'
        when activation_year = '2023-01-01' and activation_month_year = '2023-02-01' then '2023-02'
        when activation_year = '2023-01-01' and activation_month_year = '2023-03-01' then '2023-03'
        when activation_year = '2023-01-01' and activation_month_year = '2023-04-01' then '2023-04'
        when activation_year = '2023-01-01' and activation_month_year = '2023-05-01' then '2023-05'
        when activation_year = '2023-01-01' and activation_month_year = '2023-06-01' then '2023-06'
        when activation_year = '2023-01-01' and activation_month_year = '2023-07-01' then '2023-07'
        when activation_year = '2023-01-01' and activation_month_year = '2023-08-01' then '2023-08' end as activation_flag,
        case when first_discount_percentage = 0 then '0'
            when first_discount_percentage = 10 then '10'
            when first_discount_percentage > 10 and first_discount_percentage <= 15 then '11-15'
            when first_discount_percentage > 15 and first_discount_percentage <= 20 then '16-20'
            when first_discount_percentage > 20 and first_discount_percentage <= 25 then '21-25'
            when first_discount_percentage > 25 and first_discount_percentage <= 30 then '26-30'
            when first_discount_percentage > 30 and first_discount_percentage <= 40 then '31-40'
            when first_discount_percentage > 40 and first_discount_percentage <= 50 then '41-50'
            when first_discount_percentage > 50 then '50+' end as discount_category,
            channel_grouping,
            channel_platform,
            checkout_survey_response,
            was_previously_paused as pause_converted_to_cancel
from seed_data.dev.v_subscription_master as sub
     join SEED_DATA.DEV.V_SUBSCRIPTION_PAUSE_HISTORY as p on sub.recurly_subscription_id = p.subscription_uuid
qualify pause_number = 1 -- Only keeping their first pause instance
--where p.version_started_at >= '2023-03-01' --and p.version_started_At < '2023-09-01'
)

, activity as 
(
     with pause_no_cancel_activity as 
        ( with pause_no_cancel_cohort as 
            (
                select *,
                    row_number() over (partition by subscription_id order by invoice_date) as invoice_rank,
                    floor((datediff(day,to_date(p.pause_start_Date),to_date(invoice_date)))/30) as order_since_pause_days
                from pause_rank as p
                    left join SEED_DATA.DEV.V_ORDER_HISTORY as o on p.subscription_uuid = o.subscription_id
                where invoice_date >= p.pause_start_date
            
            )
            select subscription_uuid, 
                max(case when order_since_pause_days = 0 and invoice_rank = 1 then 1 else 0 end) as first_rebill_days_0to30_resume,
                max(case when order_since_pause_days = 1 and invoice_rank = 1 then 1 else 0 end) as first_rebill_days_31to60_resume,
                max(case when order_since_pause_days = 2 and invoice_rank = 1 then 1 else 0 end) as first_rebill_days_61to90_resume,
                max(case when order_since_pause_days = 3 and invoice_rank = 1 then 1 else 0 end) as first_rebill_days_91to120_resume,
                max(case when order_since_pause_days = 4 and invoice_rank = 1 then 1 else 0 end) as first_rebill_days_121to150_resume,
                max(case when order_since_pause_days = 5 and invoice_rank = 1 then 1 else 0 end) as first_rebill_days_151to180_resume,
                max(case when order_since_pause_days = 0 then 1 else 0 end) as rebill_days_0to30_resume,
                max(case when order_since_pause_days = 1 then 1 else 0 end) as rebill_days_31to60_resume,
                max(case when order_since_pause_days = 2 then 1 else 0 end) as rebill_days_61to90_resume,
                max(case when order_since_pause_days = 3 then 1 else 0 end) as rebill_days_91to120_resume,
                max(case when order_since_pause_days = 4 then 1 else 0 end) as rebill_days_121to150_resume,
                max(case when order_since_pause_days = 5 then 1 else 0 end) as rebill_days_151to180_resume

            from pause_no_cancel_cohort
            group by 1
         )
    ,pause_cancel_activity as 
        (
            with pause_cancel_cohort as 
            (
                select *,p.subscription_uuid as sub_id,
                    row_number() over (partition by subscription_id order by invoice_date) as invoice_rank,
                    floor((datediff(day,to_date(p.pause_start_Date),to_date(invoice_date)))/30) as order_since_pause_days
                from pause_rank as p
                    left join SEED_DATA.DEV.V_ORDER_HISTORY as o on p.customer_id = o.customer_id
                    left join seed_data.dev.v_subscription_master as s on o.subscription_id = s.RECURLY_SUBSCRIPTION_ID
                where invoice_date >= p.cancelled_date and s.reactivation_flag = 1
            )

            select sub_id, 
                max(case when order_since_pause_days = 0 and invoice_rank = 1 then 1 else 0 end) as first_rebill_days_0to30_Reactivate,
                max(case when order_since_pause_days = 1 and invoice_rank = 1 then 1 else 0 end) as first_rebill_days_31to60_Reactivate,
                max(case when order_since_pause_days = 2 and invoice_rank = 1 then 1 else 0 end) as first_rebill_days_61to90_Reactivate,
                max(case when order_since_pause_days = 3 and invoice_rank = 1 then 1 else 0 end) as first_rebill_days_91to120_Reactivate,
                max(case when order_since_pause_days = 4 and invoice_rank = 1 then 1 else 0 end) as first_rebill_days_121to150_Reactivate,
                max(case when order_since_pause_days = 5 and invoice_rank = 1 then 1 else 0 end) as first_rebill_days_151to180_Reactivate,
                max(case when order_since_pause_days = 0 then 1 else 0 end) as rebill_days_0to30_Reactivate,
                max(case when order_since_pause_days = 1 then 1 else 0 end) as rebill_days_31to60_Reactivate,
                max(case when order_since_pause_days = 2 then 1 else 0 end) as rebill_days_61to90_Reactivate,
                max(case when order_since_pause_days = 3 then 1 else 0 end) as rebill_days_91to120_Reactivate,
                max(case when order_since_pause_days = 4 then 1 else 0 end) as rebill_days_121to150_Reactivate,
                max(case when order_since_pause_days = 5 then 1 else 0 end) as rebill_days_151to180_Reactivate
            from pause_cancel_cohort
            group by 1
        )
     
        select *, 
             greatest(coalesce(first_rebill_days_0to30_resume,0),coalesce(first_rebill_days_0to30_Reactivate,0)) as first_rebill_days_0to30,
             greatest(coalesce(first_rebill_days_31to60_resume,0),coalesce(first_rebill_days_31to60_Reactivate,0)) as first_rebill_days_31to60,
             greatest(coalesce(first_rebill_days_61to90_resume,0),coalesce(first_rebill_days_61to90_Reactivate,0)) as first_rebill_days_61to90,
             greatest(coalesce(first_rebill_days_91to120_resume,0),coalesce(first_rebill_days_91to120_Reactivate,0)) as first_rebill_days_91to120,
             greatest(coalesce(first_rebill_days_121to150_resume,0),coalesce(first_rebill_days_121to150_Reactivate,0)) as first_rebill_days_121to150,
             greatest(coalesce(first_rebill_days_151to180_resume,0),coalesce(first_rebill_days_151to180_Reactivate,0)) as first_rebill_days_151to180,
             greatest(coalesce(rebill_days_0to30_resume,0),coalesce(rebill_days_0to30_Reactivate,0)) as rebill_days_0to30,
             greatest(coalesce(rebill_days_31to60_resume,0),coalesce(rebill_days_31to60_Reactivate,0)) as rebill_days_31to60,
             greatest(coalesce(rebill_days_61to90_resume,0),coalesce(rebill_days_61to90_Reactivate,0)) as rebill_days_61to90,
             greatest(coalesce(rebill_days_91to120_resume,0),coalesce(rebill_days_91to120_Reactivate,0)) as rebill_days_91to120,
             greatest(coalesce(rebill_days_121to150_resume,0),coalesce(rebill_days_121to150_Reactivate,0)) as rebill_days_121to150,
             greatest(coalesce(rebill_days_151to180_resume,0),coalesce(rebill_days_151to180_Reactivate,0)) as rebill_days_151to180
             
        from pause_cancel_activity as pc
             full join pause_no_cancel_activity as pnc on pc.sub_id = pnc.subscription_uuid
     

)   

select p.*,
    a.first_rebill_days_0to30_resume, a.first_rebill_days_31to60_resume, a.first_rebill_days_61to90_resume,
    a.first_rebill_days_91to120_resume, a.first_rebill_days_121to150_resume, a.first_rebill_days_151to180_resume,
    a.rebill_days_0to30_resume, a.rebill_days_31to60_resume,a.rebill_days_61to90_resume,
    a.rebill_days_91to120_resume, a.rebill_days_121to150_resume, a.rebill_days_151to180_resume,
    a.first_rebill_days_0to30_Reactivate, a.first_rebill_days_31to60_Reactivate, a.first_rebill_days_61to90_Reactivate,
    a.first_rebill_days_91to120_Reactivate, a.first_rebill_days_121to150_Reactivate, a.first_rebill_days_151to180_Reactivate,
    a.rebill_days_0to30_Reactivate, a.rebill_days_31to60_Reactivate,a.rebill_days_61to90_Reactivate,
    a.rebill_days_91to120_Reactivate, a.rebill_days_121to150_Reactivate, a.rebill_days_151to180_Reactivate,
    a.first_rebill_days_0to30, a.first_rebill_days_31to60, a.first_rebill_days_61to90,
    a.first_rebill_days_91to120, a.first_rebill_days_121to150, a.first_rebill_days_151to180,
    a.rebill_days_0to30, a.rebill_days_31to60,a.rebill_days_61to90,
    a.rebill_days_91to120, a.rebill_days_121to150, a.rebill_days_151to180
from pause_rank  as p
    left join activity as a on p.subscription_uuid = a.subscription_uuid
        

