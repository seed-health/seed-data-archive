create or replace view SEED_DATA.DEV.V_PAUSED_CANCELED_SUBSCRIBER_ANALYSIS as 

with pause as 
(
select 
    subscription_uuid,
    version_started_at_ts as event_date,
    'paused' as flag,
    row_number() over(partition by subscription_uuid order by version_started_at) as pause_number
    from SEED_DATA.DEV.SUBSCRIPTION_PAUSE_HISTORY
qualify pause_number = 1
)

,cancel as 
(
 select recurly_subscription_id as subscription_id,
     cancelled_at as event_date,
     case when was_previously_paused = 'Y' then 'paused_cancel'
         else 'cancel' end as flag
 from seed_data.dev.subscription_master
 where cancelled_at is not null and recurly_subscription_id is not null --and reactivation_flag = 0
)

,pause_and_cancel as 
(
select coalesce(subscription_id,subscription_uuid) as subscription_id,
    max(case when pause.event_date is null then cancel.event_date
        when cancel.event_date is null then pause.event_date
        when pause.event_date <= cancel.event_date then pause.event_date
        else cancel.event_date
        end) as event_date,
    max(case when pause.flag is null and cancel.flag = 'cancel' and to_date(cancel.event_date) >= '2023-09-01' then 'cancel_after_pause_was_decontinued'
        when pause.flag is null and cancel.flag = 'cancel' and to_date(cancel.event_date) >= '2023-03-01' and to_date(cancel.event_date) < '2023-09-01' then 'cancel_after_pause_was_introduced'
        when pause.flag is null and cancel.flag = 'cancel' and to_date(cancel.event_date) < '2023-03-01' then 'cancel_before_pause_was_introduced'
        when pause.flag is null and cancel.flag = 'paused_cancel' then 'pause_in_future_but_cancel'
        when pause.flag = 'paused' and cancel.flag = 'paused_cancel' then 'pause_in_effect_but_cancel'
        when pause.flag = 'paused' and (cancel.flag is null or cancel.flag = 'cancel') then 'paused' 
        end) as event_flag
from cancel full outer join pause on cancel.subscription_id = pause.subscription_uuid
group by 1
)


, cohort as 
(
    select pc.subscription_id as subscription_uuid,
        sub.customer_id,
        to_date(sub.activated_at) as activation_date,
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
        sub.first_product as product,
        sub.first_discount_percentage_base_price as first_discount_percentage,
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
        pc.event_date,
        pc.event_flag
    from pause_and_cancel as pc left join 
        seed_data.dev.subscription_master as sub on pc.subscription_id = sub.recurly_subscription_id
)



, pause_no_cancel_activity as 
        ( with pause_no_cancel_cohort as 
            (
                select p.subscription_uuid,
                    (o.Total_amount_paid_less_cogs + o.TOTAL_SHIPPING_COST - o.AMOUNT_REFUNDED) as ltv,
                    o.amount_paid_by_transaction as revenue,
                    row_number() over (partition by subscription_id order by order_date) as invoice_rank,
                    floor((datediff(day,to_date(p.event_date),to_date(order_date)))/30) as order_since_pause_days
                from cohort as p
                    left join SEED_DATA.DEV.V_ORDER_HISTORY_COGS_UPDATE as o on p.subscription_uuid = o.subscription_id
                where order_date >= p.event_date and event_flag ilike 'pause%'
            
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
                max(case when order_since_pause_days = 5 then 1 else 0 end) as rebill_days_151to180_resume,
                sum(case when order_since_pause_days = 0 then ltv else 0 end) as ltv_days_0to30_resume,
                sum(case when order_since_pause_days = 1 then ltv else 0 end) as ltv_days_31to60_resume,
                sum(case when order_since_pause_days = 2 then ltv else 0 end) as ltv_days_61to90_resume,
                sum(case when order_since_pause_days = 3 then ltv else 0 end) as ltv_days_91to120_resume,
                sum(case when order_since_pause_days = 4 then ltv else 0 end) as ltv_days_121to150_resume,
                sum(case when order_since_pause_days = 5 then ltv else 0 end) as ltv_days_151to180_resume,
                sum(case when order_since_pause_days = 0 then revenue else 0 end) as revenue_days_0to30_resume,
                sum(case when order_since_pause_days = 1 then revenue else 0 end) as revenue_days_31to60_resume,
                sum(case when order_since_pause_days = 2 then revenue else 0 end) as revenue_days_61to90_resume,
                sum(case when order_since_pause_days = 3 then revenue else 0 end) as revenue_days_91to120_resume,
                sum(case when order_since_pause_days = 4 then revenue else 0 end) as revenue_days_121to150_resume,
                sum(case when order_since_pause_days = 5 then revenue else 0 end) as revenue_days_151to180_resume

            from pause_no_cancel_cohort
            group by 1
         )
         
,pause_cancel_activity as 
        (
            with pause_cancel_cohort as 
            (
                select p.subscription_uuid as sub_id,
                    (o.Total_amount_paid_less_cogs + o.TOTAL_SHIPPING_COST - o.AMOUNT_REFUNDED) as ltv,
                    o.amount_paid_by_transaction as revenue,
                    row_number() over (partition by subscription_id order by order_date) as invoice_rank,
                    floor((datediff(day,to_date(p.event_date),to_date(order_date)))/30) as order_since_pause_days
                from cohort as p
                    left join SEED_DATA.DEV.V_ORDER_HISTORY_COGS_UPDATE as o on p.customer_id = o.customer_id
                    left join seed_data.dev.subscription_master as s on o.subscription_id = s.RECURLY_SUBSCRIPTION_ID
                where order_date >= p.event_date and s.activated_at >= p.event_date
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
                max(case when order_since_pause_days = 5 then 1 else 0 end) as rebill_days_151to180_Reactivate,
                sum(case when order_since_pause_days = 0 then ltv else 0 end) as ltv_days_0to30_Reactivate,
                sum(case when order_since_pause_days = 1 then ltv else 0 end) as ltv_days_31to60_Reactivate,
                sum(case when order_since_pause_days = 2 then ltv else 0 end) as ltv_days_61to90_Reactivate,
                sum(case when order_since_pause_days = 3 then ltv else 0 end) as ltv_days_91to120_Reactivate,
                sum(case when order_since_pause_days = 4 then ltv else 0 end) as ltv_days_121to150_Reactivate,
                sum(case when order_since_pause_days = 5 then ltv else 0 end) as ltv_days_151to180_Reactivate,
                sum(case when order_since_pause_days = 0 then revenue else 0 end) as revenue_days_0to30_Reactivate,
                sum(case when order_since_pause_days = 1 then revenue else 0 end) as revenue_days_31to60_Reactivate,
                sum(case when order_since_pause_days = 2 then revenue else 0 end) as revenue_days_61to90_Reactivate,
                sum(case when order_since_pause_days = 3 then revenue else 0 end) as revenue_days_91to120_Reactivate,
                sum(case when order_since_pause_days = 4 then revenue else 0 end) as revenue_days_121to150_Reactivate,
                sum(case when order_since_pause_days = 5 then revenue else 0 end) as revenue_days_151to180_Reactivate
            from pause_cancel_cohort
            group by 1
        )

/*, activity as 
(
        select coalesce(pc.sub_id,pnc.subscription_uuid) as subscription_id, 
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
        group by 1
     

)   
*/


select c.*,
    pnc.first_rebill_days_0to30_resume, pnc.first_rebill_days_31to60_resume, pnc.first_rebill_days_61to90_resume,
    pnc.first_rebill_days_91to120_resume, pnc.first_rebill_days_121to150_resume, pnc.first_rebill_days_151to180_resume,
    pnc.rebill_days_0to30_resume, pnc.rebill_days_31to60_resume,pnc.rebill_days_61to90_resume,
    pnc.rebill_days_91to120_resume, pnc.rebill_days_121to150_resume, pnc.rebill_days_151to180_resume,
    pnc.ltv_days_0to30_resume,pnc.ltv_days_31to60_resume,pnc.ltv_days_61to90_resume,
    pnc.ltv_days_91to120_resume,pnc.ltv_days_121to150_resume,pnc.ltv_days_151to180_resume,
    pnc.revenue_days_0to30_resume,pnc.revenue_days_31to60_resume,pnc.revenue_days_61to90_resume,
    pnc.revenue_days_91to120_resume,pnc.revenue_days_121to150_resume,pnc.revenue_days_151to180_resume,
    pc.first_rebill_days_0to30_Reactivate, pc.first_rebill_days_31to60_Reactivate, pc.first_rebill_days_61to90_Reactivate,
    pc.first_rebill_days_91to120_Reactivate, pc.first_rebill_days_121to150_Reactivate, pc.first_rebill_days_151to180_Reactivate,
    pc.rebill_days_0to30_Reactivate, pc.rebill_days_31to60_Reactivate,pc.rebill_days_61to90_Reactivate,
    pc.rebill_days_91to120_Reactivate, pc.rebill_days_121to150_Reactivate, pc.rebill_days_151to180_Reactivate,
    pc.ltv_days_0to30_Reactivate,pc.ltv_days_31to60_Reactivate,pc.ltv_days_61to90_Reactivate,
    pc.ltv_days_91to120_Reactivate,pc.ltv_days_121to150_Reactivate,pc.ltv_days_151to180_Reactivate,
    pc.revenue_days_0to30_Reactivate,pc.revenue_days_31to60_Reactivate,pc.revenue_days_61to90_Reactivate,
    pc.revenue_days_91to120_Reactivate,pc.revenue_days_121to150_Reactivate,pc.revenue_days_151to180_Reactivate,
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
    greatest(coalesce(rebill_days_151to180_resume,0),coalesce(rebill_days_151to180_Reactivate,0)) as rebill_days_151to180,
    (coalesce(pnc.ltv_days_0to30_resume,0) + coalesce(pc.ltv_days_0to30_Reactivate,0)) as  ltv_days_0to30,
    (coalesce(pnc.ltv_days_31to60_resume,0) + coalesce(pc.ltv_days_31to60_Reactivate,0)) as ltv_days_31to60,
    (coalesce(pnc.ltv_days_61to90_resume,0) + coalesce(pc.ltv_days_61to90_Reactivate,0)) as ltv_days_61to90,
    (coalesce(pnc.ltv_days_91to120_resume,0) + coalesce(pc.ltv_days_91to120_Reactivate,0)) as ltv_days_91to120,
    (coalesce(pnc.ltv_days_121to150_resume,0) + coalesce(pc.ltv_days_121to150_Reactivate,0)) as ltv_days_121to150,
    (coalesce(pnc.ltv_days_151to180_resume,0) + coalesce(pc.ltv_days_151to180_Reactivate,0)) as ltv_days_151to180,
    (coalesce(pnc.revenue_days_0to30_resume,0) + coalesce(pc.revenue_days_0to30_Reactivate,0)) as  revenue_days_0to30,
    (coalesce(pnc.revenue_days_31to60_resume,0) + coalesce(pc.revenue_days_31to60_Reactivate,0)) as revenue_days_31to60,
    (coalesce(pnc.revenue_days_61to90_resume,0) + coalesce(pc.revenue_days_61to90_Reactivate,0)) as revenue_days_61to90,
    (coalesce(pnc.revenue_days_91to120_resume,0) + coalesce(pc.revenue_days_91to120_Reactivate,0)) as revenue_days_91to120,
    (coalesce(pnc.revenue_days_121to150_resume,0) + coalesce(pc.revenue_days_121to150_Reactivate,0)) as revenue_days_121to150,
    (coalesce(pnc.revenue_days_151to180_resume,0) + coalesce(pc.revenue_days_151to180_Reactivate,0)) as revenue_days_151to180
    
from cohort  as c
    left join pause_no_cancel_activity as pnc on c.subscription_uuid = pnc.subscription_uuid
    left join pause_cancel_activity as pc on c.subscription_uuid = pc.sub_id;