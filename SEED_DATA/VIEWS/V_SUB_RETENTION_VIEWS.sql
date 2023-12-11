create or replace view SEED_DATA.DEV.V_SUB_RETENTION_VIEWS as

with all_orders as 
(
    select * from SEED_DATA.DEV.V_ORDER_HISTORY_COGS_UPDATE
),

consent_flag_tbl as 
(
select email, max(case when accepts_marketing ilike '%TRUE%' or consent is not null then 1 else 0 end) as consent_flag
from SEGMENT_EVENTS.SEED_COM.IDENTIFIES
where not (consent is null and accepts_marketing is null)
group by 1 
),

all_subs as 
(
    select customer_id,
        customer_email,
        recharge_subscription_id,
        recurly_subscription_id,
        concat(coalesce(recharge_subscription_id,','),coalesce(recurly_subscription_id,',')) as subscription_id,
        to_date(first_subscription_date) as customer_cohort_date,
        to_date(activated_at) as subscription_cohort_date,
        to_date(s.cancelled_at) as subscription_cancelled_at, 
        case when srp_3mo_ever_flag = 1 or srp_6mo_ever_flag = 1 then 1 else 0 end as is_srp_ever,
        case when srp_3mo_ever_flag = 1 then 1 else 0 end as is_srp_3mo_ever,
        case when srp_6mo_ever_flag = 1 then 1 else 0 end as is_srp_6mo_ever,
        case when srp_3mo_currently = 1 or srp_6mo_currently = 1 then 1 else 0 end as is_srp_currently,
        case when srp_3mo_currently = 1 then 1 else 0 end as is_srp_3mo_currently,
        case when srp_6mo_currently = 1 then 1 else 0 end as is_srp_6mo_currently,
        case when first_product = 'DS-01' then 1 else 0 end as is_sub_ds01,
        case when first_product = 'PDS-08' then 1 else 0 end as is_sub_pds08,
        case when first_subscription_date_ds01 is null and first_subscription_date_pds08 is not null then 1 else 0 end as is_cus_pds08_only,
        case when first_subscription_date_pds08 is null and first_subscription_date_ds01 is not null then 1 else 0 end as is_cus_ds01_only,
        case when first_subscription_date_pds08 is not null and first_subscription_date_ds01 is not null then 1 else 0 end as is_cus_ds01_pds08,
        case when first_subscription_date_ds01 = first_subscription_date_pds08 then 1 else 0 end as is_cus_dsandpds_sametime,
        case when first_subscription_date_ds01 < first_subscription_date_pds08 then 1 else 0 end as is_cus_dsandps_dsfirst,
        case when first_subscription_date_ds01 > first_subscription_date_pds08 then 1 else 0 end as is_cus_dsandps_pdsfirst,
        case when pause_start_date is not null then 1 else 0 end as is_pause_ever,
        c.consent_flag as is_email_consent_ever,
        case when first_discount_percentage is not null and 
            first_discount_percentage > 0 and 
            first_discount_percentage <= 100 then 1 else 0 end as is_first_order_discounted,
        case when first_discount_percentage is null then 1 else 0 end as is_first_order_not_discounted,
        case when first_discount_percentage = 0 then 1 else 0 end as is_first_discount_0,
        case when first_discount_percentage = 10 then 1 else 0 end as is_first_discount_10,
        case when first_discount_percentage = 15 then 1 else 0 end as is_first_discount_15,
        case when first_discount_percentage = 20 then 1 else 0 end as is_first_discount_20,
        case when first_discount_percentage = 25 then 1 else 0 end as is_first_discount_25,
        case when first_discount_percentage = 30 then 1 else 0 end as is_first_discount_30,
        case when first_discount_percentage = 40 then 1 else 0 end as is_first_discount_40,
        case when first_discount_percentage = 50 then 1 else 0 end as is_first_discount_50,
        case when first_discount_percentage = 100 then 1 else 0 end as is_first_discount_100,
        case when first_discount_percentage > 0 and first_discount_percentage <= 10 then 1 else 0 end as is_first_discount_0to10,
        case when first_discount_percentage > 10 and first_discount_percentage <= 20 then 1 else 0 end as is_first_discount_10to20,
        case when first_discount_percentage > 20 and first_discount_percentage <= 30 then 1 else 0 end as is_first_discount_20to30,
        case when first_discount_percentage > 30 and first_discount_percentage <= 40 then 1 else 0 end as is_first_discount_30to40,
        case when first_discount_percentage > 40 and first_discount_percentage <= 50 then 1 else 0 end as is_first_discount_40to50,
        case when first_discount_percentage > 50 and first_discount_percentage <= 60 then 1 else 0 end as is_first_discount_50to60,
        case when first_discount_percentage > 60 and first_discount_percentage <= 70 then 1 else 0 end as is_first_discount_60to70,
        case when first_discount_percentage > 70 and first_discount_percentage <= 80 then 1 else 0 end as is_first_discount_70to80,
        case when first_discount_percentage > 80 and first_discount_percentage <= 90 then 1 else 0 end as is_first_discount_80to90,
        case when first_discount_percentage > 90 and first_discount_percentage <= 100 then 1 else 0 end as is_first_discount_90to100,
        case when to_date(activated_at) < to_date(cancelled_at) then 1 else 0 end as is_cancelled_before_activation, 
        reactivation_flag,
        case when has_contacted_mx = 'Y' then 1 else 0 end as MX_flag ,
        case when has_contacted_care = 'Y' then 1 else 0 end as Care_flag,
        case when has_contacted_sci_care = 'Y' then 1 else 0 end as SCI_Care_flag,
        --- Adding coupon flags for Bobby, Gwyenth, and Steph
        case when first_promotion_code = 'sss' or first_promotion_code = 'steph25' or first_promotion_code = 'steph30' or first_promotion_code = 'sss20'
            or first_promotion_code = 'savory25' then 'Sweet Savory Steph'
        when first_promotion_code = 'flavcity15' or first_promotion_code = 'bobby15' or first_promotion_code = 'bobby' or first_promotion_code = 'flavcity'
            or first_promotion_code = 'flavcity30' or first_promotion_code = 'flavcity40' or first_promotion_code = 'bobbyapproved' 
            or first_promotion_code = 'flavcity25' then 'Bobby Parish'
        when first_promotion_code = 'gwyneth' or first_promotion_code = 'gwyneth30' then 'Gwyneth'
        when first_promotion_code is not null then 'Other Coupon code'
        else 'No Coupon code' end as coupon_flag
    from "SEED_DATA"."DEV"."V_SUBSCRIPTION_MASTER" as s
        left join consent_flag_tbl as c on s.customer_email = c.email

),

sub_inv as 
(
    -- joining recharge invoices
    select s.recharge_subscription_id,s.recurly_subscription_id,s.subscription_id,
        floor(datediff(days,s.subscription_cohort_date,o.order_date)/30) as invoice_sub_month,
        floor(datediff(days,s.subscription_cohort_date,s.subscription_cancelled_at)/30) as cancelled_sub_month,
        (Total_amount_paid_less_cogs + TOTAL_SHIPPING_COST - AMOUNT_REFUNDED) as ltv,
        invoice_refund_flag
    from all_subs as s
        join all_orders as o on s.recharge_subscription_id = o.subscription_id
        
    union all 
    
    --- joining recurly invoices
    select s.recharge_subscription_id,s.recurly_subscription_id,s.subscription_id,
        floor(datediff(days,s.subscription_cohort_date,o.order_date)/30) as invoice_sub_month,
        floor(datediff(days,s.subscription_cohort_date,s.subscription_cancelled_at)/30) as cancelled_sub_month,
        (Total_amount_paid_less_cogs + TOTAL_SHIPPING_COST - AMOUNT_REFUNDED) as ltv,
        invoice_refund_flag
    from all_subs as s
        join all_orders as o on s.recurly_subscription_id = o.subscription_id 
),


active_cylces as 
(
select distinct d1.date,(floor(datediff(days,d1.date,d2.date)/30)-1) as cycle
from dim_date as d1
cross join dim_date as d2 
where d1.date >= '2018-01-01' and d1.date < current_date and d2.date < current_date and cycle >=0 
order by 1,2
),

final_table as 
(
select s.*,c.cycle,
    max(case when cycle = 0 and (invoice_sub_month = -1 or invoice_sub_month = 0) then 1
             when cycle > 0 and cancelled_sub_month >= 0 and cancelled_sub_month <= cycle-1 and invoice_sub_month = cycle then 1
             when cycle > 0 and cancelled_sub_month >= 0 and cancelled_sub_month <= cycle-1 then 0 
             when cycle > 0 and invoice_sub_month = cycle or invoice_sub_month = cycle-1 then 1 
             else 0 end) as active_flag,
    max(case when cycle = 0 and (invoice_sub_month = -1 or invoice_sub_month = 0) then 1
             when cycle > 0 and invoice_sub_month = cycle then 1 
             else 0 end) as active_flag_revenue_retention,
    sum(case when cycle = 0 and (invoice_sub_month = -1 or invoice_sub_month = 0) and invoice_refund_flag = 'not_fully_refunded' then ltv
             when cycle > 0 and  invoice_sub_month = cycle and invoice_refund_flag = 'not_fully_refunded' then ltv
             else 0 end) as ltv
    
from all_subs as s
    left join active_cylces as c on s.subscription_cohort_date = c.date
    left join sub_inv as si on s.subscription_id = si.subscription_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52
order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52
)


select * EXCLUDE (customer_id, customer_email,recharge_subscription_id,recurly_subscription_id,subscription_id,customer_cohort_date,subscription_cancelled_at,subscription_cohort_date,active_flag_revenue_retention,ltv),
left(subscription_cohort_date,7) as subscription_month_year,
count(*) as subscription_denominator,
sum(active_flag) as subscription_numerator, 
sum(active_flag_revenue_retention) as subscription_numerator_rr,
sum(ltv) as subscription_numerator_ltv
from final_table
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46
order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46;