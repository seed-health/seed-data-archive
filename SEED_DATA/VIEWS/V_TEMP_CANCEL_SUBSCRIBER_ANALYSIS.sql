create or replace view SEED_DATA.DEV.V_TEMP_CANCEL_SUBSCRIBER_ANALYSIS as 
--- select * from SEED_DATA.DEV.V_TEMP_CANCEL_SUBSCRIBER_ANALYSIS

with cancel as 
(
select *,
to_date(activated_at) as activated_date,
to_date(first_cancel_date) as first_cancel_date_date,
date_trunc('month',first_cancel_date_date) as first_cancel_month_year,
datediff(days,first_cancel_date,current_date) as days_since_cancel,
to_date(first_subscription_date) as activation_date,
date_trunc(month,activation_date) as activation_month_year,
date_trunc(year,activation_date) as activation_year,
first_discount_percentage as first_discount_percentage,
--datediff(days,first_cancel_date_date,activated_date) as days_since_cancel,
case when days_since_cancel >=0 and days_since_cancel <= 10 then 'cancel-0-10'
        when days_since_cancel >=11 and days_since_cancel <= 20 then 'cancel-11-20'
        when days_since_cancel >=21 and days_since_cancel <= 30 then 'cancel-21-30'
        when days_since_cancel >=31 and days_since_cancel <= 40 then 'cancel-31-40'
        when days_since_cancel >=41 and days_since_cancel <= 50 then 'cancel-41-50'
        when days_since_cancel >=51 and days_since_cancel <= 60 then 'cancel-51-60'
        when days_since_cancel >=60 then 'cancel-60+'
        end as cancel_days_cat,
        case when reactivation_flag = 1 and activated_date >= first_cancel_date_date then 1 end as reactivation_tag
from SEED_DATA.DEV.SUBSCRIPTION_MASTER
where to_date(first_cancel_date) >= '2023-01-01' and to_date(first_cancel_date) < '2023-09-01'
)

select 
distinct RECURLY_SUBSCRIPTION_ID as SUBSCRIPTION_UUID, first_cancel_month_year
from cancel
where 
first_cancel_month_year >= '2023-04-01' and 
ifnull(reactivation_tag,0) <> 0 and first_product = 'DS-01'
