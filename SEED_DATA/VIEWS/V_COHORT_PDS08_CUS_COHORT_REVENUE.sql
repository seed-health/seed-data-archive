create or replace view SEED_DATA.DEV.V_COHORT_PDS08_CUS_COHORT_REVENUE(
	CREATED_MONTH_YEAR,
	COHORT_QUANTITY,
	ACTIVE_MONTH_0,
	ACTIVE_MONTH_1,
	ACTIVE_MONTH_2,
	ACTIVE_MONTH_3,
	ACTIVE_MONTH_4,
	ACTIVE_MONTH_5,
	ACTIVE_MONTH_6,
	ACTIVE_MONTH_7,
	ACTIVE_MONTH_8,
	ACTIVE_MONTH_9,
	ACTIVE_MONTH_10,
	ACTIVE_MONTH_11,
	ACTIVE_MONTH_12,
	ACTIVE_MONTH_13,
	ACTIVE_MONTH_14,
	ACTIVE_MONTH_15,
	ACTIVE_MONTH_16,
	ACTIVE_MONTH_17,
	ACTIVE_MONTH_18,
	ACTIVE_MONTH_19,
	ACTIVE_MONTH_20,
	ACTIVE_MONTH_21,
	ACTIVE_MONTH_22,
	ACTIVE_MONTH_23,
	ACTIVE_MONTH_24,
	ACTIVE_MONTH_25,
	ACTIVE_MONTH_26,
	ACTIVE_MONTH_27,
	ACTIVE_MONTH_28,
	ACTIVE_MONTH_29,
	ACTIVE_MONTH_30,
	ACTIVE_MONTH_31,
	ACTIVE_MONTH_32,
	ACTIVE_MONTH_33,
	ACTIVE_MONTH_34,
	ACTIVE_MONTH_35,
	ACTIVE_MONTH_36,
	ACTIVE_MONTH_37,
	ACTIVE_MONTH_38,
	ACTIVE_MONTH_39,
	ACTIVE_MONTH_40,
	ACTIVE_MONTH_41,
	ACTIVE_MONTH_42,
	ACTIVE_MONTH_43,
	ACTIVE_MONTH_44,
	ACTIVE_MONTH_45,
	ACTIVE_MONTH_46,
	ACTIVE_MONTH_47,
	ACTIVE_MONTH_48,
	ACTIVE_MONTH_49,
	ACTIVE_MONTH_50,
	ACTIVE_MONTH_51,
	ACTIVE_MONTH_52,
	ACTIVE_MONTH_53,
	ACTIVE_MONTH_54,
	ACTIVE_MONTH_55,
	ACTIVE_MONTH_56,
	ACTIVE_MONTH_57,
	ACTIVE_MONTH_58,
	ACTIVE_MONTH_59,
	ACTIVE_MONTH_60
) as 

with all_orders as 
(
    select * from SEED_DATA.DEV.V_ORDER_HISTORY_COGS_UPDATE
    where product = 'PDS-08'
),

sub_inv as 
(
    -- joining recharge invoices
    select s.recharge_subscription_id,
        s.recurly_subscription_id,
        to_date(s.activated_at) as activated_at,
        s.customer_id,
        to_date(s.first_subscription_date) as account_activated_at,
        coalesce(s.first_quantity,s.quantity) as subscription_quantity,
        to_date(o.order_date) as invoice_date,
        to_date(s.cancelled_at) as cancelled_at,
        floor(datediff(days,to_date(s.first_subscription_date),to_date(o.order_date))/30) as invoice_month,
        o.quantity,
        floor(datediff(days,to_date(s.first_subscription_date),to_date(s.cancelled_at))/30) as cancelled_month,
        Total_amount_paid as revenue
    from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER" as s
        join all_orders as o on s.recharge_subscription_id = o.subscription_id 
    where first_product = 'PDS-08'
    union all 
    --- joining recurly invoices
    select s.recharge_subscription_id,
        s.recurly_subscription_id,
        to_date(s.activated_at) as activated_at,
        s.customer_id,
        to_date(s.first_subscription_date) as account_activated_at,
        coalesce(s.first_quantity,s.quantity) as subscription_quantity,
        to_date(o.order_date) as invoice_date,
        to_date(s.cancelled_at) as cancelled_at,
        floor(datediff(days,to_date(s.first_subscription_date),to_date(o.order_date))/30) as invoice_month,
        o.quantity,
        floor(datediff(days,to_date(s.first_subscription_date),to_date(s.cancelled_at))/30) as cancelled_month,
        Total_amount_paid as revenue
    from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER" as s
        join all_orders as o on s.recurly_subscription_id = o.subscription_id 
    where first_product = 'PDS-08'
), 

sub_inv_flag as
(
    select recharge_subscription_id,recurly_subscription_id,activated_at,
        sum(case when invoice_month = 0 or invoice_month = -1 then revenue else null end) as invoice_month_0_flag,
        sum(case when cancelled_month <= 0 and invoice_month = 1 then revenue
                when cancelled_month <= 0 then 0
                when invoice_month = 0 or invoice_month = 1 then revenue 
                else null end) as invoice_month_1_flag,
        sum(case when cancelled_month <= 1 and invoice_month = 2 then revenue
                when cancelled_month <= 1 then 0
                when invoice_month = 1 or invoice_month = 2 then revenue
                else null end) as invoice_month_2_flag,
        sum(case when cancelled_month <= 2 and invoice_month = 3 then revenue 
                when cancelled_month <= 2 then 0
                when invoice_month = 2 or invoice_month = 3 then revenue 
                else null end) as invoice_month_3_flag,
        sum(case when cancelled_month <= 3 and invoice_month = 4 then revenue
                when cancelled_month <= 3 then 0
                when invoice_month = 3 or invoice_month = 4 then revenue
                else null end) as invoice_month_4_flag,
        sum(case when cancelled_month <= 4 and invoice_month = 5 then revenue
                when cancelled_month <= 4 then 0
                when invoice_month = 4 or invoice_month = 5 then revenue 
                else null end) as invoice_month_5_flag,
        sum(case when cancelled_month <= 5 and invoice_month = 6 then revenue
                when cancelled_month <= 5 then 0
                when invoice_month = 5 or invoice_month = 6 then revenue
                else null end) as invoice_month_6_flag,
        sum(case when cancelled_month <= 6 and invoice_month = 7 then revenue
                when cancelled_month <= 6 then 0
                when invoice_month = 6 or invoice_month = 7 then revenue 
                else null end) as invoice_month_7_flag,
        sum(case when cancelled_month <= 7 and invoice_month = 8 then revenue
                when cancelled_month <= 7 then 0
                when invoice_month = 7 or invoice_month = 8 then revenue
                else null end) as invoice_month_8_flag,
        sum(case when cancelled_month <= 8 and invoice_month = 9 then revenue
                when cancelled_month <= 8 then 0
                when invoice_month = 8 or invoice_month = 9 then revenue 
                else null end) as invoice_month_9_flag,
        sum(case when cancelled_month <= 9 and invoice_month = 10 then revenue
                when cancelled_month <= 9 then 0
                when invoice_month = 9 or invoice_month = 10 then revenue
                else null end) as invoice_month_10_flag,
        sum(case when cancelled_month <= 10 and invoice_month = 11 then revenue
                when cancelled_month <= 10 then 0
                when invoice_month = 10 or invoice_month = 11 then revenue 
                else null end) as invoice_month_11_flag,
        sum(case when cancelled_month <= 11 and invoice_month = 12 then revenue
                when cancelled_month <= 11 then 0
                when invoice_month = 11 or invoice_month = 12 then revenue
                else null end) as invoice_month_12_flag,
        sum(case when cancelled_month <= 12 and invoice_month = 13 then revenue
                when cancelled_month <= 12 then 0
                when invoice_month = 12 or invoice_month = 13 then revenue 
                else null end) as invoice_month_13_flag,
        sum(case when cancelled_month <= 13 and invoice_month = 14 then revenue
                when cancelled_month <= 13 then 0
                when invoice_month = 13 or invoice_month = 14 then revenue
                else null end) as invoice_month_14_flag,
        sum(case when cancelled_month <= 14 and invoice_month = 15 then revenue
                when cancelled_month <= 14 then 0
                when invoice_month = 14 or invoice_month = 15 then revenue 
                else null end) as invoice_month_15_flag,
        sum(case when cancelled_month <= 15 and invoice_month = 16 then revenue
                when cancelled_month <= 15 then 0
                when invoice_month = 15 or invoice_month = 16 then revenue
                else null end) as invoice_month_16_flag,
        sum(case when cancelled_month <= 16 and invoice_month = 17 then revenue 
                when cancelled_month <= 16 then 0
                when invoice_month = 16 or invoice_month = 17 then revenue 
                else null end) as invoice_month_17_flag,
        sum(case when cancelled_month <= 17 and invoice_month = 18 then revenue
                when cancelled_month <= 17 then 0
                when invoice_month = 17 or invoice_month = 18 then revenue
                else null end) as invoice_month_18_flag,
        sum(case when cancelled_month <= 18 and invoice_month = 19 then revenue
                when cancelled_month <= 18 then 0
                when invoice_month = 18 or invoice_month = 19 then revenue 
                else null end) as invoice_month_19_flag,
        sum(case when cancelled_month <= 19 and invoice_month = 20 then revenue
                when cancelled_month <= 19 then 0
                when invoice_month = 19 or invoice_month = 20 then revenue
                else null end) as invoice_month_20_flag,
        sum(case when cancelled_month <= 20 and invoice_month = 21 then revenue
                when cancelled_month <= 20 then 0
                when invoice_month = 20 or invoice_month = 21 then revenue 
                else null end) as invoice_month_21_flag,
        sum(case when cancelled_month <= 21 and invoice_month = 22 then revenue
                when cancelled_month <= 21 then 0
                when invoice_month = 21 or invoice_month = 22 then revenue
                else null end) as invoice_month_22_flag,
        sum(case when cancelled_month <= 22 and invoice_month = 23 then revenue
                when cancelled_month <= 22 then 0
                when invoice_month = 22 or invoice_month = 23 then revenue 
                else null end) as invoice_month_23_flag,
        sum(case when cancelled_month <= 23 and invoice_month = 24 then revenue
                when cancelled_month <= 23 then 0
                when invoice_month = 23 or invoice_month = 24 then revenue
                else null end) as invoice_month_24_flag,
        sum(case when cancelled_month <= 24 and invoice_month = 25 then revenue
                when cancelled_month <= 24 then 0
                when invoice_month = 24 or invoice_month = 25 then revenue 
                else null end) as invoice_month_25_flag,
        sum(case when cancelled_month <= 25 and invoice_month = 26 then revenue
                when cancelled_month <= 25 then 0
                when invoice_month = 25 or invoice_month = 26 then revenue
                else null end) as invoice_month_26_flag,
        sum(case when cancelled_month <= 26 and invoice_month = 27 then revenue
                when cancelled_month <= 26 then 0
                when invoice_month = 26 or invoice_month = 27 then revenue 
                else null end) as invoice_month_27_flag,
        sum(case when cancelled_month <= 27 and invoice_month = 28 then revenue
                when cancelled_month <= 27 then 0
                when invoice_month = 27 or invoice_month = 28 then revenue
                else null end) as invoice_month_28_flag,
        sum(case when cancelled_month <= 28 and invoice_month = 29 then revenue
                when cancelled_month <= 28 then 0
                when invoice_month = 28 or invoice_month = 29 then revenue 
                else null end) as invoice_month_29_flag,
        sum(case when cancelled_month <= 29 and invoice_month = 30 then revenue
                when cancelled_month <= 29 then 0
                when invoice_month = 29 or invoice_month = 30 then revenue
                else null end) as invoice_month_30_flag,
        sum(case when cancelled_month <= 30 and invoice_month = 31 then revenue
                when cancelled_month <= 30 then 0
                when invoice_month = 30 or invoice_month = 31 then revenue 
                else null end) as invoice_month_31_flag,
        sum(case when cancelled_month <= 31 and invoice_month = 32 then revenue
                when cancelled_month <= 31 then 0
                when invoice_month = 31 or invoice_month = 32 then revenue
                else null end) as invoice_month_32_flag,
        sum(case when cancelled_month <= 32 and invoice_month = 33 then revenue
                when cancelled_month <= 32 then 0
                when invoice_month = 32 or invoice_month = 33 then revenue 
                else null end) as invoice_month_33_flag,
        sum(case when cancelled_month <= 33 and invoice_month = 34 then revenue
                when cancelled_month <= 33 then 0
                when invoice_month = 33 or invoice_month = 34 then revenue
                else null end) as invoice_month_34_flag,
        sum(case when cancelled_month <= 34 and invoice_month = 35 then revenue
                when cancelled_month <= 34 then 0
                when invoice_month = 34 or invoice_month = 35 then revenue 
                else null end) as invoice_month_35_flag,
        sum(case when cancelled_month <= 35 and invoice_month = 36 then revenue
                when cancelled_month <= 35 then 0
                when invoice_month = 35 or invoice_month = 36 then revenue
                else null end) as invoice_month_36_flag,
        sum(case when cancelled_month <= 36 and invoice_month = 37 then revenue
                when cancelled_month <= 36 then 0
                when invoice_month = 36 or invoice_month = 37 then revenue 
                else null end) as invoice_month_37_flag,
        sum(case when cancelled_month <= 37 and invoice_month = 38 then revenue
                when cancelled_month <= 37 then 0
                when invoice_month = 37 or invoice_month = 38 then revenue
                else null end) as invoice_month_38_flag,
        sum(case when cancelled_month <= 38 and invoice_month = 39 then revenue
                when cancelled_month <= 38 then 0
                when invoice_month = 38 or invoice_month = 39 then revenue 
                else null end) as invoice_month_39_flag,
        sum(case when cancelled_month <= 39 and invoice_month = 40 then revenue
                when cancelled_month <= 39 then 0
                when invoice_month = 39 or invoice_month = 40 then revenue
                else null end) as invoice_month_40_flag,
        sum(case when cancelled_month <= 40 and invoice_month = 41 then revenue 
                when cancelled_month <= 40 then 0
                when invoice_month = 40 or invoice_month = 41 then revenue 
                else null end) as invoice_month_41_flag,
        sum(case when cancelled_month <= 41 and invoice_month = 42 then revenue
                when cancelled_month <= 41 then 0
                when invoice_month = 41 or invoice_month = 42 then revenue
                else null end) as invoice_month_42_flag,
        sum(case when cancelled_month <= 42 and invoice_month = 43 then revenue
                when cancelled_month <= 42 then 0
                when invoice_month = 42 or invoice_month = 43 then revenue 
                else null end) as invoice_month_43_flag,
        sum(case when cancelled_month <= 43 and invoice_month = 44 then revenue
                when cancelled_month <= 43 then 0
                when invoice_month = 43 or invoice_month = 44 then revenue
                else null end) as invoice_month_44_flag,
        sum(case when cancelled_month <= 44 and invoice_month = 45 then revenue
                when cancelled_month <= 44 then 0
                when invoice_month = 44 or invoice_month = 45 then revenue 
                else null end) as invoice_month_45_flag,
        sum(case when cancelled_month <= 45 and invoice_month = 46 then revenue
                when cancelled_month <= 45 then 0
                when invoice_month = 45 or invoice_month = 46 then revenue
                else null end) as invoice_month_46_flag,
        sum(case when cancelled_month <= 46 and invoice_month = 47 then revenue
                when cancelled_month <= 46 then 0
                when invoice_month = 46 or invoice_month = 47 then revenue 
                else null end) as invoice_month_47_flag,
        sum(case when cancelled_month <= 47 and invoice_month = 48 then revenue
                when cancelled_month <= 47 then 0
                when invoice_month = 47 or invoice_month = 48 then revenue
                else null end) as invoice_month_48_flag,
        sum(case when cancelled_month <= 48 and invoice_month = 49 then revenue
                when cancelled_month <= 48 then 0
                when invoice_month = 48 or invoice_month = 49 then revenue 
                else null end) as invoice_month_49_flag,
        sum(case when cancelled_month <= 49 and invoice_month = 50 then revenue
                when cancelled_month <= 49 then 0
                when invoice_month = 49 or invoice_month = 50 then revenue
                else null end) as invoice_month_50_flag,
        sum(case when cancelled_month <= 50 and invoice_month = 51 then revenue
                when cancelled_month <= 50 then 0
                when invoice_month = 50 or invoice_month = 51 then revenue 
                else null end) as invoice_month_51_flag,
        sum(case when cancelled_month <= 51 and invoice_month = 52 then revenue
                when cancelled_month <= 51 then 0
                when invoice_month = 51 or invoice_month = 52 then revenue
                else null end) as invoice_month_52_flag,
        sum(case when cancelled_month <= 52 and invoice_month = 53 then revenue
                when cancelled_month <= 52 then 0
                when invoice_month = 52 or invoice_month = 53 then revenue 
                else null end) as invoice_month_53_flag,
        sum(case when cancelled_month <= 53 and invoice_month = 54 then revenue
                when cancelled_month <= 53 then 0
                when invoice_month = 53 or invoice_month = 54 then revenue
                else null end) as invoice_month_54_flag,
        sum(case when cancelled_month <= 54 and invoice_month = 55 then revenue
                when cancelled_month <= 54 then 0
                when invoice_month = 54 or invoice_month = 55 then revenue 
                else null end) as invoice_month_55_flag,
        sum(case when cancelled_month <= 55 and invoice_month = 56 then revenue
                when cancelled_month <= 55 then 0
                when invoice_month = 55 or invoice_month = 56 then revenue
                else null end) as invoice_month_56_flag,
        sum(case when cancelled_month <= 56 and invoice_month = 57 then revenue
                when cancelled_month <= 56 then 0
                when invoice_month = 56 or invoice_month = 57 then revenue 
                else null end) as invoice_month_57_flag,
        sum(case when cancelled_month <= 57 and invoice_month = 58 then revenue
                when cancelled_month <= 57 then 0
                when invoice_month = 57 or invoice_month = 58 then revenue
                else null end) as invoice_month_58_flag,
        sum(case when cancelled_month <= 58 and invoice_month = 59 then revenue
                when cancelled_month <= 58 then 0
                when invoice_month = 58 or invoice_month = 59 then revenue 
                else null end) as invoice_month_59_flag,
        sum(case when cancelled_month <= 59 and invoice_month = 60 then revenue
                when cancelled_month <= 59 then 0
                when invoice_month = 59 or invoice_month = 60 then revenue
                else null end) as invoice_month_60_flag,
        sum(case when cancelled_month <= 60 and invoice_month = 61 then revenue
                when cancelled_month <= 60 then 0
                when invoice_month = 60 or invoice_month = 61 then revenue
                else null end) as invoice_month_61_flag,
        sum(case when cancelled_month <= 61 and invoice_month = 62 then revenue
                when cancelled_month <= 61 then 0
                when invoice_month = 61 or invoice_month = 62 then revenue
                else null end) as invoice_month_62_flag
        
    from sub_inv
    group by recharge_subscription_id,recurly_subscription_id,activated_at
)

select 
    date_trunc('month',to_date(activated_at)) as created_month_year,
    ---left(to_date(activated_at),7) as created_month_year,
    count(*) as cohort_quantity,
    sum(invoice_month_0_flag) as active_month_0,
    ---- new build / removes the additional forward-looking months on the cohort
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 1 then null else invoice_month_1_flag end) as active_month_1,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 2 then null else invoice_month_2_flag end) as active_month_2,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 3 then null else invoice_month_3_flag end) as active_month_3,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 4 then null else invoice_month_4_flag end) as active_month_4,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 5 then null else invoice_month_5_flag end) as active_month_5,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 6 then null else invoice_month_6_flag end) as active_month_6,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 7 then null else invoice_month_7_flag end) as active_month_7,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 8 then null else invoice_month_8_flag end) as active_month_8,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 9 then null else invoice_month_9_flag end) as active_month_9,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 10 then null else invoice_month_10_flag end) as active_month_10,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 11 then null else invoice_month_11_flag end) as active_month_11,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 12 then null else invoice_month_12_flag end) as active_month_12,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 13 then null else invoice_month_13_flag end) as active_month_13,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 14 then null else invoice_month_14_flag end) as active_month_14,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 15 then null else invoice_month_15_flag end) as active_month_15,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 16 then null else invoice_month_16_flag end) as active_month_16,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 17 then null else invoice_month_17_flag end) as active_month_17,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 18 then null else invoice_month_18_flag end) as active_month_18,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 19 then null else invoice_month_19_flag end) as active_month_19,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 20 then null else invoice_month_20_flag end) as active_month_20,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 21 then null else invoice_month_21_flag end) as active_month_21,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 22 then null else invoice_month_22_flag end) as active_month_22,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 23 then null else invoice_month_23_flag end) as active_month_23,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 24 then null else invoice_month_24_flag end) as active_month_24,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 25 then null else invoice_month_25_flag end) as active_month_25,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 26 then null else invoice_month_26_flag end) as active_month_26,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 27 then null else invoice_month_27_flag end) as active_month_27,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 28 then null else invoice_month_28_flag end) as active_month_28,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 29 then null else invoice_month_29_flag end) as active_month_29,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 30 then null else invoice_month_30_flag end) as active_month_30,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 31 then null else invoice_month_31_flag end) as active_month_31,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 32 then null else invoice_month_32_flag end) as active_month_32,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 33 then null else invoice_month_33_flag end) as active_month_33,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 34 then null else invoice_month_34_flag end) as active_month_34,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 35 then null else invoice_month_35_flag end) as active_month_35,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 36 then null else invoice_month_36_flag end) as active_month_36,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 37 then null else invoice_month_37_flag end) as active_month_37,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 38 then null else invoice_month_38_flag end) as active_month_38,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 39 then null else invoice_month_39_flag end) as active_month_39,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 40 then null else invoice_month_40_flag end) as active_month_40,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 41 then null else invoice_month_41_flag end) as active_month_41,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 42 then null else invoice_month_42_flag end) as active_month_42,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 43 then null else invoice_month_43_flag end) as active_month_43,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 44 then null else invoice_month_44_flag end) as active_month_44,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 45 then null else invoice_month_45_flag end) as active_month_45,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 46 then null else invoice_month_46_flag end) as active_month_46,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 47 then null else invoice_month_47_flag end) as active_month_47,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 48 then null else invoice_month_48_flag end) as active_month_48,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 49 then null else invoice_month_49_flag end) as active_month_49,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 50 then null else invoice_month_50_flag end) as active_month_50,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 51 then null else invoice_month_51_flag end) as active_month_51,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 52 then null else invoice_month_52_flag end) as active_month_52,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 53 then null else invoice_month_53_flag end) as active_month_53,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 54 then null else invoice_month_54_flag end) as active_month_54,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 55 then null else invoice_month_55_flag end) as active_month_55,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 56 then null else invoice_month_56_flag end) as active_month_56,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 57 then null else invoice_month_57_flag end) as active_month_57,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 58 then null else invoice_month_58_flag end) as active_month_58,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 59 then null else invoice_month_59_flag end) as active_month_59,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 60 then null else invoice_month_60_flag end) as active_month_60
    
from sub_inv_flag
where date_trunc('month',to_date(activated_at)) <= DATEADD(month, -1, date_trunc('month',to_date(current_date()))) --- only through previous month
group by 1
order by 1;