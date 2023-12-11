create or replace view SEED_DATA.DEV.V_COHORT_TOTAL_CUS_COHORT_NO_PAUSE(
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
	ACTIVE_MONTH_60,
	ACTIVE_MONTH_61,
	ACTIVE_MONTH_62,
	ACTIVE_MONTH_63,
	ACTIVE_MONTH_64,
	ACTIVE_MONTH_65,
	ACTIVE_MONTH_66,
	ACTIVE_MONTH_67,
	ACTIVE_MONTH_68,
	ACTIVE_MONTH_69,
	ACTIVE_MONTH_70
) as

with all_orders as 
(
    select * from SEED_DATA.DEV.V_ORDER_HISTORY_SKU_ADJUSTED
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
        to_date(o.invoice_date) as invoice_date,
        to_date(s.cancelled_at) as cancelled_at,
        floor(datediff(days,to_date(s.first_subscription_date),to_date(o.invoice_date))/30) as invoice_month,
        invoiced_quantity,
        floor(datediff(days,to_date(s.first_subscription_date),to_date(s.cancelled_at))/30) as cancelled_month
    from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER" as s
        join all_orders as o on s.recharge_subscription_id = o.subscription_id 
    union all 
    --- joining recurly invoices
    select s.recharge_subscription_id,
        s.recurly_subscription_id,
        to_date(s.activated_at) as activated_at,
        s.customer_id,
        to_date(s.first_subscription_date) as account_activated_at,
        coalesce(s.first_quantity,s.quantity) as subscription_quantity,
        to_date(o.invoice_date) as invoice_date,
        to_date(s.cancelled_at) as cancelled_at,
        floor(datediff(days,to_date(s.first_subscription_date),to_date(o.invoice_date))/30) as invoice_month,
        invoiced_quantity,
        floor(datediff(days,to_date(s.first_subscription_date),to_date(s.cancelled_at))/30) as cancelled_month
    from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER" as s
        join all_orders as o on s.recurly_subscription_id = o.subscription_id 
), 

sub_inv_flag as
(
    select customer_id, account_activated_at,
        max(case when invoice_month = 0 or invoice_month = -1 then 1 else null end) as invoice_month_0_flag,
        max(case when invoice_month = 1 then 1 
                else null end) as invoice_month_1_flag,
        max(case when invoice_month = 2 then 1 
                else null end) as invoice_month_2_flag,
        max(case when invoice_month = 3 then 1 
                else null end) as invoice_month_3_flag,
        max(case when invoice_month = 4 then 1 
                else null end) as invoice_month_4_flag,
        max(case when invoice_month = 5 then 1 
                else null end) as invoice_month_5_flag,
        max(case when invoice_month = 6 then 1 
                else null end) as invoice_month_6_flag,
        max(case when invoice_month = 7 then 1 
                else null end) as invoice_month_7_flag,
        max(case when invoice_month = 8 then 1 
                else null end) as invoice_month_8_flag,
        max(case when invoice_month = 9 then 1 
                else null end) as invoice_month_9_flag,
        max(case when invoice_month = 10 then 1 
                else null end) as invoice_month_10_flag,
        max(case when invoice_month = 11 then 1 
                else null end) as invoice_month_11_flag,
        max(case when invoice_month = 12 then 1 
                else null end) as invoice_month_12_flag,
        max(case when invoice_month = 13 then 1 
                else null end) as invoice_month_13_flag,
        max(case when invoice_month = 14 then 1 
                else null end) as invoice_month_14_flag,
        max(case when invoice_month = 15 then 1 
                else null end) as invoice_month_15_flag,
        max(case when invoice_month = 16 then 1 
                else null end) as invoice_month_16_flag,
        max(case when invoice_month = 17 then 1 
                else null end) as invoice_month_17_flag,
        max(case when invoice_month = 18 then 1 
                else null end) as invoice_month_18_flag,
        max(case when invoice_month = 19 then 1 
                else null end) as invoice_month_19_flag,
        max(case when invoice_month = 20 then 1 
                else null end) as invoice_month_20_flag,
        max(case when invoice_month = 21 then 1 
                else null end) as invoice_month_21_flag,
        max(case when invoice_month = 22 then 1 
                else null end) as invoice_month_22_flag,
        max(case when invoice_month = 23 then 1 
                else null end) as invoice_month_23_flag,
        max(case when invoice_month = 24 then 1 
                else null end) as invoice_month_24_flag,
        max(case when invoice_month = 25 then 1 
                else null end) as invoice_month_25_flag,
        max(case when invoice_month = 26 then 1 
                else null end) as invoice_month_26_flag,
        max(case when invoice_month = 27 then 1 
                else null end) as invoice_month_27_flag,
        max(case when invoice_month = 28 then 1 
                else null end) as invoice_month_28_flag,
        max(case when invoice_month = 29 then 1 
                else null end) as invoice_month_29_flag,
        max(case when invoice_month = 30 then 1 
                else null end) as invoice_month_30_flag,
        max(case when invoice_month = 31 then 1 
                else null end) as invoice_month_31_flag,
        max(case when invoice_month = 32 then 1 
                else null end) as invoice_month_32_flag,
        max(case when invoice_month = 33 then 1 
                else null end) as invoice_month_33_flag,
        max(case when invoice_month = 34 then 1 
                else null end) as invoice_month_34_flag,
        max(case when invoice_month = 35 then 1 
                else null end) as invoice_month_35_flag,
        max(case when invoice_month = 36 then 1 
                else null end) as invoice_month_36_flag,
        max(case when invoice_month = 37 then 1 
                else null end) as invoice_month_37_flag,
        max(case when invoice_month = 38 then 1 
                else null end) as invoice_month_38_flag,
        max(case when invoice_month = 39 then 1 
                else null end) as invoice_month_39_flag,
        max(case when invoice_month = 40 then 1 
                else null end) as invoice_month_40_flag,
        max(case when invoice_month = 41 then 1 
                else null end) as invoice_month_41_flag,
        max(case when invoice_month = 42 then 1 
                else null end) as invoice_month_42_flag,
        max(case when invoice_month = 43 then 1 
                else null end) as invoice_month_43_flag,
        max(case when invoice_month = 44 then 1 
                else null end) as invoice_month_44_flag,
        max(case when invoice_month = 45 then 1 
                else null end) as invoice_month_45_flag,
        max(case when invoice_month = 46 then 1 
                else null end) as invoice_month_46_flag,
        max(case when invoice_month = 47 then 1 
                else null end) as invoice_month_47_flag,
        max(case when invoice_month = 48 then 1 
                else null end) as invoice_month_48_flag,
        max(case when invoice_month = 49 then 1 
                else null end) as invoice_month_49_flag,
        max(case when invoice_month = 50 then 1 
                else null end) as invoice_month_50_flag,
        max(case when invoice_month = 51 then 1 
                else null end) as invoice_month_51_flag,
        max(case when invoice_month = 52 then 1 
                else null end) as invoice_month_52_flag,
        max(case when invoice_month = 53 then 1 
                else null end) as invoice_month_53_flag,
        max(case when invoice_month = 54 then 1 
                else null end) as invoice_month_54_flag,
        max(case when invoice_month = 55 then 1 
                else null end) as invoice_month_55_flag,
        max(case when invoice_month = 56 then 1 
                else null end) as invoice_month_56_flag,
        max(case when invoice_month = 57 then 1 
                else null end) as invoice_month_57_flag,
        max(case when invoice_month = 58 then 1 
                else null end) as invoice_month_58_flag,
        max(case when invoice_month = 59 then 1 
                else null end) as invoice_month_59_flag,
        max(case when invoice_month = 60 then 1 
                else null end) as invoice_month_60_flag,
        max(case when invoice_month = 61 then 1 
                else null end) as invoice_month_61_flag,
        max(case when invoice_month = 62 then 1 
                else null end) as invoice_month_62_flag,
        max(case when invoice_month = 63 then 1 
                else null end) as invoice_month_63_flag,
        max(case when invoice_month = 64 then 1 
                else null end) as invoice_month_64_flag,
        max(case when invoice_month = 65 then 1 
                else null end) as invoice_month_65_flag,
        max(case when invoice_month = 66 then 1 
                else null end) as invoice_month_66_flag,
        max(case when invoice_month = 67 then 1 
                else null end) as invoice_month_67_flag,
        max(case when invoice_month = 68 then 1 
                else null end) as invoice_month_68_flag,
        max(case when invoice_month = 69 then 1 
                else null end) as invoice_month_69_flag,
        max(case when invoice_month = 70 then 1 
                else null end) as invoice_month_70_flag

    from sub_inv
    group by customer_id, account_activated_at
)

select 
    date_trunc('month',to_date(account_activated_at)) as created_month_year,
    ---left(to_date(activated_at),7) as created_month_year,
    count(*) as cohort_quantity,
    sum(invoice_month_0_flag) as active_month_0,
    ---- new build / removes the additional forward looking months on the cohort
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
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 60 then null else invoice_month_60_flag end) as active_month_60,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 61 then null else invoice_month_61_flag end) as active_month_61,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 62 then null else invoice_month_62_flag end) as active_month_62,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 63 then null else invoice_month_63_flag end) as active_month_63,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 64 then null else invoice_month_64_flag end) as active_month_64,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 65 then null else invoice_month_65_flag end) as active_month_65,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 66 then null else invoice_month_66_flag end) as active_month_66,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 67 then null else invoice_month_67_flag end) as active_month_67,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 68 then null else invoice_month_68_flag end) as active_month_68,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 69 then null else invoice_month_69_flag end) as active_month_69,
    sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 70 then null else invoice_month_70_flag end) as active_month_70

from sub_inv_flag
where date_trunc('month',to_date(account_activated_at)) <= DATEADD(month, -1, date_trunc('month',to_date(current_date()))) --- only through previous month
group by 1
order by 1;