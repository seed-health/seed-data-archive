create or replace view SEED_DATA.DEV.V_COHORT_DS01_SUB_COHORT_Tableau as

with all_orders as 
(
    select * from SEED_DATA.DEV.V_ORDER_HISTORY_SKU_ADJUSTED
    where product = 'DS-01'
),

sub_inv as 
(
    -- joining recharge invoices
    select s.recharge_subscription_id,
        s.recurly_subscription_id,
        to_date(s.activated_at) as activated_at,
        coalesce(s.first_quantity,s.quantity) as subscription_quantity,
        to_date(o.invoice_date) as invoice_date,
        to_date(s.cancelled_at) as cancelled_at,
        floor(datediff(days,s.activated_at,o.invoice_date)/30) as invoice_month,
        invoiced_quantity,
        floor(datediff(days,s.activated_at,s.cancelled_at)/30) as cancelled_month
    from "SEED_DATA"."DEV"."V_SUBSCRIPTION_MASTER" as s
        join all_orders as o on s.recharge_subscription_id = o.subscription_id 
    where first_product = 'DS-01'
    union all 
    --- joining recurly invoices
    select s.recharge_subscription_id, 
        s.recurly_subscription_id,
        to_date(s.activated_at) as activated_at,
        coalesce(s.first_quantity,s.quantity) as subscription_quantity,
        to_date(o.invoice_date) as invoice_date,
        to_date(s.cancelled_at) as cancelled_at,
        floor(datediff(days,s.activated_at,o.invoice_date)/30) as invoice_month,
        invoiced_quantity,
        floor(datediff(days,s.activated_at,s.cancelled_at)/30) as cancelled_month
    from "SEED_DATA"."DEV"."V_SUBSCRIPTION_MASTER" as s
        join all_orders as o on s.recurly_subscription_id = o.subscription_id 
    where first_product = 'DS-01'
),

 

sub_inv_flag as
(
    select recharge_subscription_id,recurly_subscription_id,activated_at,
        max(case when invoice_month = 0 or invoice_month = -1 then 1 else null end) as invoice_month_0_flag,
        max(case when cancelled_month <= 0 and invoice_month = 1 then 1
                when cancelled_month <= 0 then 0
                when invoice_month = 0 or invoice_month = 1 then 1 
                else null end) as invoice_month_1_flag,
        max(case when cancelled_month <= 1 and invoice_month = 2 then 1
                when cancelled_month <= 1 then 0
                when invoice_month = 1 or invoice_month = 2 then 1
                else null end) as invoice_month_2_flag,
        max(case when cancelled_month <= 2 and invoice_month = 3 then 1 
                when cancelled_month <= 2 then 0
                when invoice_month = 2 or invoice_month = 3 then 1 
                else null end) as invoice_month_3_flag,
        max(case when cancelled_month <= 3 and invoice_month = 4 then 1
                when cancelled_month <= 3 then 0
                when invoice_month = 3 or invoice_month = 4 then 1
                else null end) as invoice_month_4_flag,
        max(case when cancelled_month <= 4 and invoice_month = 5 then 1
                when cancelled_month <= 4 then 0
                when invoice_month = 4 or invoice_month = 5 then 1 
                else null end) as invoice_month_5_flag,
        max(case when cancelled_month <= 5 and invoice_month = 6 then 1
                when cancelled_month <= 5 then 0
                when invoice_month = 5 or invoice_month = 6 then 1
                else null end) as invoice_month_6_flag,
        max(case when cancelled_month <= 6 and invoice_month = 7 then 1
                when cancelled_month <= 6 then 0
                when invoice_month = 6 or invoice_month = 7 then 1 
                else null end) as invoice_month_7_flag,
        max(case when cancelled_month <= 7 and invoice_month = 8 then 1
                when cancelled_month <= 7 then 0
                when invoice_month = 7 or invoice_month = 8 then 1
                else null end) as invoice_month_8_flag,
        max(case when cancelled_month <= 8 and invoice_month = 9 then 1
                when cancelled_month <= 8 then 0
                when invoice_month = 8 or invoice_month = 9 then 1 
                else null end) as invoice_month_9_flag,
        max(case when cancelled_month <= 9 and invoice_month = 10 then 1
                when cancelled_month <= 9 then 0
                when invoice_month = 9 or invoice_month = 10 then 1
                else null end) as invoice_month_10_flag,
        max(case when cancelled_month <= 10 and invoice_month = 11 then 1
                when cancelled_month <= 10 then 0
                when invoice_month = 10 or invoice_month = 11 then 1 
                else null end) as invoice_month_11_flag,
        max(case when cancelled_month <= 11 and invoice_month = 12 then 1
                when cancelled_month <= 11 then 0
                when invoice_month = 11 or invoice_month = 12 then 1
                else null end) as invoice_month_12_flag,
        max(case when cancelled_month <= 12 and invoice_month = 13 then 1
                when cancelled_month <= 12 then 0
                when invoice_month = 12 or invoice_month = 13 then 1 
                else null end) as invoice_month_13_flag,
        max(case when cancelled_month <= 13 and invoice_month = 14 then 1
                when cancelled_month <= 13 then 0
                when invoice_month = 13 or invoice_month = 14 then 1
                else null end) as invoice_month_14_flag,
        max(case when cancelled_month <= 14 and invoice_month = 15 then 1
                when cancelled_month <= 14 then 0
                when invoice_month = 14 or invoice_month = 15 then 1 
                else null end) as invoice_month_15_flag,
        max(case when cancelled_month <= 15 and invoice_month = 16 then 1
                when cancelled_month <= 15 then 0
                when invoice_month = 15 or invoice_month = 16 then 1
                else null end) as invoice_month_16_flag,
        max(case when cancelled_month <= 16 and invoice_month = 17 then 1 
                when cancelled_month <= 16 then 0
                when invoice_month = 16 or invoice_month = 17 then 1 
                else null end) as invoice_month_17_flag,
        max(case when cancelled_month <= 17 and invoice_month = 18 then 1
                when cancelled_month <= 17 then 0
                when invoice_month = 17 or invoice_month = 18 then 1
                else null end) as invoice_month_18_flag,
        max(case when cancelled_month <= 18 and invoice_month = 19 then 1
                when cancelled_month <= 18 then 0
                when invoice_month = 18 or invoice_month = 19 then 1 
                else null end) as invoice_month_19_flag,
        max(case when cancelled_month <= 19 and invoice_month = 20 then 1
                when cancelled_month <= 19 then 0
                when invoice_month = 19 or invoice_month = 20 then 1
                else null end) as invoice_month_20_flag,
        max(case when cancelled_month <= 20 and invoice_month = 21 then 1
                when cancelled_month <= 20 then 0
                when invoice_month = 20 or invoice_month = 21 then 1 
                else null end) as invoice_month_21_flag,
        max(case when cancelled_month <= 21 and invoice_month = 22 then 1
                when cancelled_month <= 21 then 0
                when invoice_month = 21 or invoice_month = 22 then 1
                else null end) as invoice_month_22_flag,
        max(case when cancelled_month <= 22 and invoice_month = 23 then 1
                when cancelled_month <= 22 then 0
                when invoice_month = 22 or invoice_month = 23 then 1 
                else null end) as invoice_month_23_flag,
        max(case when cancelled_month <= 23 and invoice_month = 24 then 1
                when cancelled_month <= 23 then 0
                when invoice_month = 23 or invoice_month = 24 then 1
                else null end) as invoice_month_24_flag,
        max(case when cancelled_month <= 24 and invoice_month = 25 then 1
                when cancelled_month <= 24 then 0
                when invoice_month = 24 or invoice_month = 25 then 1 
                else null end) as invoice_month_25_flag,
        max(case when cancelled_month <= 25 and invoice_month = 26 then 1
                when cancelled_month <= 25 then 0
                when invoice_month = 25 or invoice_month = 26 then 1
                else null end) as invoice_month_26_flag,
        max(case when cancelled_month <= 26 and invoice_month = 27 then 1
                when cancelled_month <= 26 then 0
                when invoice_month = 26 or invoice_month = 27 then 1 
                else null end) as invoice_month_27_flag,
        max(case when cancelled_month <= 27 and invoice_month = 28 then 1
                when cancelled_month <= 27 then 0
                when invoice_month = 27 or invoice_month = 28 then 1
                else null end) as invoice_month_28_flag,
        max(case when cancelled_month <= 28 and invoice_month = 29 then 1
                when cancelled_month <= 28 then 0
                when invoice_month = 28 or invoice_month = 29 then 1 
                else null end) as invoice_month_29_flag,
        max(case when cancelled_month <= 29 and invoice_month = 30 then 1
                when cancelled_month <= 29 then 0
                when invoice_month = 29 or invoice_month = 30 then 1
                else null end) as invoice_month_30_flag,
        max(case when cancelled_month <= 30 and invoice_month = 31 then 1
                when cancelled_month <= 30 then 0
                when invoice_month = 30 or invoice_month = 31 then 1 
                else null end) as invoice_month_31_flag,
        max(case when cancelled_month <= 31 and invoice_month = 32 then 1
                when cancelled_month <= 31 then 0
                when invoice_month = 31 or invoice_month = 32 then 1
                else null end) as invoice_month_32_flag,
        max(case when cancelled_month <= 32 and invoice_month = 33 then 1
                when cancelled_month <= 32 then 0
                when invoice_month = 32 or invoice_month = 33 then 1 
                else null end) as invoice_month_33_flag,
        max(case when cancelled_month <= 33 and invoice_month = 34 then 1
                when cancelled_month <= 33 then 0
                when invoice_month = 33 or invoice_month = 34 then 1
                else null end) as invoice_month_34_flag,
        max(case when cancelled_month <= 34 and invoice_month = 35 then 1
                when cancelled_month <= 34 then 0
                when invoice_month = 34 or invoice_month = 35 then 1 
                else null end) as invoice_month_35_flag,
        max(case when cancelled_month <= 35 and invoice_month = 36 then 1
                when cancelled_month <= 35 then 0
                when invoice_month = 35 or invoice_month = 36 then 1
                else null end) as invoice_month_36_flag,
        max(case when cancelled_month <= 36 and invoice_month = 37 then 1
                when cancelled_month <= 36 then 0
                when invoice_month = 36 or invoice_month = 37 then 1 
                else null end) as invoice_month_37_flag,
        max(case when cancelled_month <= 37 and invoice_month = 38 then 1
                when cancelled_month <= 37 then 0
                when invoice_month = 37 or invoice_month = 38 then 1
                else null end) as invoice_month_38_flag,
        max(case when cancelled_month <= 38 and invoice_month = 39 then 1
                when cancelled_month <= 38 then 0
                when invoice_month = 38 or invoice_month = 39 then 1 
                else null end) as invoice_month_39_flag,
        max(case when cancelled_month <= 39 and invoice_month = 40 then 1
                when cancelled_month <= 39 then 0
                when invoice_month = 39 or invoice_month = 40 then 1
                else null end) as invoice_month_40_flag,
        max(case when cancelled_month <= 40 and invoice_month = 41 then 1 
                when cancelled_month <= 40 then 0
                when invoice_month = 40 or invoice_month = 41 then 1 
                else null end) as invoice_month_41_flag,
        max(case when cancelled_month <= 41 and invoice_month = 42 then 1
                when cancelled_month <= 41 then 0
                when invoice_month = 41 or invoice_month = 42 then 1
                else null end) as invoice_month_42_flag,
        max(case when cancelled_month <= 42 and invoice_month = 43 then 1
                when cancelled_month <= 42 then 0
                when invoice_month = 42 or invoice_month = 43 then 1 
                else null end) as invoice_month_43_flag,
        max(case when cancelled_month <= 43 and invoice_month = 44 then 1
                when cancelled_month <= 43 then 0
                when invoice_month = 43 or invoice_month = 44 then 1
                else null end) as invoice_month_44_flag,
        max(case when cancelled_month <= 44 and invoice_month = 45 then 1
                when cancelled_month <= 44 then 0
                when invoice_month = 44 or invoice_month = 45 then 1 
                else null end) as invoice_month_45_flag,
        max(case when cancelled_month <= 45 and invoice_month = 46 then 1
                when cancelled_month <= 45 then 0
                when invoice_month = 45 or invoice_month = 46 then 1
                else null end) as invoice_month_46_flag,
        max(case when cancelled_month <= 46 and invoice_month = 47 then 1
                when cancelled_month <= 46 then 0
                when invoice_month = 46 or invoice_month = 47 then 1 
                else null end) as invoice_month_47_flag,
        max(case when cancelled_month <= 47 and invoice_month = 48 then 1
                when cancelled_month <= 47 then 0
                when invoice_month = 47 or invoice_month = 48 then 1
                else null end) as invoice_month_48_flag,
        max(case when cancelled_month <= 48 and invoice_month = 49 then 1
                when cancelled_month <= 48 then 0
                when invoice_month = 48 or invoice_month = 49 then 1 
                else null end) as invoice_month_49_flag,
        max(case when cancelled_month <= 49 and invoice_month = 50 then 1
                when cancelled_month <= 49 then 0
                when invoice_month = 49 or invoice_month = 50 then 1
                else null end) as invoice_month_50_flag,
        max(case when cancelled_month <= 50 and invoice_month = 51 then 1
                when cancelled_month <= 50 then 0
                when invoice_month = 50 or invoice_month = 51 then 1 
                else null end) as invoice_month_51_flag,
        max(case when cancelled_month <= 51 and invoice_month = 52 then 1
                when cancelled_month <= 51 then 0
                when invoice_month = 51 or invoice_month = 52 then 1
                else null end) as invoice_month_52_flag,
        max(case when cancelled_month <= 52 and invoice_month = 53 then 1
                when cancelled_month <= 52 then 0
                when invoice_month = 52 or invoice_month = 53 then 1 
                else null end) as invoice_month_53_flag,
        max(case when cancelled_month <= 53 and invoice_month = 54 then 1
                when cancelled_month <= 53 then 0
                when invoice_month = 53 or invoice_month = 54 then 1
                else null end) as invoice_month_54_flag,
        max(case when cancelled_month <= 54 and invoice_month = 55 then 1
                when cancelled_month <= 54 then 0
                when invoice_month = 54 or invoice_month = 55 then 1 
                else null end) as invoice_month_55_flag,
        max(case when cancelled_month <= 55 and invoice_month = 56 then 1
                when cancelled_month <= 55 then 0
                when invoice_month = 55 or invoice_month = 56 then 1
                else null end) as invoice_month_56_flag,
        max(case when cancelled_month <= 56 and invoice_month = 57 then 1
                when cancelled_month <= 56 then 0
                when invoice_month = 56 or invoice_month = 57 then 1 
                else null end) as invoice_month_57_flag,
        max(case when cancelled_month <= 57 and invoice_month = 58 then 1
                when cancelled_month <= 57 then 0
                when invoice_month = 57 or invoice_month = 58 then 1
                else null end) as invoice_month_58_flag,
        max(case when cancelled_month <= 58 and invoice_month = 59 then 1
                when cancelled_month <= 58 then 0
                when invoice_month = 58 or invoice_month = 59 then 1 
                else null end) as invoice_month_59_flag,
        max(case when cancelled_month <= 59 and invoice_month = 60 then 1
                when cancelled_month <= 59 then 0
                when invoice_month = 59 or invoice_month = 60 then 1
                else null end) as invoice_month_60_flag
        
    from sub_inv
    group by recharge_subscription_id,recurly_subscription_id,activated_at
)


select agg_tbl.*,
    s.CUSTOMER_ID,
	CUSTOMER_EMAIL,
	QUANTITY,
	CANCELLED_AT,
	SKU,
	CANCELLED_PRIMARY_REASON,
	CANCELLED_SECONDARY_REASON,
	REASON_GROUP,
	ORIGINATION_PLATFORM,
	FIRST_SUBSCRIPTION_DATE_DS01,
	FIRST_SUBSCRIPTION_DATE_PDS08,
	FIRST_SUBSCRIPTION_DATE,
	LAST_SUBSCRIPTION_DATE_DS01,
	LAST_SUBSCRIPTION_DATE_PDS08,
	LAST_SUBSCRIPTION_DATE,
	FIRST_CANCEL_DATE_DS01,
	FIRST_CANCEL_DATE_PDS08,
	FIRST_CANCEL_DATE,
	LAST_CANCEL_DATE_DS01,
	LAST_CANCEL_DATE_PDS08,
	LAST_CANCEL_DATE,
	FIRST_ORDER_DATE_DS01,
	FIRST_ORDER_DATE_PDS08,
	FIRST_ORDER_DATE,
	FIRST_PRODUCT,
	FIRST_SKU,
	FIRST_QUANTITY,
	LAST_ORDER_DATE_DS01,
	LAST_ORDER_DATE_PDS08,
	LAST_ORDER_DATE,
	LAST_PRODUCT,
	LAST_SKU,
	LAST_QUANTITY,
	MONTHS_ACTIVE,
	MONTHS_ACTIVE_CUSTOMER,
	PAUSE_START_DATE,
	PAUSE_END_DATE,
	NEXT_BILL_DATE,
	FIRST_ENROLL_DATE_SRP_3MO,
	LAST_ENROLL_DATE_SRP_3MO,
	FIRST_ENROLL_DATE_SRP_6MO,
	LAST_ENROLL_DATE_SRP_6MO,
	SRP_3MO_EVER_FLAG,
	SRP_6MO_EVER_FLAG,
	SRP_3MO_CURRENTLY,
	SRP_6MO_CURRENTLY,
	REACTIVATION_FLAG 
from sub_inv_flag as agg_tbl
    left join "SEED_DATA"."DEV"."V_SUBSCRIPTION_MASTER" as s 
        on coalesce(agg_tbl.recharge_subscription_id,'')  = coalesce(s.recharge_subscription_id,'') and 
            coalesce(agg_tbl.recurly_subscription_id,'') = coalesce(s.recurly_subscription_id,'');