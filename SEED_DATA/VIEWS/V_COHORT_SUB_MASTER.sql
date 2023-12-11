create or replace view SEED_DATA.DEV.V_COHORT_SUB_MASTER as

    with all_orders as 
    (
        --select * from SEED_DATA.DEV.V_ORDER_HISTORY_SKU_ADJUSTED
        select * from SEED_DATA.DEV.V_ORDER_HISTORY_COGS_UPDATE
    ),
    
    sub_inv as 
    (
        -- joining recharge invoices
        select 
            s.recharge_subscription_id,
            s.recurly_subscription_id,
            to_date(s.activated_at) as activated_at,
            coalesce(s.first_quantity,s.quantity) as subscription_quantity,
            to_date(o.order_date) as invoice_date,
            to_date(s.cancelled_at) as cancelled_at,
            floor(datediff(days,to_date(s.activated_at),to_date(o.order_date))/30) as invoice_month,
            floor(datediff(days,to_date(s.activated_at),to_date(s.cancelled_at))/30) as cancelled_month,
            ------ dimensions 
            channel_grouping,
            channel_platform,
            ------ metrics
            o.quantity as invoiced_quantity,
            (Total_amount_paid + TOTAL_SHIPPING_COST - AMOUNT_REFUNDED) as revenue,
            (Total_amount_paid_less_cogs + TOTAL_SHIPPING_COST - AMOUNT_REFUNDED) as ltv,
            invoice_refund_flag
        from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER" as s
            join all_orders as o on s.recharge_subscription_id = o.subscription_id 
        union all 
        --- joining recurly invoices
        select 
            s.recharge_subscription_id, 
            s.recurly_subscription_id,
            to_date(s.activated_at) as activated_at,
            coalesce(s.first_quantity,s.quantity) as subscription_quantity,
            to_date(o.order_date) as invoice_date,
            to_date(s.cancelled_at) as cancelled_at,
            floor(datediff(days,to_date(s.activated_at),to_date(o.order_date))/30) as invoice_month,
            floor(datediff(days,to_date(s.activated_at),to_date(s.cancelled_at))/30) as cancelled_month,
            ------ dimensions 
            channel_grouping,
            channel_platform,
            ------ metrics
            o.quantity as invoiced_quantity,
            (Total_amount_paid + TOTAL_SHIPPING_COST - AMOUNT_REFUNDED) as revenue,
            (Total_amount_paid_less_cogs + TOTAL_SHIPPING_COST - AMOUNT_REFUNDED) as ltv,
            invoice_refund_flag
        from "SEED_DATA"."DEV"."SUBSCRIPTION_MASTER" as s
            join all_orders as o on s.recurly_subscription_id = o.subscription_id 
    ), 
    
    sub_inv_flag as
    (
        select recharge_subscription_id,recurly_subscription_id,activated_at,
            channel_grouping,
            channel_platform,
        ----- seed retention volume
            max(case when invoice_month = 0 or invoice_month = -1 then 1 else null end) as invoice_month_0_volume,
            max(case when cancelled_month <= 0 and invoice_month = 1 then 1 when cancelled_month <= 0 then 0 when invoice_month = 0 or invoice_month = 1 then 1  else null end) as invoice_month_1_volume,
            max(case when cancelled_month <= 1 and invoice_month = 2 then 1 when cancelled_month <= 1 then 0 when invoice_month = 1 or invoice_month = 2 then 1 else null end) as invoice_month_2_volume,
            max(case when cancelled_month <= 2 and invoice_month = 3 then 1 when cancelled_month <= 2 then 0 when invoice_month = 2 or invoice_month = 3 then 1  else null end) as invoice_month_3_volume,
            max(case when cancelled_month <= 3 and invoice_month = 4 then 1 when cancelled_month <= 3 then 0 when invoice_month = 3 or invoice_month = 4 then 1 else null end) as invoice_month_4_volume,
            max(case when cancelled_month <= 4 and invoice_month = 5 then 1 when cancelled_month <= 4 then 0 when invoice_month = 4 or invoice_month = 5 then 1  else null end) as invoice_month_5_volume,
            max(case when cancelled_month <= 5 and invoice_month = 6 then 1 when cancelled_month <= 5 then 0 when invoice_month = 5 or invoice_month = 6 then 1 else null end) as invoice_month_6_volume,
            max(case when cancelled_month <= 6 and invoice_month = 7 then 1 when cancelled_month <= 6 then 0 when invoice_month = 6 or invoice_month = 7 then 1  else null end) as invoice_month_7_volume,
            max(case when cancelled_month <= 7 and invoice_month = 8 then 1 when cancelled_month <= 7 then 0 when invoice_month = 7 or invoice_month = 8 then 1 else null end) as invoice_month_8_volume,
            max(case when cancelled_month <= 8 and invoice_month = 9 then 1 when cancelled_month <= 8 then 0 when invoice_month = 8 or invoice_month = 9 then 1  else null end) as invoice_month_9_volume,
            max(case when cancelled_month <= 9 and invoice_month = 10 then 1 when cancelled_month <= 9 then 0 when invoice_month = 9 or invoice_month = 10 then 1 else null end) as invoice_month_10_volume,
            max(case when cancelled_month <= 10 and invoice_month = 11 then 1 when cancelled_month <= 10 then 0 when invoice_month = 10 or invoice_month = 11 then 1 else null end) as invoice_month_11_volume,
            max(case when cancelled_month <= 11 and invoice_month = 12 then 1 when cancelled_month <= 11 then 0 when invoice_month = 11 or invoice_month = 12 then 1 else null end) as invoice_month_12_volume,
            max(case when cancelled_month <= 12 and invoice_month = 13 then 1 when cancelled_month <= 12 then 0 when invoice_month = 12 or invoice_month = 13 then 1 else null end) as invoice_month_13_volume,
            max(case when cancelled_month <= 13 and invoice_month = 14 then 1 when cancelled_month <= 13 then 0 when invoice_month = 13 or invoice_month = 14 then 1 else null end) as invoice_month_14_volume,
            max(case when cancelled_month <= 14 and invoice_month = 15 then 1 when cancelled_month <= 14 then 0 when invoice_month = 14 or invoice_month = 15 then 1 else null end) as invoice_month_15_volume,
            max(case when cancelled_month <= 15 and invoice_month = 16 then 1 when cancelled_month <= 15 then 0 when invoice_month = 15 or invoice_month = 16 then 1 else null end) as invoice_month_16_volume,
            max(case when cancelled_month <= 16 and invoice_month = 17 then 1 when cancelled_month <= 16 then 0 when invoice_month = 16 or invoice_month = 17 then 1 else null end) as invoice_month_17_volume,
            max(case when cancelled_month <= 17 and invoice_month = 18 then 1 when cancelled_month <= 17 then 0 when invoice_month = 17 or invoice_month = 18 then 1 else null end) as invoice_month_18_volume,
            max(case when cancelled_month <= 18 and invoice_month = 19 then 1 when cancelled_month <= 18 then 0 when invoice_month = 18 or invoice_month = 19 then 1 else null end) as invoice_month_19_volume,
            max(case when cancelled_month <= 19 and invoice_month = 20 then 1 when cancelled_month <= 19 then 0 when invoice_month = 19 or invoice_month = 20 then 1 else null end) as invoice_month_20_volume,
            max(case when cancelled_month <= 20 and invoice_month = 21 then 1 when cancelled_month <= 20 then 0 when invoice_month = 20 or invoice_month = 21 then 1 else null end) as invoice_month_21_volume,
            max(case when cancelled_month <= 21 and invoice_month = 22 then 1 when cancelled_month <= 21 then 0 when invoice_month = 21 or invoice_month = 22 then 1 else null end) as invoice_month_22_volume,
            max(case when cancelled_month <= 22 and invoice_month = 23 then 1 when cancelled_month <= 22 then 0 when invoice_month = 22 or invoice_month = 23 then 1 else null end) as invoice_month_23_volume,
            max(case when cancelled_month <= 23 and invoice_month = 24 then 1 when cancelled_month <= 23 then 0 when invoice_month = 23 or invoice_month = 24 then 1 else null end) as invoice_month_24_volume,
            max(case when cancelled_month <= 24 and invoice_month = 25 then 1 when cancelled_month <= 24 then 0 when invoice_month = 24 or invoice_month = 25 then 1 else null end) as invoice_month_25_volume,
            max(case when cancelled_month <= 25 and invoice_month = 26 then 1 when cancelled_month <= 25 then 0 when invoice_month = 25 or invoice_month = 26 then 1 else null end) as invoice_month_26_volume,
            max(case when cancelled_month <= 26 and invoice_month = 27 then 1 when cancelled_month <= 26 then 0 when invoice_month = 26 or invoice_month = 27 then 1 else null end) as invoice_month_27_volume,
            max(case when cancelled_month <= 27 and invoice_month = 28 then 1 when cancelled_month <= 27 then 0 when invoice_month = 27 or invoice_month = 28 then 1 else null end) as invoice_month_28_volume,
            max(case when cancelled_month <= 28 and invoice_month = 29 then 1 when cancelled_month <= 28 then 0 when invoice_month = 28 or invoice_month = 29 then 1 else null end) as invoice_month_29_volume,
            max(case when cancelled_month <= 29 and invoice_month = 30 then 1 when cancelled_month <= 29 then 0 when invoice_month = 29 or invoice_month = 30 then 1 else null end) as invoice_month_30_volume,
            max(case when cancelled_month <= 30 and invoice_month = 31 then 1 when cancelled_month <= 30 then 0 when invoice_month = 30 or invoice_month = 31 then 1 else null end) as invoice_month_31_volume,
            max(case when cancelled_month <= 31 and invoice_month = 32 then 1 when cancelled_month <= 31 then 0 when invoice_month = 31 or invoice_month = 32 then 1 else null end) as invoice_month_32_volume,
            max(case when cancelled_month <= 32 and invoice_month = 33 then 1 when cancelled_month <= 32 then 0 when invoice_month = 32 or invoice_month = 33 then 1 else null end) as invoice_month_33_volume,
            max(case when cancelled_month <= 33 and invoice_month = 34 then 1 when cancelled_month <= 33 then 0 when invoice_month = 33 or invoice_month = 34 then 1 else null end) as invoice_month_34_volume,
            max(case when cancelled_month <= 34 and invoice_month = 35 then 1 when cancelled_month <= 34 then 0 when invoice_month = 34 or invoice_month = 35 then 1 else null end) as invoice_month_35_volume,
            max(case when cancelled_month <= 35 and invoice_month = 36 then 1 when cancelled_month <= 35 then 0 when invoice_month = 35 or invoice_month = 36 then 1 else null end) as invoice_month_36_volume,
            max(case when cancelled_month <= 36 and invoice_month = 37 then 1 when cancelled_month <= 36 then 0 when invoice_month = 36 or invoice_month = 37 then 1 else null end) as invoice_month_37_volume,
            max(case when cancelled_month <= 37 and invoice_month = 38 then 1 when cancelled_month <= 37 then 0 when invoice_month = 37 or invoice_month = 38 then 1 else null end) as invoice_month_38_volume,
            max(case when cancelled_month <= 38 and invoice_month = 39 then 1 when cancelled_month <= 38 then 0 when invoice_month = 38 or invoice_month = 39 then 1 else null end) as invoice_month_39_volume,
            max(case when cancelled_month <= 39 and invoice_month = 40 then 1 when cancelled_month <= 39 then 0 when invoice_month = 39 or invoice_month = 40 then 1 else null end) as invoice_month_40_volume,
            max(case when cancelled_month <= 40 and invoice_month = 41 then 1 when cancelled_month <= 40 then 0 when invoice_month = 40 or invoice_month = 41 then 1 else null end) as invoice_month_41_volume,
            max(case when cancelled_month <= 41 and invoice_month = 42 then 1 when cancelled_month <= 41 then 0 when invoice_month = 41 or invoice_month = 42 then 1 else null end) as invoice_month_42_volume,
            max(case when cancelled_month <= 42 and invoice_month = 43 then 1 when cancelled_month <= 42 then 0 when invoice_month = 42 or invoice_month = 43 then 1 else null end) as invoice_month_43_volume,
            max(case when cancelled_month <= 43 and invoice_month = 44 then 1 when cancelled_month <= 43 then 0 when invoice_month = 43 or invoice_month = 44 then 1 else null end) as invoice_month_44_volume,
            max(case when cancelled_month <= 44 and invoice_month = 45 then 1 when cancelled_month <= 44 then 0 when invoice_month = 44 or invoice_month = 45 then 1 else null end) as invoice_month_45_volume,
            max(case when cancelled_month <= 45 and invoice_month = 46 then 1 when cancelled_month <= 45 then 0 when invoice_month = 45 or invoice_month = 46 then 1 else null end) as invoice_month_46_volume,
            max(case when cancelled_month <= 46 and invoice_month = 47 then 1 when cancelled_month <= 46 then 0 when invoice_month = 46 or invoice_month = 47 then 1 else null end) as invoice_month_47_volume,
            max(case when cancelled_month <= 47 and invoice_month = 48 then 1 when cancelled_month <= 47 then 0 when invoice_month = 47 or invoice_month = 48 then 1 else null end) as invoice_month_48_volume,
            max(case when cancelled_month <= 48 and invoice_month = 49 then 1 when cancelled_month <= 48 then 0 when invoice_month = 48 or invoice_month = 49 then 1 else null end) as invoice_month_49_volume,
            max(case when cancelled_month <= 49 and invoice_month = 50 then 1 when cancelled_month <= 49 then 0 when invoice_month = 49 or invoice_month = 50 then 1 else null end) as invoice_month_50_volume,
            max(case when cancelled_month <= 50 and invoice_month = 51 then 1 when cancelled_month <= 50 then 0 when invoice_month = 50 or invoice_month = 51 then 1 else null end) as invoice_month_51_volume,
            max(case when cancelled_month <= 51 and invoice_month = 52 then 1 when cancelled_month <= 51 then 0 when invoice_month = 51 or invoice_month = 52 then 1 else null end) as invoice_month_52_volume,
            max(case when cancelled_month <= 52 and invoice_month = 53 then 1 when cancelled_month <= 52 then 0 when invoice_month = 52 or invoice_month = 53 then 1 else null end) as invoice_month_53_volume,
            max(case when cancelled_month <= 53 and invoice_month = 54 then 1 when cancelled_month <= 53 then 0 when invoice_month = 53 or invoice_month = 54 then 1 else null end) as invoice_month_54_volume,
            max(case when cancelled_month <= 54 and invoice_month = 55 then 1 when cancelled_month <= 54 then 0 when invoice_month = 54 or invoice_month = 55 then 1 else null end) as invoice_month_55_volume,
            max(case when cancelled_month <= 55 and invoice_month = 56 then 1 when cancelled_month <= 55 then 0 when invoice_month = 55 or invoice_month = 56 then 1 else null end) as invoice_month_56_volume,
            max(case when cancelled_month <= 56 and invoice_month = 57 then 1 when cancelled_month <= 56 then 0 when invoice_month = 56 or invoice_month = 57 then 1 else null end) as invoice_month_57_volume,
            max(case when cancelled_month <= 57 and invoice_month = 58 then 1 when cancelled_month <= 57 then 0 when invoice_month = 57 or invoice_month = 58 then 1 else null end) as invoice_month_58_volume,
            max(case when cancelled_month <= 58 and invoice_month = 59 then 1 when cancelled_month <= 58 then 0 when invoice_month = 58 or invoice_month = 59 then 1 else null end) as invoice_month_59_volume,
            max(case when cancelled_month <= 59 and invoice_month = 60 then 1 when cancelled_month <= 59 then 0 when invoice_month = 59 or invoice_month = 60 then 1 else null end) as invoice_month_60_volume,
            max(case when cancelled_month <= 60 and invoice_month = 61 then 1 when cancelled_month <= 60 then 0 when invoice_month = 60 or invoice_month = 61 then 1 else null end) as invoice_month_61_volume,
            max(case when cancelled_month <= 61 and invoice_month = 62 then 1 when cancelled_month <= 61 then 0 when invoice_month = 61 or invoice_month = 62 then 1 else null end) as invoice_month_62_volume,
            max(case when cancelled_month <= 62 and invoice_month = 63 then 1 when cancelled_month <= 62 then 0 when invoice_month = 62 or invoice_month = 63 then 1 else null end) as invoice_month_63_volume,
            max(case when cancelled_month <= 63 and invoice_month = 64 then 1 when cancelled_month <= 63 then 0 when invoice_month = 63 or invoice_month = 64 then 1 else null end) as invoice_month_64_volume,
            max(case when cancelled_month <= 64 and invoice_month = 65 then 1 when cancelled_month <= 64 then 0 when invoice_month = 64 or invoice_month = 65 then 1 else null end) as invoice_month_65_volume,
            max(case when cancelled_month <= 65 and invoice_month = 66 then 1 when cancelled_month <= 65 then 0 when invoice_month = 65 or invoice_month = 66 then 1 else null end) as invoice_month_66_volume,
            max(case when cancelled_month <= 66 and invoice_month = 67 then 1 when cancelled_month <= 66 then 0 when invoice_month = 66 or invoice_month = 67 then 1 else null end) as invoice_month_67_volume,
            max(case when cancelled_month <= 67 and invoice_month = 68 then 1 when cancelled_month <= 67 then 0 when invoice_month = 67 or invoice_month = 68 then 1 else null end) as invoice_month_68_volume,
            max(case when cancelled_month <= 68 and invoice_month = 69 then 1 when cancelled_month <= 68 then 0 when invoice_month = 68 or invoice_month = 69 then 1 else null end) as invoice_month_69_volume,
            max(case when cancelled_month <= 69 and invoice_month = 70 then 1 when cancelled_month <= 69 then 0 when invoice_month = 69 or invoice_month = 70 then 1 else null end) as invoice_month_70_volume,
        ---- revenue retention
            max(case when invoice_month = 0 or invoice_month = -1 then 1 else null end) as invoice_month_0_volume_transc,
            max(case when invoice_month = 1 then 1 else null end) as invoice_month_1_volume_transc,
            max(case when invoice_month = 2 then 1 else null end) as invoice_month_2_volume_transc,
            max(case when invoice_month = 3 then 1 else null end) as invoice_month_3_volume_transc,
            max(case when invoice_month = 4 then 1 else null end) as invoice_month_4_volume_transc,
            max(case when invoice_month = 5 then 1 else null end) as invoice_month_5_volume_transc,
            max(case when invoice_month = 6 then 1 else null end) as invoice_month_6_volume_transc,
            max(case when invoice_month = 7 then 1 else null end) as invoice_month_7_volume_transc,
            max(case when invoice_month = 8 then 1 else null end) as invoice_month_8_volume_transc,
            max(case when invoice_month = 9 then 1 else null end) as invoice_month_9_volume_transc,
            max(case when invoice_month = 10 then 1 else null end) as invoice_month_10_volume_transc,
            max(case when invoice_month = 11 then 1 else null end) as invoice_month_11_volume_transc,
            max(case when invoice_month = 12 then 1 else null end) as invoice_month_12_volume_transc,
            max(case when invoice_month = 13 then 1 else null end) as invoice_month_13_volume_transc,
            max(case when invoice_month = 14 then 1 else null end) as invoice_month_14_volume_transc,
            max(case when invoice_month = 15 then 1 else null end) as invoice_month_15_volume_transc,
            max(case when invoice_month = 16 then 1 else null end) as invoice_month_16_volume_transc,
            max(case when invoice_month = 17 then 1 else null end) as invoice_month_17_volume_transc,
            max(case when invoice_month = 18 then 1 else null end) as invoice_month_18_volume_transc,
            max(case when invoice_month = 19 then 1 else null end) as invoice_month_19_volume_transc,
            max(case when invoice_month = 20 then 1 else null end) as invoice_month_20_volume_transc,
            max(case when invoice_month = 21 then 1 else null end) as invoice_month_21_volume_transc,
            max(case when invoice_month = 22 then 1 else null end) as invoice_month_22_volume_transc,
            max(case when invoice_month = 23 then 1 else null end) as invoice_month_23_volume_transc,
            max(case when invoice_month = 24 then 1 else null end) as invoice_month_24_volume_transc,
            max(case when invoice_month = 25 then 1 else null end) as invoice_month_25_volume_transc,
            max(case when invoice_month = 26 then 1 else null end) as invoice_month_26_volume_transc,
            max(case when invoice_month = 27 then 1 else null end) as invoice_month_27_volume_transc,
            max(case when invoice_month = 28 then 1 else null end) as invoice_month_28_volume_transc,
            max(case when invoice_month = 29 then 1 else null end) as invoice_month_29_volume_transc,
            max(case when invoice_month = 30 then 1 else null end) as invoice_month_30_volume_transc,
            max(case when invoice_month = 31 then 1 else null end) as invoice_month_31_volume_transc,
            max(case when invoice_month = 32 then 1 else null end) as invoice_month_32_volume_transc,
            max(case when invoice_month = 33 then 1 else null end) as invoice_month_33_volume_transc,
            max(case when invoice_month = 34 then 1 else null end) as invoice_month_34_volume_transc,
            max(case when invoice_month = 35 then 1 else null end) as invoice_month_35_volume_transc,
            max(case when invoice_month = 36 then 1 else null end) as invoice_month_36_volume_transc,
            max(case when invoice_month = 37 then 1 else null end) as invoice_month_37_volume_transc,
            max(case when invoice_month = 38 then 1 else null end) as invoice_month_38_volume_transc,
            max(case when invoice_month = 39 then 1 else null end) as invoice_month_39_volume_transc,
            max(case when invoice_month = 40 then 1 else null end) as invoice_month_40_volume_transc,
            max(case when invoice_month = 41 then 1 else null end) as invoice_month_41_volume_transc,
            max(case when invoice_month = 42 then 1 else null end) as invoice_month_42_volume_transc,
            max(case when invoice_month = 43 then 1 else null end) as invoice_month_43_volume_transc,
            max(case when invoice_month = 44 then 1 else null end) as invoice_month_44_volume_transc,
            max(case when invoice_month = 45 then 1 else null end) as invoice_month_45_volume_transc,
            max(case when invoice_month = 46 then 1 else null end) as invoice_month_46_volume_transc,
            max(case when invoice_month = 47 then 1 else null end) as invoice_month_47_volume_transc,
            max(case when invoice_month = 48 then 1 else null end) as invoice_month_48_volume_transc,
            max(case when invoice_month = 49 then 1 else null end) as invoice_month_49_volume_transc,
            max(case when invoice_month = 50 then 1 else null end) as invoice_month_50_volume_transc,
            max(case when invoice_month = 51 then 1 else null end) as invoice_month_51_volume_transc,
            max(case when invoice_month = 52 then 1 else null end) as invoice_month_52_volume_transc,
            max(case when invoice_month = 53 then 1 else null end) as invoice_month_53_volume_transc,
            max(case when invoice_month = 54 then 1 else null end) as invoice_month_54_volume_transc,
            max(case when invoice_month = 55 then 1 else null end) as invoice_month_55_volume_transc,
            max(case when invoice_month = 56 then 1 else null end) as invoice_month_56_volume_transc,
            max(case when invoice_month = 57 then 1 else null end) as invoice_month_57_volume_transc,
            max(case when invoice_month = 58 then 1 else null end) as invoice_month_58_volume_transc,
            max(case when invoice_month = 59 then 1 else null end) as invoice_month_59_volume_transc,
            max(case when invoice_month = 60 then 1 else null end) as invoice_month_60_volume_transc,
            max(case when invoice_month = 61 then 1 else null end) as invoice_month_61_volume_transc,
            max(case when invoice_month = 62 then 1 else null end) as invoice_month_62_volume_transc,
            max(case when invoice_month = 63 then 1 else null end) as invoice_month_63_volume_transc,
            max(case when invoice_month = 64 then 1 else null end) as invoice_month_64_volume_transc,
            max(case when invoice_month = 65 then 1 else null end) as invoice_month_65_volume_transc,
            max(case when invoice_month = 66 then 1 else null end) as invoice_month_66_volume_transc,
            max(case when invoice_month = 67 then 1 else null end) as invoice_month_67_volume_transc,
            max(case when invoice_month = 68 then 1 else null end) as invoice_month_68_volume_transc,
            max(case when invoice_month = 69 then 1 else null end) as invoice_month_69_volume_transc,
            max(case when invoice_month = 70 then 1 else null end) as invoice_month_70_volume_transc,
        ---- revenue 
            sum(case when invoice_month = 0 or invoice_month = -1 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_0_revenue,
            sum(case when invoice_month = 1 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_1_revenue,
            sum(case when invoice_month = 2 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_2_revenue,
            sum(case when invoice_month = 3 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_3_revenue,
            sum(case when invoice_month = 4 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_4_revenue,
            sum(case when invoice_month = 5 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_5_revenue,
            sum(case when invoice_month = 6 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_6_revenue,
            sum(case when invoice_month = 7 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_7_revenue,
            sum(case when invoice_month = 8 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_8_revenue,
            sum(case when invoice_month = 9 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_9_revenue,
            sum(case when invoice_month = 10 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_10_revenue,
            sum(case when invoice_month = 11 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_11_revenue,
            sum(case when invoice_month = 12 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_12_revenue,
            sum(case when invoice_month = 13 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_13_revenue,
            sum(case when invoice_month = 14 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_14_revenue,
            sum(case when invoice_month = 15 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_15_revenue,
            sum(case when invoice_month = 16 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_16_revenue,
            sum(case when invoice_month = 17 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_17_revenue,
            sum(case when invoice_month = 18 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_18_revenue,
            sum(case when invoice_month = 19 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_19_revenue,
            sum(case when invoice_month = 20 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_20_revenue,
            sum(case when invoice_month = 21 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_21_revenue,
            sum(case when invoice_month = 22 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_22_revenue,
            sum(case when invoice_month = 23 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_23_revenue,
            sum(case when invoice_month = 24 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_24_revenue,
            sum(case when invoice_month = 25 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_25_revenue,
            sum(case when invoice_month = 26 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_26_revenue,
            sum(case when invoice_month = 27 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_27_revenue,
            sum(case when invoice_month = 28 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_28_revenue,
            sum(case when invoice_month = 29 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_29_revenue,
            sum(case when invoice_month = 30 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_30_revenue,
            sum(case when invoice_month = 31 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_31_revenue,
            sum(case when invoice_month = 32 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_32_revenue,
            sum(case when invoice_month = 33 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_33_revenue,
            sum(case when invoice_month = 34 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_34_revenue,
            sum(case when invoice_month = 35 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_35_revenue,
            sum(case when invoice_month = 36 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_36_revenue,
            sum(case when invoice_month = 37 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_37_revenue,
            sum(case when invoice_month = 38 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_38_revenue,
            sum(case when invoice_month = 39 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_39_revenue,
            sum(case when invoice_month = 40 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_40_revenue,
            sum(case when invoice_month = 41 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_41_revenue,
            sum(case when invoice_month = 42 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_42_revenue,
            sum(case when invoice_month = 43 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_43_revenue,
            sum(case when invoice_month = 44 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_44_revenue,
            sum(case when invoice_month = 45 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_45_revenue,
            sum(case when invoice_month = 46 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_46_revenue,
            sum(case when invoice_month = 47 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_47_revenue,
            sum(case when invoice_month = 48 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_48_revenue,
            sum(case when invoice_month = 49 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_49_revenue,
            sum(case when invoice_month = 50 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_50_revenue,
            sum(case when invoice_month = 51 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_51_revenue,
            sum(case when invoice_month = 52 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_52_revenue,
            sum(case when invoice_month = 53 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_53_revenue,
            sum(case when invoice_month = 54 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_54_revenue,
            sum(case when invoice_month = 55 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_55_revenue,
            sum(case when invoice_month = 56 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_56_revenue,
            sum(case when invoice_month = 57 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_57_revenue,
            sum(case when invoice_month = 58 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_58_revenue,
            sum(case when invoice_month = 59 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as  invoice_month_59_revenue,
            sum(case when invoice_month = 60 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_60_revenue,
            sum(case when invoice_month = 61 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_61_revenue,
            sum(case when invoice_month = 62 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_62_revenue,
            sum(case when invoice_month = 63 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_63_revenue,
            sum(case when invoice_month = 64 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_64_revenue,
            sum(case when invoice_month = 65 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_65_revenue,
            sum(case when invoice_month = 66 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_66_revenue,
            sum(case when invoice_month = 67 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_67_revenue,
            sum(case when invoice_month = 68 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_68_revenue,
            sum(case when invoice_month = 69 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_69_revenue,
            sum(case when invoice_month = 70 and invoice_refund_flag = 'not_fully_refunded' then revenue else 0 end) as invoice_month_70_revenue,
        ----- cumulative revenue
            sum(case when invoice_month = 0 or invoice_month = -1 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_0_revenue_cuml,
            sum(case when invoice_month <= 1 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_1_revenue_cuml,
            sum(case when invoice_month <= 2 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_2_revenue_cuml,
            sum(case when invoice_month <= 3 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_3_revenue_cuml,
            sum(case when invoice_month <= 4 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_4_revenue_cuml,
            sum(case when invoice_month <= 5 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_5_revenue_cuml,
            sum(case when invoice_month <= 6 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_6_revenue_cuml,
            sum(case when invoice_month <= 7 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_7_revenue_cuml,
            sum(case when invoice_month <= 8 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_8_revenue_cuml,
            sum(case when invoice_month <= 9 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_9_revenue_cuml,
            sum(case when invoice_month <= 10 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_10_revenue_cuml,
            sum(case when invoice_month <= 11 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_11_revenue_cuml,
            sum(case when invoice_month <= 12 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_12_revenue_cuml,
            sum(case when invoice_month <= 13 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_13_revenue_cuml,
            sum(case when invoice_month <= 14 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_14_revenue_cuml,
            sum(case when invoice_month <= 15 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_15_revenue_cuml,
            sum(case when invoice_month <= 16 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_16_revenue_cuml,
            sum(case when invoice_month <= 17 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_17_revenue_cuml,
            sum(case when invoice_month <= 18 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_18_revenue_cuml,
            sum(case when invoice_month <= 19 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_19_revenue_cuml,
            sum(case when invoice_month <= 20 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_20_revenue_cuml,
            sum(case when invoice_month <= 21 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_21_revenue_cuml,
            sum(case when invoice_month <= 22 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_22_revenue_cuml,
            sum(case when invoice_month <= 23 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_23_revenue_cuml,
            sum(case when invoice_month <= 24 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_24_revenue_cuml,
            sum(case when invoice_month <= 25 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_25_revenue_cuml,
            sum(case when invoice_month <= 26 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_26_revenue_cuml,
            sum(case when invoice_month <= 27 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_27_revenue_cuml,
            sum(case when invoice_month <= 28 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_28_revenue_cuml,
            sum(case when invoice_month <= 29 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_29_revenue_cuml,
            sum(case when invoice_month <= 30 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_30_revenue_cuml,
            sum(case when invoice_month <= 31 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_31_revenue_cuml,
            sum(case when invoice_month <= 32 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_32_revenue_cuml,
            sum(case when invoice_month <= 33 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_33_revenue_cuml,
            sum(case when invoice_month <= 34 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_34_revenue_cuml,
            sum(case when invoice_month <= 35 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_35_revenue_cuml,
            sum(case when invoice_month <= 36 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_36_revenue_cuml,
            sum(case when invoice_month <= 37 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_37_revenue_cuml,
            sum(case when invoice_month <= 38 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_38_revenue_cuml,
            sum(case when invoice_month <= 39 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_39_revenue_cuml,
            sum(case when invoice_month <= 40 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_40_revenue_cuml,
            sum(case when invoice_month <= 41 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_41_revenue_cuml,
            sum(case when invoice_month <= 42 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_42_revenue_cuml,
            sum(case when invoice_month <= 43 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_43_revenue_cuml,
            sum(case when invoice_month <= 44 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_44_revenue_cuml,
            sum(case when invoice_month <= 45 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_45_revenue_cuml,
            sum(case when invoice_month <= 46 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_46_revenue_cuml,
            sum(case when invoice_month <= 47 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_47_revenue_cuml,
            sum(case when invoice_month <= 48 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_48_revenue_cuml,
            sum(case when invoice_month <= 49 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_49_revenue_cuml,
            sum(case when invoice_month <= 50 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_50_revenue_cuml,
            sum(case when invoice_month <= 51 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_51_revenue_cuml,
            sum(case when invoice_month <= 52 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_52_revenue_cuml,
            sum(case when invoice_month <= 53 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_53_revenue_cuml,
            sum(case when invoice_month <= 54 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_54_revenue_cuml,
            sum(case when invoice_month <= 55 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_55_revenue_cuml,
            sum(case when invoice_month <= 56 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_56_revenue_cuml,
            sum(case when invoice_month <= 57 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_57_revenue_cuml,
            sum(case when invoice_month <= 58 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_58_revenue_cuml,
            sum(case when invoice_month <= 59 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_59_revenue_cuml,
            sum(case when invoice_month <= 60 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_60_revenue_cuml,
            sum(case when invoice_month <= 61 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_61_revenue_cuml,
            sum(case when invoice_month <= 62 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_62_revenue_cuml,
            sum(case when invoice_month <= 63 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_63_revenue_cuml,
            sum(case when invoice_month <= 64 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_64_revenue_cuml,
            sum(case when invoice_month <= 65 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_65_revenue_cuml,
            sum(case when invoice_month <= 66 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_66_revenue_cuml,
            sum(case when invoice_month <= 67 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_67_revenue_cuml,
            sum(case when invoice_month <= 68 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_68_revenue_cuml,
            sum(case when invoice_month <= 69 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as  invoice_month_69_revenue_cuml,
            sum(case when invoice_month <= 70 and invoice_refund_flag = 'not_fully_refunded' then revenue  else 0 end) as invoice_month_70_revenue_cuml,
        ----- ltv
            sum(case when invoice_month = 0 or invoice_month = -1 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_0_ltv,
            sum(case when invoice_month = 1 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_1_ltv,
            sum(case when invoice_month = 2 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_2_ltv,
            sum(case when invoice_month = 3 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_3_ltv,
            sum(case when invoice_month = 4 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_4_ltv,
            sum(case when invoice_month = 5 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_5_ltv,
            sum(case when invoice_month = 6 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_6_ltv,
            sum(case when invoice_month = 7 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_7_ltv,
            sum(case when invoice_month = 8 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_8_ltv,
            sum(case when invoice_month = 9 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_9_ltv,
            sum(case when invoice_month = 10 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_10_ltv,
            sum(case when invoice_month = 11 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_11_ltv,
            sum(case when invoice_month = 12 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_12_ltv,
            sum(case when invoice_month = 13 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_13_ltv,
            sum(case when invoice_month = 14 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_14_ltv,
            sum(case when invoice_month = 15 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_15_ltv,
            sum(case when invoice_month = 16 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_16_ltv,
            sum(case when invoice_month = 17 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_17_ltv,
            sum(case when invoice_month = 18 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_18_ltv,
            sum(case when invoice_month = 19 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_19_ltv,
            sum(case when invoice_month = 20 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_20_ltv,
            sum(case when invoice_month = 21 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_21_ltv,
            sum(case when invoice_month = 22 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_22_ltv,
            sum(case when invoice_month = 23 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_23_ltv,
            sum(case when invoice_month = 24 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_24_ltv,
            sum(case when invoice_month = 25 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_25_ltv,
            sum(case when invoice_month = 26 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_26_ltv,
            sum(case when invoice_month = 27 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_27_ltv,
            sum(case when invoice_month = 28 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_28_ltv,
            sum(case when invoice_month = 29 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_29_ltv,
            sum(case when invoice_month = 30 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_30_ltv,
            sum(case when invoice_month = 31 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_31_ltv,
            sum(case when invoice_month = 32 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_32_ltv,
            sum(case when invoice_month = 33 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_33_ltv,
            sum(case when invoice_month = 34 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_34_ltv,
            sum(case when invoice_month = 35 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_35_ltv,
            sum(case when invoice_month = 36 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_36_ltv,
            sum(case when invoice_month = 37 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_37_ltv,
            sum(case when invoice_month = 38 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_38_ltv,
            sum(case when invoice_month = 39 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_39_ltv,
            sum(case when invoice_month = 40 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_40_ltv,
            sum(case when invoice_month = 41 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_41_ltv,
            sum(case when invoice_month = 42 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_42_ltv,
            sum(case when invoice_month = 43 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_43_ltv,
            sum(case when invoice_month = 44 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_44_ltv,
            sum(case when invoice_month = 45 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_45_ltv,
            sum(case when invoice_month = 46 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_46_ltv,
            sum(case when invoice_month = 47 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_47_ltv,
            sum(case when invoice_month = 48 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_48_ltv,
            sum(case when invoice_month = 49 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_49_ltv,
            sum(case when invoice_month = 50 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_50_ltv,
            sum(case when invoice_month = 51 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_51_ltv,
            sum(case when invoice_month = 52 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_52_ltv,
            sum(case when invoice_month = 53 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_53_ltv,
            sum(case when invoice_month = 54 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_54_ltv,
            sum(case when invoice_month = 55 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_55_ltv,
            sum(case when invoice_month = 56 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_56_ltv,
            sum(case when invoice_month = 57 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_57_ltv,
            sum(case when invoice_month = 58 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_58_ltv,
            sum(case when invoice_month = 59 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_59_ltv,
            sum(case when invoice_month = 60 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_60_ltv,
            sum(case when invoice_month = 61 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_61_ltv,
            sum(case when invoice_month = 62 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_62_ltv,
            sum(case when invoice_month = 63 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_63_ltv,
            sum(case when invoice_month = 64 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_64_ltv,
            sum(case when invoice_month = 65 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_65_ltv,
            sum(case when invoice_month = 66 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_66_ltv,
            sum(case when invoice_month = 67 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_67_ltv,
            sum(case when invoice_month = 68 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_68_ltv,
            sum(case when invoice_month = 69 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_69_ltv,
            sum(case when invoice_month = 70 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_70_ltv,
            ----- cumulative ltv
            sum(case when invoice_month = 0 or invoice_month = -1 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_0_ltv_cuml,
            sum(case when invoice_month <= 1 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_1_ltv_cuml,
            sum(case when invoice_month <= 2 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_2_ltv_cuml,
            sum(case when invoice_month <= 3 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_3_ltv_cuml,
            sum(case when invoice_month <= 4 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_4_ltv_cuml,
            sum(case when invoice_month <= 5 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_5_ltv_cuml,
            sum(case when invoice_month <= 6 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_6_ltv_cuml,
            sum(case when invoice_month <= 7 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_7_ltv_cuml,
            sum(case when invoice_month <= 8 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_8_ltv_cuml,
            sum(case when invoice_month <= 9 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_9_ltv_cuml,
            sum(case when invoice_month <= 10 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_10_ltv_cuml,
            sum(case when invoice_month <= 11 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_11_ltv_cuml,
            sum(case when invoice_month <= 12 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_12_ltv_cuml,
            sum(case when invoice_month <= 13 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_13_ltv_cuml,
            sum(case when invoice_month <= 14 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_14_ltv_cuml,
            sum(case when invoice_month <= 15 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_15_ltv_cuml,
            sum(case when invoice_month <= 16 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_16_ltv_cuml,
            sum(case when invoice_month <= 17 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_17_ltv_cuml,
            sum(case when invoice_month <= 18 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_18_ltv_cuml,
            sum(case when invoice_month <= 19 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_19_ltv_cuml,
            sum(case when invoice_month <= 20 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_20_ltv_cuml,
            sum(case when invoice_month <= 21 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_21_ltv_cuml,
            sum(case when invoice_month <= 22 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_22_ltv_cuml,
            sum(case when invoice_month <= 23 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_23_ltv_cuml,
            sum(case when invoice_month <= 24 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_24_ltv_cuml,
            sum(case when invoice_month <= 25 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_25_ltv_cuml,
            sum(case when invoice_month <= 26 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_26_ltv_cuml,
            sum(case when invoice_month <= 27 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_27_ltv_cuml,
            sum(case when invoice_month <= 28 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_28_ltv_cuml,
            sum(case when invoice_month <= 29 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_29_ltv_cuml,
            sum(case when invoice_month <= 30 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_30_ltv_cuml,
            sum(case when invoice_month <= 31 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_31_ltv_cuml,
            sum(case when invoice_month <= 32 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_32_ltv_cuml,
            sum(case when invoice_month <= 33 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_33_ltv_cuml,
            sum(case when invoice_month <= 34 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_34_ltv_cuml,
            sum(case when invoice_month <= 35 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_35_ltv_cuml,
            sum(case when invoice_month <= 36 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_36_ltv_cuml,
            sum(case when invoice_month <= 37 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_37_ltv_cuml,
            sum(case when invoice_month <= 38 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_38_ltv_cuml,
            sum(case when invoice_month <= 39 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_39_ltv_cuml,
            sum(case when invoice_month <= 40 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_40_ltv_cuml,
            sum(case when invoice_month <= 41 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_41_ltv_cuml,
            sum(case when invoice_month <= 42 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_42_ltv_cuml,
            sum(case when invoice_month <= 43 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_43_ltv_cuml,
            sum(case when invoice_month <= 44 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_44_ltv_cuml,
            sum(case when invoice_month <= 45 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_45_ltv_cuml,
            sum(case when invoice_month <= 46 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_46_ltv_cuml,
            sum(case when invoice_month <= 47 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_47_ltv_cuml,
            sum(case when invoice_month <= 48 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_48_ltv_cuml,
            sum(case when invoice_month <= 49 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_49_ltv_cuml,
            sum(case when invoice_month <= 50 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_50_ltv_cuml,
            sum(case when invoice_month <= 51 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_51_ltv_cuml,
            sum(case when invoice_month <= 52 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_52_ltv_cuml,
            sum(case when invoice_month <= 53 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_53_ltv_cuml,
            sum(case when invoice_month <= 54 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_54_ltv_cuml,
            sum(case when invoice_month <= 55 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_55_ltv_cuml,
            sum(case when invoice_month <= 56 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_56_ltv_cuml,
            sum(case when invoice_month <= 57 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_57_ltv_cuml,
            sum(case when invoice_month <= 58 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_58_ltv_cuml,
            sum(case when invoice_month <= 59 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_59_ltv_cuml,
            sum(case when invoice_month <= 60 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_60_ltv_cuml,
            sum(case when invoice_month <= 61 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_61_ltv_cuml,
            sum(case when invoice_month <= 62 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_62_ltv_cuml,
            sum(case when invoice_month <= 63 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_63_ltv_cuml,
            sum(case when invoice_month <= 64 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_64_ltv_cuml,
            sum(case when invoice_month <= 65 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_65_ltv_cuml,
            sum(case when invoice_month <= 66 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_66_ltv_cuml,
            sum(case when invoice_month <= 67 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_67_ltv_cuml,
            sum(case when invoice_month <= 68 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_68_ltv_cuml,
            sum(case when invoice_month <= 69 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as  invoice_month_69_ltv_cuml,
            sum(case when invoice_month <= 70 and invoice_refund_flag = 'not_fully_refunded' then ltv  else 0 end) as invoice_month_70_ltv_cuml
            
        from sub_inv
        group by 1,2,3,4,5
    ), 
    
    final as
    (select 
        date_trunc('month',to_date(activated_at)) as created_month_year,
        channel_grouping, channel_platform,
        count(*) as cohort_volume,
        ----- seed retention volume
        sum(invoice_month_0_volume) as "M0 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 1 then null else invoice_month_1_volume end) as "M01 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 2 then null else invoice_month_2_volume end) as "M02 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 3 then null else invoice_month_3_volume end) as "M03 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 4 then null else invoice_month_4_volume end) as "M04 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 5 then null else invoice_month_5_volume end) as "M05 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 6 then null else invoice_month_6_volume end) as "M06 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 7 then null else invoice_month_7_volume end) as "M07 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 8 then null else invoice_month_8_volume end) as "M08 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 9 then null else invoice_month_9_volume end) as "M09 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 10 then null else invoice_month_10_volume end) as "M10 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 11 then null else invoice_month_11_volume end) as "M11 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 12 then null else invoice_month_12_volume end) as "M12 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 13 then null else invoice_month_13_volume end) as "M13 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 14 then null else invoice_month_14_volume end) as "M14 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 15 then null else invoice_month_15_volume end) as "M15 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 16 then null else invoice_month_16_volume end) as "M16 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 17 then null else invoice_month_17_volume end) as "M17 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 18 then null else invoice_month_18_volume end) as "M18 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 19 then null else invoice_month_19_volume end) as "M19 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 20 then null else invoice_month_20_volume end) as "M20 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 21 then null else invoice_month_21_volume end) as "M21 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 22 then null else invoice_month_22_volume end) as "M22 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 23 then null else invoice_month_23_volume end) as "M23 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 24 then null else invoice_month_24_volume end) as "M24 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 25 then null else invoice_month_25_volume end) as "M25 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 26 then null else invoice_month_26_volume end) as "M26 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 27 then null else invoice_month_27_volume end) as "M27 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 28 then null else invoice_month_28_volume end) as "M28 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 29 then null else invoice_month_29_volume end) as "M29 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 30 then null else invoice_month_30_volume end) as "M30 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 31 then null else invoice_month_31_volume end) as "M31 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 32 then null else invoice_month_32_volume end) as "M32 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 33 then null else invoice_month_33_volume end) as "M33 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 34 then null else invoice_month_34_volume end) as "M34 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 35 then null else invoice_month_35_volume end) as "M35 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 36 then null else invoice_month_36_volume end) as "M36 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 37 then null else invoice_month_37_volume end) as "M37 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 38 then null else invoice_month_38_volume end) as "M38 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 39 then null else invoice_month_39_volume end) as "M39 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 40 then null else invoice_month_40_volume end) as "M40 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 41 then null else invoice_month_41_volume end) as "M41 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 42 then null else invoice_month_42_volume end) as "M42 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 43 then null else invoice_month_43_volume end) as "M43 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 44 then null else invoice_month_44_volume end) as "M44 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 45 then null else invoice_month_45_volume end) as "M45 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 46 then null else invoice_month_46_volume end) as "M46 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 47 then null else invoice_month_47_volume end) as "M47 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 48 then null else invoice_month_48_volume end) as "M48 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 49 then null else invoice_month_49_volume end) as "M49 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 50 then null else invoice_month_50_volume end) as "M50 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 51 then null else invoice_month_51_volume end) as "M51 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 52 then null else invoice_month_52_volume end) as "M52 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 53 then null else invoice_month_53_volume end) as "M53 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 54 then null else invoice_month_54_volume end) as "M54 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 55 then null else invoice_month_55_volume end) as "M55 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 56 then null else invoice_month_56_volume end) as "M56 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 57 then null else invoice_month_57_volume end) as "M57 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 58 then null else invoice_month_58_volume end) as "M58 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 59 then null else invoice_month_59_volume end) as "M59 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 60 then null else invoice_month_60_volume end) as "M60 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 61 then null else invoice_month_61_volume end) as "M61 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 62 then null else invoice_month_62_volume end) as "M62 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 63 then null else invoice_month_63_volume end) as "M63 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 64 then null else invoice_month_64_volume end) as "M64 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 65 then null else invoice_month_65_volume end) as "M65 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 66 then null else invoice_month_66_volume end) as "M66 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 67 then null else invoice_month_67_volume end) as "M67 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 68 then null else invoice_month_68_volume end) as "M68 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 69 then null else invoice_month_69_volume end) as "M69 Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 70 then null else invoice_month_70_volume end) as "M70 Vol",
        ----- transactional retention volume
        sum(invoice_month_0_volume_transc) as "M0 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 1 then null else invoice_month_1_volume_transc end) as "M01 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 2 then null else invoice_month_2_volume_transc end) as "M02 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 3 then null else invoice_month_3_volume_transc end) as "M03 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 4 then null else invoice_month_4_volume_transc end) as "M04 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 5 then null else invoice_month_5_volume_transc end) as "M05 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 6 then null else invoice_month_6_volume_transc end) as "M06 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 7 then null else invoice_month_7_volume_transc end) as "M07 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 8 then null else invoice_month_8_volume_transc end) as "M08 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 9 then null else invoice_month_9_volume_transc end) as "M09 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 10 then null else invoice_month_10_volume_transc end) as "M10 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 11 then null else invoice_month_11_volume_transc end) as "M11 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 12 then null else invoice_month_12_volume_transc end) as "M12 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 13 then null else invoice_month_13_volume_transc end) as "M13 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 14 then null else invoice_month_14_volume_transc end) as "M14 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 15 then null else invoice_month_15_volume_transc end) as "M15 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 16 then null else invoice_month_16_volume_transc end) as "M16 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 17 then null else invoice_month_17_volume_transc end) as "M17 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 18 then null else invoice_month_18_volume_transc end) as "M18 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 19 then null else invoice_month_19_volume_transc end) as "M19 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 20 then null else invoice_month_20_volume_transc end) as "M20 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 21 then null else invoice_month_21_volume_transc end) as "M21 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 22 then null else invoice_month_22_volume_transc end) as "M22 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 23 then null else invoice_month_23_volume_transc end) as "M23 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 24 then null else invoice_month_24_volume_transc end) as "M24 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 25 then null else invoice_month_25_volume_transc end) as "M25 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 26 then null else invoice_month_26_volume_transc end) as "M26 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 27 then null else invoice_month_27_volume_transc end) as "M27 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 28 then null else invoice_month_28_volume_transc end) as "M28 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 29 then null else invoice_month_29_volume_transc end) as "M29 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 30 then null else invoice_month_30_volume_transc end) as "M30 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 31 then null else invoice_month_31_volume_transc end) as "M31 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 32 then null else invoice_month_32_volume_transc end) as "M32 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 33 then null else invoice_month_33_volume_transc end) as "M33 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 34 then null else invoice_month_34_volume_transc end) as "M34 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 35 then null else invoice_month_35_volume_transc end) as "M35 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 36 then null else invoice_month_36_volume_transc end) as "M36 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 37 then null else invoice_month_37_volume_transc end) as "M37 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 38 then null else invoice_month_38_volume_transc end) as "M38 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 39 then null else invoice_month_39_volume_transc end) as "M39 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 40 then null else invoice_month_40_volume_transc end) as "M40 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 41 then null else invoice_month_41_volume_transc end) as "M41 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 42 then null else invoice_month_42_volume_transc end) as "M42 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 43 then null else invoice_month_43_volume_transc end) as "M43 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 44 then null else invoice_month_44_volume_transc end) as "M44 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 45 then null else invoice_month_45_volume_transc end) as "M45 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 46 then null else invoice_month_46_volume_transc end) as "M46 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 47 then null else invoice_month_47_volume_transc end) as "M47 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 48 then null else invoice_month_48_volume_transc end) as "M48 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 49 then null else invoice_month_49_volume_transc end) as "M49 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 50 then null else invoice_month_50_volume_transc end) as "M50 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 51 then null else invoice_month_51_volume_transc end) as "M51 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 52 then null else invoice_month_52_volume_transc end) as "M52 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 53 then null else invoice_month_53_volume_transc end) as "M53 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 54 then null else invoice_month_54_volume_transc end) as "M54 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 55 then null else invoice_month_55_volume_transc end) as "M55 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 56 then null else invoice_month_56_volume_transc end) as "M56 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 57 then null else invoice_month_57_volume_transc end) as "M57 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 58 then null else invoice_month_58_volume_transc end) as "M58 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 59 then null else invoice_month_59_volume_transc end) as "M59 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 60 then null else invoice_month_60_volume_transc end) as "M60 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 61 then null else invoice_month_61_volume_transc end) as "M61 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 62 then null else invoice_month_62_volume_transc end) as "M62 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 63 then null else invoice_month_63_volume_transc end) as "M63 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 64 then null else invoice_month_64_volume_transc end) as "M64 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 65 then null else invoice_month_65_volume_transc end) as "M65 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 66 then null else invoice_month_66_volume_transc end) as "M66 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 67 then null else invoice_month_67_volume_transc end) as "M67 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 68 then null else invoice_month_68_volume_transc end) as "M68 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 69 then null else invoice_month_69_volume_transc end) as "M69 Trnsc Vol",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 70 then null else invoice_month_70_volume_transc end) as "M70 Trnsc Vol",
        ----- revenue
        sum(invoice_month_0_revenue) as "M0 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 1 then null else invoice_month_1_revenue end) as "M01 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 2 then null else invoice_month_2_revenue end) as "M02 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 3 then null else invoice_month_3_revenue end) as "M03 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 4 then null else invoice_month_4_revenue end) as "M04 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 5 then null else invoice_month_5_revenue end) as "M05 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 6 then null else invoice_month_6_revenue end) as "M06 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 7 then null else invoice_month_7_revenue end) as "M07 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 8 then null else invoice_month_8_revenue end) as "M08 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 9 then null else invoice_month_9_revenue end) as "M09 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 10 then null else invoice_month_10_revenue end) as "M10 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 11 then null else invoice_month_11_revenue end) as "M11 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 12 then null else invoice_month_12_revenue end) as "M12 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 13 then null else invoice_month_13_revenue end) as "M13 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 14 then null else invoice_month_14_revenue end) as "M14 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 15 then null else invoice_month_15_revenue end) as "M15 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 16 then null else invoice_month_16_revenue end) as "M16 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 17 then null else invoice_month_17_revenue end) as "M17 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 18 then null else invoice_month_18_revenue end) as "M18 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 19 then null else invoice_month_19_revenue end) as "M19 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 20 then null else invoice_month_20_revenue end) as "M20 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 21 then null else invoice_month_21_revenue end) as "M21 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 22 then null else invoice_month_22_revenue end) as "M22 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 23 then null else invoice_month_23_revenue end) as "M23 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 24 then null else invoice_month_24_revenue end) as "M24 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 25 then null else invoice_month_25_revenue end) as "M25 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 26 then null else invoice_month_26_revenue end) as "M26 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 27 then null else invoice_month_27_revenue end) as "M27 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 28 then null else invoice_month_28_revenue end) as "M28 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 29 then null else invoice_month_29_revenue end) as "M29 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 30 then null else invoice_month_30_revenue end) as "M30 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 31 then null else invoice_month_31_revenue end) as "M31 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 32 then null else invoice_month_32_revenue end) as "M32 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 33 then null else invoice_month_33_revenue end) as "M33 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 34 then null else invoice_month_34_revenue end) as "M34 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 35 then null else invoice_month_35_revenue end) as "M35 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 36 then null else invoice_month_36_revenue end) as "M36 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 37 then null else invoice_month_37_revenue end) as "M37 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 38 then null else invoice_month_38_revenue end) as "M38 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 39 then null else invoice_month_39_revenue end) as "M39 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 40 then null else invoice_month_40_revenue end) as "M40 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 41 then null else invoice_month_41_revenue end) as "M41 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 42 then null else invoice_month_42_revenue end) as "M42 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 43 then null else invoice_month_43_revenue end) as "M43 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 44 then null else invoice_month_44_revenue end) as "M44 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 45 then null else invoice_month_45_revenue end) as "M45 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 46 then null else invoice_month_46_revenue end) as "M46 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 47 then null else invoice_month_47_revenue end) as "M47 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 48 then null else invoice_month_48_revenue end) as "M48 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 49 then null else invoice_month_49_revenue end) as "M49 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 50 then null else invoice_month_50_revenue end) as "M50 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 51 then null else invoice_month_51_revenue end) as "M51 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 52 then null else invoice_month_52_revenue end) as "M52 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 53 then null else invoice_month_53_revenue end) as "M53 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 54 then null else invoice_month_54_revenue end) as "M54 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 55 then null else invoice_month_55_revenue end) as "M55 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 56 then null else invoice_month_56_revenue end) as "M56 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 57 then null else invoice_month_57_revenue end) as "M57 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 58 then null else invoice_month_58_revenue end) as "M58 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 59 then null else invoice_month_59_revenue end) as "M59 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 60 then null else invoice_month_60_revenue end) as "M60 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 61 then null else invoice_month_61_revenue end) as "M61 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 62 then null else invoice_month_62_revenue end) as "M62 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 63 then null else invoice_month_63_revenue end) as "M63 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 64 then null else invoice_month_64_revenue end) as "M64 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 65 then null else invoice_month_65_revenue end) as "M65 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 66 then null else invoice_month_66_revenue end) as "M66 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 67 then null else invoice_month_67_revenue end) as "M67 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 68 then null else invoice_month_68_revenue end) as "M68 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 69 then null else invoice_month_69_revenue end) as "M69 Rev $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 70 then null else invoice_month_70_revenue end) as "M70 Rev $",
        ----- cumulative revenue
        sum(invoice_month_0_revenue_cuml) as "M0 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 1 then null else invoice_month_1_revenue_cuml end) as "M01 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 2 then null else invoice_month_2_revenue_cuml end) as "M02 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 3 then null else invoice_month_3_revenue_cuml end) as "M03 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 4 then null else invoice_month_4_revenue_cuml end) as "M04 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 5 then null else invoice_month_5_revenue_cuml end) as "M05 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 6 then null else invoice_month_6_revenue_cuml end) as "M06 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 7 then null else invoice_month_7_revenue_cuml end) as "M07 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 8 then null else invoice_month_8_revenue_cuml end) as "M08 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 9 then null else invoice_month_9_revenue_cuml end) as "M09 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 10 then null else invoice_month_10_revenue_cuml end) as "M10 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 11 then null else invoice_month_11_revenue_cuml end) as "M11 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 12 then null else invoice_month_12_revenue_cuml end) as "M12 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 13 then null else invoice_month_13_revenue_cuml end) as "M13 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 14 then null else invoice_month_14_revenue_cuml end) as "M14 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 15 then null else invoice_month_15_revenue_cuml end) as "M15 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 16 then null else invoice_month_16_revenue_cuml end) as "M16 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 17 then null else invoice_month_17_revenue_cuml end) as "M17 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 18 then null else invoice_month_18_revenue_cuml end) as "M18 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 19 then null else invoice_month_19_revenue_cuml end) as "M19 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 20 then null else invoice_month_20_revenue_cuml end) as "M20 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 21 then null else invoice_month_21_revenue_cuml end) as "M21 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 22 then null else invoice_month_22_revenue_cuml end) as "M22 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 23 then null else invoice_month_23_revenue_cuml end) as "M23 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 24 then null else invoice_month_24_revenue_cuml end) as "M24 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 25 then null else invoice_month_25_revenue_cuml end) as "M25 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 26 then null else invoice_month_26_revenue_cuml end) as "M26 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 27 then null else invoice_month_27_revenue_cuml end) as "M27 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 28 then null else invoice_month_28_revenue_cuml end) as "M28 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 29 then null else invoice_month_29_revenue_cuml end) as "M29 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 30 then null else invoice_month_30_revenue_cuml end) as "M30 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 31 then null else invoice_month_31_revenue_cuml end) as "M31 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 32 then null else invoice_month_32_revenue_cuml end) as "M32 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 33 then null else invoice_month_33_revenue_cuml end) as "M33 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 34 then null else invoice_month_34_revenue_cuml end) as "M34 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 35 then null else invoice_month_35_revenue_cuml end) as "M35 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 36 then null else invoice_month_36_revenue_cuml end) as "M36 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 37 then null else invoice_month_37_revenue_cuml end) as "M37 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 38 then null else invoice_month_38_revenue_cuml end) as "M38 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 39 then null else invoice_month_39_revenue_cuml end) as "M39 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 40 then null else invoice_month_40_revenue_cuml end) as "M40 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 41 then null else invoice_month_41_revenue_cuml end) as "M41 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 42 then null else invoice_month_42_revenue_cuml end) as "M42 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 43 then null else invoice_month_43_revenue_cuml end) as "M43 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 44 then null else invoice_month_44_revenue_cuml end) as "M44 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 45 then null else invoice_month_45_revenue_cuml end) as "M45 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 46 then null else invoice_month_46_revenue_cuml end) as "M46 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 47 then null else invoice_month_47_revenue_cuml end) as "M47 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 48 then null else invoice_month_48_revenue_cuml end) as "M48 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 49 then null else invoice_month_49_revenue_cuml end) as "M49 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 50 then null else invoice_month_50_revenue_cuml end) as "M50 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 51 then null else invoice_month_51_revenue_cuml end) as "M51 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 52 then null else invoice_month_52_revenue_cuml end) as "M52 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 53 then null else invoice_month_53_revenue_cuml end) as "M53 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 54 then null else invoice_month_54_revenue_cuml end) as "M54 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 55 then null else invoice_month_55_revenue_cuml end) as "M55 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 56 then null else invoice_month_56_revenue_cuml end) as "M56 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 57 then null else invoice_month_57_revenue_cuml end) as "M57 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 58 then null else invoice_month_58_revenue_cuml end) as "M58 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 59 then null else invoice_month_59_revenue_cuml end) as "M59 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 60 then null else invoice_month_60_revenue_cuml end) as "M60 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 61 then null else invoice_month_61_revenue_cuml end) as "M61 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 62 then null else invoice_month_62_revenue_cuml end) as "M62 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 63 then null else invoice_month_63_revenue_cuml end) as "M63 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 64 then null else invoice_month_64_revenue_cuml end) as "M64 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 65 then null else invoice_month_65_revenue_cuml end) as "M65 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 66 then null else invoice_month_66_revenue_cuml end) as "M66 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 67 then null else invoice_month_67_revenue_cuml end) as "M67 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 68 then null else invoice_month_68_revenue_cuml end) as "M68 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 69 then null else invoice_month_69_revenue_cuml end) as "M69 Rev Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 70 then null else invoice_month_70_revenue_cuml end) as "M70 Rev Cuml. $",
        ----- ltv
        sum(invoice_month_0_ltv) as "M0 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 1 then null else invoice_month_1_ltv end) as "M01 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 2 then null else invoice_month_2_ltv end) as "M02 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 3 then null else invoice_month_3_ltv end) as "M03 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 4 then null else invoice_month_4_ltv end) as "M04 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 5 then null else invoice_month_5_ltv end) as "M05 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 6 then null else invoice_month_6_ltv end) as "M06 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 7 then null else invoice_month_7_ltv end) as "M07 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 8 then null else invoice_month_8_ltv end) as "M08 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 9 then null else invoice_month_9_ltv end) as "M09 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 10 then null else invoice_month_10_ltv end) as "M10 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 11 then null else invoice_month_11_ltv end) as "M11 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 12 then null else invoice_month_12_ltv end) as "M12 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 13 then null else invoice_month_13_ltv end) as "M13 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 14 then null else invoice_month_14_ltv end) as "M14 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 15 then null else invoice_month_15_ltv end) as "M15 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 16 then null else invoice_month_16_ltv end) as "M16 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 17 then null else invoice_month_17_ltv end) as "M17 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 18 then null else invoice_month_18_ltv end) as "M18 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 19 then null else invoice_month_19_ltv end) as "M19 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 20 then null else invoice_month_20_ltv end) as "M20 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 21 then null else invoice_month_21_ltv end) as "M21 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 22 then null else invoice_month_22_ltv end) as "M22 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 23 then null else invoice_month_23_ltv end) as "M23 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 24 then null else invoice_month_24_ltv end) as "M24 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 25 then null else invoice_month_25_ltv end) as "M25 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 26 then null else invoice_month_26_ltv end) as "M26 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 27 then null else invoice_month_27_ltv end) as "M27 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 28 then null else invoice_month_28_ltv end) as "M28 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 29 then null else invoice_month_29_ltv end) as "M29 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 30 then null else invoice_month_30_ltv end) as "M30 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 31 then null else invoice_month_31_ltv end) as "M31 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 32 then null else invoice_month_32_ltv end) as "M32 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 33 then null else invoice_month_33_ltv end) as "M33 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 34 then null else invoice_month_34_ltv end) as "M34 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 35 then null else invoice_month_35_ltv end) as "M35 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 36 then null else invoice_month_36_ltv end) as "M36 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 37 then null else invoice_month_37_ltv end) as "M37 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 38 then null else invoice_month_38_ltv end) as "M38 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 39 then null else invoice_month_39_ltv end) as "M39 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 40 then null else invoice_month_40_ltv end) as "M40 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 41 then null else invoice_month_41_ltv end) as "M41 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 42 then null else invoice_month_42_ltv end) as "M42 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 43 then null else invoice_month_43_ltv end) as "M43 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 44 then null else invoice_month_44_ltv end) as "M44 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 45 then null else invoice_month_45_ltv end) as "M45 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 46 then null else invoice_month_46_ltv end) as "M46 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 47 then null else invoice_month_47_ltv end) as "M47 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 48 then null else invoice_month_48_ltv end) as "M48 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 49 then null else invoice_month_49_ltv end) as "M49 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 50 then null else invoice_month_50_ltv end) as "M50 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 51 then null else invoice_month_51_ltv end) as "M51 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 52 then null else invoice_month_52_ltv end) as "M52 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 53 then null else invoice_month_53_ltv end) as "M53 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 54 then null else invoice_month_54_ltv end) as "M54 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 55 then null else invoice_month_55_ltv end) as "M55 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 56 then null else invoice_month_56_ltv end) as "M56 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 57 then null else invoice_month_57_ltv end) as "M57 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 58 then null else invoice_month_58_ltv end) as "M58 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 59 then null else invoice_month_59_ltv end) as "M59 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 60 then null else invoice_month_60_ltv end) as "M60 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 61 then null else invoice_month_61_ltv end) as "M61 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 62 then null else invoice_month_62_ltv end) as "M62 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 63 then null else invoice_month_63_ltv end) as "M63 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 64 then null else invoice_month_64_ltv end) as "M64 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 65 then null else invoice_month_65_ltv end) as "M65 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 66 then null else invoice_month_66_ltv end) as "M66 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 67 then null else invoice_month_67_ltv end) as "M67 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 68 then null else invoice_month_68_ltv end) as "M68 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 69 then null else invoice_month_69_ltv end) as "M69 LTV $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 70 then null else invoice_month_70_ltv end) as "M70 LTV $",
        ----- cumulative ltv
        sum(invoice_month_0_ltv_cuml) as "M0 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 1 then null else invoice_month_1_ltv_cuml end) as "M01 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 2 then null else invoice_month_2_ltv_cuml end) as "M02 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 3 then null else invoice_month_3_ltv_cuml end) as "M03 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 4 then null else invoice_month_4_ltv_cuml end) as "M04 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 5 then null else invoice_month_5_ltv_cuml end) as "M05 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 6 then null else invoice_month_6_ltv_cuml end) as "M06 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 7 then null else invoice_month_7_ltv_cuml end) as "M07 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 8 then null else invoice_month_8_ltv_cuml end) as "M08 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 9 then null else invoice_month_9_ltv_cuml end) as "M09 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 10 then null else invoice_month_10_ltv_cuml end) as "M10 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 11 then null else invoice_month_11_ltv_cuml end) as "M11 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 12 then null else invoice_month_12_ltv_cuml end) as "M12 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 13 then null else invoice_month_13_ltv_cuml end) as "M13 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 14 then null else invoice_month_14_ltv_cuml end) as "M14 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 15 then null else invoice_month_15_ltv_cuml end) as "M15 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 16 then null else invoice_month_16_ltv_cuml end) as "M16 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 17 then null else invoice_month_17_ltv_cuml end) as "M17 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 18 then null else invoice_month_18_ltv_cuml end) as "M18 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 19 then null else invoice_month_19_ltv_cuml end) as "M19 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 20 then null else invoice_month_20_ltv_cuml end) as "M20 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 21 then null else invoice_month_21_ltv_cuml end) as "M21 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 22 then null else invoice_month_22_ltv_cuml end) as "M22 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 23 then null else invoice_month_23_ltv_cuml end) as "M23 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 24 then null else invoice_month_24_ltv_cuml end) as "M24 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 25 then null else invoice_month_25_ltv_cuml end) as "M25 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 26 then null else invoice_month_26_ltv_cuml end) as "M26 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 27 then null else invoice_month_27_ltv_cuml end) as "M27 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 28 then null else invoice_month_28_ltv_cuml end) as "M28 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 29 then null else invoice_month_29_ltv_cuml end) as "M29 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 30 then null else invoice_month_30_ltv_cuml end) as "M30 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 31 then null else invoice_month_31_ltv_cuml end) as "M31 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 32 then null else invoice_month_32_ltv_cuml end) as "M32 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 33 then null else invoice_month_33_ltv_cuml end) as "M33 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 34 then null else invoice_month_34_ltv_cuml end) as "M34 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 35 then null else invoice_month_35_ltv_cuml end) as "M35 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 36 then null else invoice_month_36_ltv_cuml end) as "M36 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 37 then null else invoice_month_37_ltv_cuml end) as "M37 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 38 then null else invoice_month_38_ltv_cuml end) as "M38 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 39 then null else invoice_month_39_ltv_cuml end) as "M39 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 40 then null else invoice_month_40_ltv_cuml end) as "M40 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 41 then null else invoice_month_41_ltv_cuml end) as "M41 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 42 then null else invoice_month_42_ltv_cuml end) as "M42 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 43 then null else invoice_month_43_ltv_cuml end) as "M43 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 44 then null else invoice_month_44_ltv_cuml end) as "M44 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 45 then null else invoice_month_45_ltv_cuml end) as "M45 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 46 then null else invoice_month_46_ltv_cuml end) as "M46 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 47 then null else invoice_month_47_ltv_cuml end) as "M47 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 48 then null else invoice_month_48_ltv_cuml end) as "M48 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 49 then null else invoice_month_49_ltv_cuml end) as "M49 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 50 then null else invoice_month_50_ltv_cuml end) as "M50 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 51 then null else invoice_month_51_ltv_cuml end) as "M51 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 52 then null else invoice_month_52_ltv_cuml end) as "M52 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 53 then null else invoice_month_53_ltv_cuml end) as "M53 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 54 then null else invoice_month_54_ltv_cuml end) as "M54 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 55 then null else invoice_month_55_ltv_cuml end) as "M55 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 56 then null else invoice_month_56_ltv_cuml end) as "M56 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 57 then null else invoice_month_57_ltv_cuml end) as "M57 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 58 then null else invoice_month_58_ltv_cuml end) as "M58 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 59 then null else invoice_month_59_ltv_cuml end) as "M59 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 60 then null else invoice_month_60_ltv_cuml end) as "M60 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 61 then null else invoice_month_61_ltv_cuml end) as "M61 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 62 then null else invoice_month_62_ltv_cuml end) as "M62 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 63 then null else invoice_month_63_ltv_cuml end) as "M63 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 64 then null else invoice_month_64_ltv_cuml end) as "M64 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 65 then null else invoice_month_65_ltv_cuml end) as "M65 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 66 then null else invoice_month_66_ltv_cuml end) as "M66 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 67 then null else invoice_month_67_ltv_cuml end) as "M67 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 68 then null else invoice_month_68_ltv_cuml end) as "M68 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 69 then null else invoice_month_69_ltv_cuml end) as "M69 LTV Cuml. $",
        sum(case when datediff(month,created_month_year ,DATEADD(month, -1, date_trunc('month',to_date(current_date()))))+1 <= 70 then null else invoice_month_70_ltv_cuml end) as "M70 LTV Cuml. $"
        
    
    from sub_inv_flag
    where date_trunc('month',to_date(activated_at)) <= DATEADD(month, -1, date_trunc('month',to_date(current_date()))) --- only through previous month
    group by 1,2,3)

    select
      *
    ---- seed retention rate
    , div0("M0 Vol", cohort_volume)  as "M0 Ret. Rate"
    , div0("M01 Vol", cohort_volume) as "M01 Ret. Rate"
    , div0("M02 Vol", cohort_volume) as "M02 Ret. Rate"
    , div0("M03 Vol", cohort_volume) as "M03 Ret. Rate"
    , div0("M04 Vol", cohort_volume) as "M04 Ret. Rate"
    , div0("M05 Vol", cohort_volume) as "M05 Ret. Rate"
    , div0("M06 Vol", cohort_volume) as "M06 Ret. Rate"
    , div0("M07 Vol", cohort_volume) as "M07 Ret. Rate"
    , div0("M08 Vol", cohort_volume) as "M08 Ret. Rate"
    , div0("M09 Vol", cohort_volume) as "M09 Ret. Rate"
    , div0("M10 Vol", cohort_volume) as "M10 Ret. Rate"
    , div0("M11 Vol", cohort_volume) as "M11 Ret. Rate"
    , div0("M12 Vol", cohort_volume) as "M12 Ret. Rate"
    , div0("M13 Vol", cohort_volume) as "M13 Ret. Rate"
    , div0("M14 Vol", cohort_volume) as "M14 Ret. Rate"
    , div0("M15 Vol", cohort_volume) as "M15 Ret. Rate"
    , div0("M16 Vol", cohort_volume) as "M16 Ret. Rate"
    , div0("M17 Vol", cohort_volume) as "M17 Ret. Rate"
    , div0("M18 Vol", cohort_volume) as "M18 Ret. Rate"
    , div0("M19 Vol", cohort_volume) as "M19 Ret. Rate"
    , div0("M20 Vol", cohort_volume) as "M20 Ret. Rate"
    , div0("M21 Vol", cohort_volume) as "M21 Ret. Rate"
    , div0("M22 Vol", cohort_volume) as "M22 Ret. Rate"
    , div0("M23 Vol", cohort_volume) as "M23 Ret. Rate"
    , div0("M24 Vol", cohort_volume) as "M24 Ret. Rate"
    , div0("M25 Vol", cohort_volume) as "M25 Ret. Rate"
    , div0("M26 Vol", cohort_volume) as "M26 Ret. Rate"
    , div0("M27 Vol", cohort_volume) as "M27 Ret. Rate"
    , div0("M28 Vol", cohort_volume) as "M28 Ret. Rate"
    , div0("M29 Vol", cohort_volume) as "M29 Ret. Rate"
    , div0("M30 Vol", cohort_volume) as "M30 Ret. Rate"
    , div0("M31 Vol", cohort_volume) as "M31 Ret. Rate"
    , div0("M32 Vol", cohort_volume) as "M32 Ret. Rate"
    , div0("M33 Vol", cohort_volume) as "M33 Ret. Rate"
    , div0("M34 Vol", cohort_volume) as "M34 Ret. Rate"
    , div0("M35 Vol", cohort_volume) as "M35 Ret. Rate"
    , div0("M36 Vol", cohort_volume) as "M36 Ret. Rate"
    , div0("M37 Vol", cohort_volume) as "M37 Ret. Rate"
    , div0("M38 Vol", cohort_volume) as "M38 Ret. Rate"
    , div0("M39 Vol", cohort_volume) as "M39 Ret. Rate"
    , div0("M40 Vol", cohort_volume) as "M40 Ret. Rate"
    , div0("M41 Vol", cohort_volume) as "M41 Ret. Rate"
    , div0("M42 Vol", cohort_volume) as "M42 Ret. Rate"
    , div0("M43 Vol", cohort_volume) as "M43 Ret. Rate"
    , div0("M44 Vol", cohort_volume) as "M44 Ret. Rate"
    , div0("M45 Vol", cohort_volume) as "M45 Ret. Rate"
    , div0("M46 Vol", cohort_volume) as "M46 Ret. Rate"
    , div0("M47 Vol", cohort_volume) as "M47 Ret. Rate"
    , div0("M48 Vol", cohort_volume) as "M48 Ret. Rate"
    , div0("M49 Vol", cohort_volume) as "M49 Ret. Rate"
    , div0("M50 Vol", cohort_volume) as "M50 Ret. Rate"
    , div0("M51 Vol", cohort_volume) as "M51 Ret. Rate"
    , div0("M52 Vol", cohort_volume) as "M52 Ret. Rate"
    , div0("M53 Vol", cohort_volume) as "M53 Ret. Rate"
    , div0("M54 Vol", cohort_volume) as "M54 Ret. Rate"
    , div0("M55 Vol", cohort_volume) as "M55 Ret. Rate"
    , div0("M56 Vol", cohort_volume) as "M56 Ret. Rate"
    , div0("M57 Vol", cohort_volume) as "M57 Ret. Rate"
    , div0("M58 Vol", cohort_volume) as "M58 Ret. Rate"
    , div0("M59 Vol", cohort_volume) as "M59 Ret. Rate"
    , div0("M60 Vol", cohort_volume) as "M60 Ret. Rate"
    , div0("M61 Vol", cohort_volume) as "M61 Ret. Rate"
    , div0("M62 Vol", cohort_volume) as "M62 Ret. Rate"
    , div0("M63 Vol", cohort_volume) as "M63 Ret. Rate"
    , div0("M64 Vol", cohort_volume) as "M64 Ret. Rate"
    , div0("M65 Vol", cohort_volume) as "M65 Ret. Rate"
    , div0("M66 Vol", cohort_volume) as "M66 Ret. Rate"
    , div0("M67 Vol", cohort_volume) as "M67 Ret. Rate"
    , div0("M68 Vol", cohort_volume) as "M68 Ret. Rate"
    , div0("M69 Vol", cohort_volume) as "M69 Ret. Rate"
    , div0("M70 Vol", cohort_volume) as "M70 Ret. Rate"
    ---- transaction retention rate
    , div0("M0 Trnsc Vol", cohort_volume)  as "M0 Trnsc Ret. Rate"
    , div0("M01 Trnsc Vol", cohort_volume) as "M01 Trnsc Ret. Rate"
    , div0("M02 Trnsc Vol", cohort_volume) as "M02 Trnsc Ret. Rate"
    , div0("M03 Trnsc Vol", cohort_volume) as "M03 Trnsc Ret. Rate"
    , div0("M04 Trnsc Vol", cohort_volume) as "M04 Trnsc Ret. Rate"
    , div0("M05 Trnsc Vol", cohort_volume) as "M05 Trnsc Ret. Rate"
    , div0("M06 Trnsc Vol", cohort_volume) as "M06 Trnsc Ret. Rate"
    , div0("M07 Trnsc Vol", cohort_volume) as "M07 Trnsc Ret. Rate"
    , div0("M08 Trnsc Vol", cohort_volume) as "M08 Trnsc Ret. Rate"
    , div0("M09 Trnsc Vol", cohort_volume) as "M09 Trnsc Ret. Rate"
    , div0("M10 Trnsc Vol", cohort_volume) as "M10 Trnsc Ret. Rate"
    , div0("M11 Trnsc Vol", cohort_volume) as "M11 Trnsc Ret. Rate"
    , div0("M12 Trnsc Vol", cohort_volume) as "M12 Trnsc Ret. Rate"
    , div0("M13 Trnsc Vol", cohort_volume) as "M13 Trnsc Ret. Rate"
    , div0("M14 Trnsc Vol", cohort_volume) as "M14 Trnsc Ret. Rate"
    , div0("M15 Trnsc Vol", cohort_volume) as "M15 Trnsc Ret. Rate"
    , div0("M16 Trnsc Vol", cohort_volume) as "M16 Trnsc Ret. Rate"
    , div0("M17 Trnsc Vol", cohort_volume) as "M17 Trnsc Ret. Rate"
    , div0("M18 Trnsc Vol", cohort_volume) as "M18 Trnsc Ret. Rate"
    , div0("M19 Trnsc Vol", cohort_volume) as "M19 Trnsc Ret. Rate"
    , div0("M20 Trnsc Vol", cohort_volume) as "M20 Trnsc Ret. Rate"
    , div0("M21 Trnsc Vol", cohort_volume) as "M21 Trnsc Ret. Rate"
    , div0("M22 Trnsc Vol", cohort_volume) as "M22 Trnsc Ret. Rate"
    , div0("M23 Trnsc Vol", cohort_volume) as "M23 Trnsc Ret. Rate"
    , div0("M24 Trnsc Vol", cohort_volume) as "M24 Trnsc Ret. Rate"
    , div0("M25 Trnsc Vol", cohort_volume) as "M25 Trnsc Ret. Rate"
    , div0("M26 Trnsc Vol", cohort_volume) as "M26 Trnsc Ret. Rate"
    , div0("M27 Trnsc Vol", cohort_volume) as "M27 Trnsc Ret. Rate"
    , div0("M28 Trnsc Vol", cohort_volume) as "M28 Trnsc Ret. Rate"
    , div0("M29 Trnsc Vol", cohort_volume) as "M29 Trnsc Ret. Rate"
    , div0("M30 Trnsc Vol", cohort_volume) as "M30 Trnsc Ret. Rate"
    , div0("M31 Trnsc Vol", cohort_volume) as "M31 Trnsc Ret. Rate"
    , div0("M32 Trnsc Vol", cohort_volume) as "M32 Trnsc Ret. Rate"
    , div0("M33 Trnsc Vol", cohort_volume) as "M33 Trnsc Ret. Rate"
    , div0("M34 Trnsc Vol", cohort_volume) as "M34 Trnsc Ret. Rate"
    , div0("M35 Trnsc Vol", cohort_volume) as "M35 Trnsc Ret. Rate"
    , div0("M36 Trnsc Vol", cohort_volume) as "M36 Trnsc Ret. Rate"
    , div0("M37 Trnsc Vol", cohort_volume) as "M37 Trnsc Ret. Rate"
    , div0("M38 Trnsc Vol", cohort_volume) as "M38 Trnsc Ret. Rate"
    , div0("M39 Trnsc Vol", cohort_volume) as "M39 Trnsc Ret. Rate"
    , div0("M40 Trnsc Vol", cohort_volume) as "M40 Trnsc Ret. Rate"
    , div0("M41 Trnsc Vol", cohort_volume) as "M41 Trnsc Ret. Rate"
    , div0("M42 Trnsc Vol", cohort_volume) as "M42 Trnsc Ret. Rate"
    , div0("M43 Trnsc Vol", cohort_volume) as "M43 Trnsc Ret. Rate"
    , div0("M44 Trnsc Vol", cohort_volume) as "M44 Trnsc Ret. Rate"
    , div0("M45 Trnsc Vol", cohort_volume) as "M45 Trnsc Ret. Rate"
    , div0("M46 Trnsc Vol", cohort_volume) as "M46 Trnsc Ret. Rate"
    , div0("M47 Trnsc Vol", cohort_volume) as "M47 Trnsc Ret. Rate"
    , div0("M48 Trnsc Vol", cohort_volume) as "M48 Trnsc Ret. Rate"
    , div0("M49 Trnsc Vol", cohort_volume) as "M49 Trnsc Ret. Rate"
    , div0("M50 Trnsc Vol", cohort_volume) as "M50 Trnsc Ret. Rate"
    , div0("M51 Trnsc Vol", cohort_volume) as "M51 Trnsc Ret. Rate"
    , div0("M52 Trnsc Vol", cohort_volume) as "M52 Trnsc Ret. Rate"
    , div0("M53 Trnsc Vol", cohort_volume) as "M53 Trnsc Ret. Rate"
    , div0("M54 Trnsc Vol", cohort_volume) as "M54 Trnsc Ret. Rate"
    , div0("M55 Trnsc Vol", cohort_volume) as "M55 Trnsc Ret. Rate"
    , div0("M56 Trnsc Vol", cohort_volume) as "M56 Trnsc Ret. Rate"
    , div0("M57 Trnsc Vol", cohort_volume) as "M57 Trnsc Ret. Rate"
    , div0("M58 Trnsc Vol", cohort_volume) as "M58 Trnsc Ret. Rate"
    , div0("M59 Trnsc Vol", cohort_volume) as "M59 Trnsc Ret. Rate"
    , div0("M60 Trnsc Vol", cohort_volume) as "M60 Trnsc Ret. Rate"
    , div0("M61 Trnsc Vol", cohort_volume) as "M61 Trnsc Ret. Rate"
    , div0("M62 Trnsc Vol", cohort_volume) as "M62 Trnsc Ret. Rate"
    , div0("M63 Trnsc Vol", cohort_volume) as "M63 Trnsc Ret. Rate"
    , div0("M64 Trnsc Vol", cohort_volume) as "M64 Trnsc Ret. Rate"
    , div0("M65 Trnsc Vol", cohort_volume) as "M65 Trnsc Ret. Rate"
    , div0("M66 Trnsc Vol", cohort_volume) as "M66 Trnsc Ret. Rate"
    , div0("M67 Trnsc Vol", cohort_volume) as "M67 Trnsc Ret. Rate"
    , div0("M68 Trnsc Vol", cohort_volume) as "M68 Trnsc Ret. Rate"
    , div0("M69 Trnsc Vol", cohort_volume) as "M69 Trnsc Ret. Rate"
    , div0("M70 Trnsc Vol", cohort_volume) as "M70 Trnsc Ret. Rate"
     ---- cuml. revenue per volume
    , div0("M0 Rev Cuml. $", cohort_volume)  as "M0 Rev Cuml. $ Per Vol."
    , div0("M01 Rev Cuml. $", cohort_volume) as "M01 Rev Cuml. $ Per Vol."
    , div0("M02 Rev Cuml. $", cohort_volume) as "M02 Rev Cuml. $ Per Vol."
    , div0("M03 Rev Cuml. $", cohort_volume) as "M03 Rev Cuml. $ Per Vol."
    , div0("M04 Rev Cuml. $", cohort_volume) as "M04 Rev Cuml. $ Per Vol."
    , div0("M05 Rev Cuml. $", cohort_volume) as "M05 Rev Cuml. $ Per Vol."
    , div0("M06 Rev Cuml. $", cohort_volume) as "M06 Rev Cuml. $ Per Vol."
    , div0("M07 Rev Cuml. $", cohort_volume) as "M07 Rev Cuml. $ Per Vol."
    , div0("M08 Rev Cuml. $", cohort_volume) as "M08 Rev Cuml. $ Per Vol."
    , div0("M09 Rev Cuml. $", cohort_volume) as "M09 Rev Cuml. $ Per Vol."
    , div0("M10 Rev Cuml. $", cohort_volume) as "M10 Rev Cuml. $ Per Vol."
    , div0("M11 Rev Cuml. $", cohort_volume) as "M11 Rev Cuml. $ Per Vol."
    , div0("M12 Rev Cuml. $", cohort_volume) as "M12 Rev Cuml. $ Per Vol."
    , div0("M13 Rev Cuml. $", cohort_volume) as "M13 Rev Cuml. $ Per Vol."
    , div0("M14 Rev Cuml. $", cohort_volume) as "M14 Rev Cuml. $ Per Vol."
    , div0("M15 Rev Cuml. $", cohort_volume) as "M15 Rev Cuml. $ Per Vol."
    , div0("M16 Rev Cuml. $", cohort_volume) as "M16 Rev Cuml. $ Per Vol."
    , div0("M17 Rev Cuml. $", cohort_volume) as "M17 Rev Cuml. $ Per Vol."
    , div0("M18 Rev Cuml. $", cohort_volume) as "M18 Rev Cuml. $ Per Vol."
    , div0("M19 Rev Cuml. $", cohort_volume) as "M19 Rev Cuml. $ Per Vol."
    , div0("M20 Rev Cuml. $", cohort_volume) as "M20 Rev Cuml. $ Per Vol."
    , div0("M21 Rev Cuml. $", cohort_volume) as "M21 Rev Cuml. $ Per Vol."
    , div0("M22 Rev Cuml. $", cohort_volume) as "M22 Rev Cuml. $ Per Vol."
    , div0("M23 Rev Cuml. $", cohort_volume) as "M23 Rev Cuml. $ Per Vol."
    , div0("M24 Rev Cuml. $", cohort_volume) as "M24 Rev Cuml. $ Per Vol."
    , div0("M25 Rev Cuml. $", cohort_volume) as "M25 Rev Cuml. $ Per Vol."
    , div0("M26 Rev Cuml. $", cohort_volume) as "M26 Rev Cuml. $ Per Vol."
    , div0("M27 Rev Cuml. $", cohort_volume) as "M27 Rev Cuml. $ Per Vol."
    , div0("M28 Rev Cuml. $", cohort_volume) as "M28 Rev Cuml. $ Per Vol."
    , div0("M29 Rev Cuml. $", cohort_volume) as "M29 Rev Cuml. $ Per Vol."
    , div0("M30 Rev Cuml. $", cohort_volume) as "M30 Rev Cuml. $ Per Vol."
    , div0("M31 Rev Cuml. $", cohort_volume) as "M31 Rev Cuml. $ Per Vol."
    , div0("M32 Rev Cuml. $", cohort_volume) as "M32 Rev Cuml. $ Per Vol."
    , div0("M33 Rev Cuml. $", cohort_volume) as "M33 Rev Cuml. $ Per Vol."
    , div0("M34 Rev Cuml. $", cohort_volume) as "M34 Rev Cuml. $ Per Vol."
    , div0("M35 Rev Cuml. $", cohort_volume) as "M35 Rev Cuml. $ Per Vol."
    , div0("M36 Rev Cuml. $", cohort_volume) as "M36 Rev Cuml. $ Per Vol."
    , div0("M37 Rev Cuml. $", cohort_volume) as "M37 Rev Cuml. $ Per Vol."
    , div0("M38 Rev Cuml. $", cohort_volume) as "M38 Rev Cuml. $ Per Vol."
    , div0("M39 Rev Cuml. $", cohort_volume) as "M39 Rev Cuml. $ Per Vol."
    , div0("M40 Rev Cuml. $", cohort_volume) as "M40 Rev Cuml. $ Per Vol."
    , div0("M41 Rev Cuml. $", cohort_volume) as "M41 Rev Cuml. $ Per Vol."
    , div0("M42 Rev Cuml. $", cohort_volume) as "M42 Rev Cuml. $ Per Vol."
    , div0("M43 Rev Cuml. $", cohort_volume) as "M43 Rev Cuml. $ Per Vol."
    , div0("M44 Rev Cuml. $", cohort_volume) as "M44 Rev Cuml. $ Per Vol."
    , div0("M45 Rev Cuml. $", cohort_volume) as "M45 Rev Cuml. $ Per Vol."
    , div0("M46 Rev Cuml. $", cohort_volume) as "M46 Rev Cuml. $ Per Vol."
    , div0("M47 Rev Cuml. $", cohort_volume) as "M47 Rev Cuml. $ Per Vol."
    , div0("M48 Rev Cuml. $", cohort_volume) as "M48 Rev Cuml. $ Per Vol."
    , div0("M49 Rev Cuml. $", cohort_volume) as "M49 Rev Cuml. $ Per Vol."
    , div0("M50 Rev Cuml. $", cohort_volume) as "M50 Rev Cuml. $ Per Vol."
    , div0("M51 Rev Cuml. $", cohort_volume) as "M51 Rev Cuml. $ Per Vol."
    , div0("M52 Rev Cuml. $", cohort_volume) as "M52 Rev Cuml. $ Per Vol."
    , div0("M53 Rev Cuml. $", cohort_volume) as "M53 Rev Cuml. $ Per Vol."
    , div0("M54 Rev Cuml. $", cohort_volume) as "M54 Rev Cuml. $ Per Vol."
    , div0("M55 Rev Cuml. $", cohort_volume) as "M55 Rev Cuml. $ Per Vol."
    , div0("M56 Rev Cuml. $", cohort_volume) as "M56 Rev Cuml. $ Per Vol."
    , div0("M57 Rev Cuml. $", cohort_volume) as "M57 Rev Cuml. $ Per Vol."
    , div0("M58 Rev Cuml. $", cohort_volume) as "M58 Rev Cuml. $ Per Vol."
    , div0("M59 Rev Cuml. $", cohort_volume) as "M59 Rev Cuml. $ Per Vol."
    , div0("M60 Rev Cuml. $", cohort_volume) as "M60 Rev Cuml. $ Per Vol."
    , div0("M61 Rev Cuml. $", cohort_volume) as "M61 Rev Cuml. $ Per Vol."
    , div0("M62 Rev Cuml. $", cohort_volume) as "M62 Rev Cuml. $ Per Vol."
    , div0("M63 Rev Cuml. $", cohort_volume) as "M63 Rev Cuml. $ Per Vol."
    , div0("M64 Rev Cuml. $", cohort_volume) as "M64 Rev Cuml. $ Per Vol."
    , div0("M65 Rev Cuml. $", cohort_volume) as "M65 Rev Cuml. $ Per Vol."
    , div0("M66 Rev Cuml. $", cohort_volume) as "M66 Rev Cuml. $ Per Vol."
    , div0("M67 Rev Cuml. $", cohort_volume) as "M67 Rev Cuml. $ Per Vol."
    , div0("M68 Rev Cuml. $", cohort_volume) as "M68 Rev Cuml. $ Per Vol."
    , div0("M69 Rev Cuml. $", cohort_volume) as "M69 Rev Cuml. $ Per Vol."
    , div0("M70 Rev Cuml. $", cohort_volume) as "M70 Rev Cuml. $ Per Vol."
    ---- cuml. ltv per volume
    , div0("M0 LTV Cuml. $", cohort_volume)  as "M0 LTV Cuml. $ Per Vol."
    , div0("M01 LTV Cuml. $", cohort_volume) as "M01 LTV Cuml. $ Per Vol."
    , div0("M02 LTV Cuml. $", cohort_volume) as "M02 LTV Cuml. $ Per Vol."
    , div0("M03 LTV Cuml. $", cohort_volume) as "M03 LTV Cuml. $ Per Vol."
    , div0("M04 LTV Cuml. $", cohort_volume) as "M04 LTV Cuml. $ Per Vol."
    , div0("M05 LTV Cuml. $", cohort_volume) as "M05 LTV Cuml. $ Per Vol."
    , div0("M06 LTV Cuml. $", cohort_volume) as "M06 LTV Cuml. $ Per Vol."
    , div0("M07 LTV Cuml. $", cohort_volume) as "M07 LTV Cuml. $ Per Vol."
    , div0("M08 LTV Cuml. $", cohort_volume) as "M08 LTV Cuml. $ Per Vol."
    , div0("M09 LTV Cuml. $", cohort_volume) as "M09 LTV Cuml. $ Per Vol."
    , div0("M10 LTV Cuml. $", cohort_volume) as "M10 LTV Cuml. $ Per Vol."
    , div0("M11 LTV Cuml. $", cohort_volume) as "M11 LTV Cuml. $ Per Vol."
    , div0("M12 LTV Cuml. $", cohort_volume) as "M12 LTV Cuml. $ Per Vol."
    , div0("M13 LTV Cuml. $", cohort_volume) as "M13 LTV Cuml. $ Per Vol."
    , div0("M14 LTV Cuml. $", cohort_volume) as "M14 LTV Cuml. $ Per Vol."
    , div0("M15 LTV Cuml. $", cohort_volume) as "M15 LTV Cuml. $ Per Vol."
    , div0("M16 LTV Cuml. $", cohort_volume) as "M16 LTV Cuml. $ Per Vol."
    , div0("M17 LTV Cuml. $", cohort_volume) as "M17 LTV Cuml. $ Per Vol."
    , div0("M18 LTV Cuml. $", cohort_volume) as "M18 LTV Cuml. $ Per Vol."
    , div0("M19 LTV Cuml. $", cohort_volume) as "M19 LTV Cuml. $ Per Vol."
    , div0("M20 LTV Cuml. $", cohort_volume) as "M20 LTV Cuml. $ Per Vol."
    , div0("M21 LTV Cuml. $", cohort_volume) as "M21 LTV Cuml. $ Per Vol."
    , div0("M22 LTV Cuml. $", cohort_volume) as "M22 LTV Cuml. $ Per Vol."
    , div0("M23 LTV Cuml. $", cohort_volume) as "M23 LTV Cuml. $ Per Vol."
    , div0("M24 LTV Cuml. $", cohort_volume) as "M24 LTV Cuml. $ Per Vol."
    , div0("M25 LTV Cuml. $", cohort_volume) as "M25 LTV Cuml. $ Per Vol."
    , div0("M26 LTV Cuml. $", cohort_volume) as "M26 LTV Cuml. $ Per Vol."
    , div0("M27 LTV Cuml. $", cohort_volume) as "M27 LTV Cuml. $ Per Vol."
    , div0("M28 LTV Cuml. $", cohort_volume) as "M28 LTV Cuml. $ Per Vol."
    , div0("M29 LTV Cuml. $", cohort_volume) as "M29 LTV Cuml. $ Per Vol."
    , div0("M30 LTV Cuml. $", cohort_volume) as "M30 LTV Cuml. $ Per Vol."
    , div0("M31 LTV Cuml. $", cohort_volume) as "M31 LTV Cuml. $ Per Vol."
    , div0("M32 LTV Cuml. $", cohort_volume) as "M32 LTV Cuml. $ Per Vol."
    , div0("M33 LTV Cuml. $", cohort_volume) as "M33 LTV Cuml. $ Per Vol."
    , div0("M34 LTV Cuml. $", cohort_volume) as "M34 LTV Cuml. $ Per Vol."
    , div0("M35 LTV Cuml. $", cohort_volume) as "M35 LTV Cuml. $ Per Vol."
    , div0("M36 LTV Cuml. $", cohort_volume) as "M36 LTV Cuml. $ Per Vol."
    , div0("M37 LTV Cuml. $", cohort_volume) as "M37 LTV Cuml. $ Per Vol."
    , div0("M38 LTV Cuml. $", cohort_volume) as "M38 LTV Cuml. $ Per Vol."
    , div0("M39 LTV Cuml. $", cohort_volume) as "M39 LTV Cuml. $ Per Vol."
    , div0("M40 LTV Cuml. $", cohort_volume) as "M40 LTV Cuml. $ Per Vol."
    , div0("M41 LTV Cuml. $", cohort_volume) as "M41 LTV Cuml. $ Per Vol."
    , div0("M42 LTV Cuml. $", cohort_volume) as "M42 LTV Cuml. $ Per Vol."
    , div0("M43 LTV Cuml. $", cohort_volume) as "M43 LTV Cuml. $ Per Vol."
    , div0("M44 LTV Cuml. $", cohort_volume) as "M44 LTV Cuml. $ Per Vol."
    , div0("M45 LTV Cuml. $", cohort_volume) as "M45 LTV Cuml. $ Per Vol."
    , div0("M46 LTV Cuml. $", cohort_volume) as "M46 LTV Cuml. $ Per Vol."
    , div0("M47 LTV Cuml. $", cohort_volume) as "M47 LTV Cuml. $ Per Vol."
    , div0("M48 LTV Cuml. $", cohort_volume) as "M48 LTV Cuml. $ Per Vol."
    , div0("M49 LTV Cuml. $", cohort_volume) as "M49 LTV Cuml. $ Per Vol."
    , div0("M50 LTV Cuml. $", cohort_volume) as "M50 LTV Cuml. $ Per Vol."
    , div0("M51 LTV Cuml. $", cohort_volume) as "M51 LTV Cuml. $ Per Vol."
    , div0("M52 LTV Cuml. $", cohort_volume) as "M52 LTV Cuml. $ Per Vol."
    , div0("M53 LTV Cuml. $", cohort_volume) as "M53 LTV Cuml. $ Per Vol."
    , div0("M54 LTV Cuml. $", cohort_volume) as "M54 LTV Cuml. $ Per Vol."
    , div0("M55 LTV Cuml. $", cohort_volume) as "M55 LTV Cuml. $ Per Vol."
    , div0("M56 LTV Cuml. $", cohort_volume) as "M56 LTV Cuml. $ Per Vol."
    , div0("M57 LTV Cuml. $", cohort_volume) as "M57 LTV Cuml. $ Per Vol."
    , div0("M58 LTV Cuml. $", cohort_volume) as "M58 LTV Cuml. $ Per Vol."
    , div0("M59 LTV Cuml. $", cohort_volume) as "M59 LTV Cuml. $ Per Vol."
    , div0("M60 LTV Cuml. $", cohort_volume) as "M60 LTV Cuml. $ Per Vol."
    , div0("M61 LTV Cuml. $", cohort_volume) as "M61 LTV Cuml. $ Per Vol."
    , div0("M62 LTV Cuml. $", cohort_volume) as "M62 LTV Cuml. $ Per Vol."
    , div0("M63 LTV Cuml. $", cohort_volume) as "M63 LTV Cuml. $ Per Vol."
    , div0("M64 LTV Cuml. $", cohort_volume) as "M64 LTV Cuml. $ Per Vol."
    , div0("M65 LTV Cuml. $", cohort_volume) as "M65 LTV Cuml. $ Per Vol."
    , div0("M66 LTV Cuml. $", cohort_volume) as "M66 LTV Cuml. $ Per Vol."
    , div0("M67 LTV Cuml. $", cohort_volume) as "M67 LTV Cuml. $ Per Vol."
    , div0("M68 LTV Cuml. $", cohort_volume) as "M68 LTV Cuml. $ Per Vol."
    , div0("M69 LTV Cuml. $", cohort_volume) as "M69 LTV Cuml. $ Per Vol."
    , div0("M70 LTV Cuml. $", cohort_volume) as "M70 LTV Cuml. $ Per Vol."
    from final
    order by 1;