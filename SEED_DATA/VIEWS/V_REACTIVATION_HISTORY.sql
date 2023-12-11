create or replace view SEED_DATA.DEV.V_REACTIVATION_HISTORY as 
    with orders as 
    (
        with orders_clean as 
        (
            select subscription_id, 
                invoice_date, 
                lower(sku) as sku,
                case when sku ilike '%syn%' or sku ilike 'ds01%' then 'DS-01'
                    when sku ilike '%pds%' then 'PDS-08'
                    else null end as product,
                case when product = 'DS-01' and sku ilike '%wk%' then 'DS-01 Welcome Kit'
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
                row_number() over(partition by subscription_id,product order by invoice_date,sku_ranking) as first_invoice_product_rank,
                row_number() over (partition by subscription_id,product order by invoice_date desc ,sku_ranking desc) as last_invoice_product_rank,
                row_number() over (partition by subscription_id order by invoice_date,sku_ranking ) as first_invoice_rank,
                row_number() over (partition by subscription_id order by invoice_date desc, sku_ranking desc) as last_invoice_rank,
                case when product = 'DS-01' and first_invoice_product_rank = 1 then invoice_date
                    else null end as first_order_date_ds01,
                case when product = 'PDS-08' and first_invoice_product_rank = 1 then invoice_date
                    else null end as first_order_date_pds08,
                case when first_invoice_rank = 1 then invoice_date 
                    else null end as first_order_date,
                case when product = 'DS-01' and last_invoice_product_rank = 1 then invoice_date
                    else null end as last_order_date_ds01,
                case when product = 'PDS-08' and last_invoice_product_rank = 1 then invoice_date
                    else null end as last_order_date_pds08,
                case when last_invoice_rank = 1 then invoice_date 
                    else null end as last_order_date,
                case when first_invoice_rank = 1 then product
                    else null end as first_product,
                case when first_invoice_rank = 1 then clean_sku
                    else null end as first_sku,
                case when last_invoice_rank = 1 then product 
                    else null end as last_product,
                case when last_invoice_rank = 1 then clean_sku
                    else null end as last_sku
    
            from "SEED_DATA"."DEV"."V_ORDER_HISTORY" as o 
            where subscription_id is not null
        ),
    
        order_summary as
        (
            select
                subscription_id, 
                min(first_order_date_ds01) as first_order_date_ds01,
                min(first_order_date_pds08) as first_order_date_pds08,
                min(first_order_date) as first_order_date,
                min(first_product) as first_product,
                min(first_sku) as first_sku,
                max(last_order_date_ds01) as last_order_date_ds01,
                max(last_order_date_pds08) as last_order_date_pds08,
                max(last_order_date) as last_order_date,
                max(last_product) as last_product,
                max(last_sku) as last_sku 
            from orders_clean    
            group by subscription_id
        )
    
        select recharge_subscription_id,
            recurly_subscription_id,
            min(coalesce(order_recharge.first_order_date_ds01,order_recurly.first_order_date_ds01)) as first_order_date_ds01,
            min(coalesce(order_recharge.first_order_date_pds08,order_recurly.first_order_date_pds08)) as first_order_date_pds08,
            min(coalesce(order_recharge.first_order_date,order_recurly.first_order_date)) as first_order_date,
            min(coalesce(order_recharge.first_product,order_recurly.first_product)) as first_product,
            min(coalesce(order_recharge.first_sku,order_recurly.first_sku)) as first_sku,
            max(coalesce(order_recurly.last_order_date_ds01,order_recharge.last_order_date_ds01)) as last_order_date_ds01,
            max(coalesce(order_recurly.last_order_date_pds08,order_recharge.last_order_date_pds08)) as last_order_date_pds08,
            max(coalesce(order_recurly.last_order_date,order_recharge.last_order_date)) as last_order_date,
            max(coalesce(order_recurly.last_product,order_recharge.last_product)) as last_product,
            max(coalesce(order_recurly.last_sku,order_recharge.last_sku)) as last_sku          
        from "SEED_DATA"."DEV"."V_SUBSCRIPTION" as s 
            left join order_summary as order_recharge on order_recharge.subscription_id = s.recharge_subscription_id
            left join order_summary as order_recurly on order_recurly.subscription_id = s.recurly_subscription_id
        group by recharge_subscription_id,recurly_subscription_id
       
    ), 
    
    customer_sub_info as 
    (
        with subscription_product as 
        (
            select customer_id, created_at,cancelled_at,
                case when sku ilike '%syn%' or sku ilike 'ds01%' then 'DS-01'
                    when sku ilike '%pds%' then 'PDS-08'
                    when sku is null and is_recharge_native = 1 and is_imported = 0 then 'DS-01'
                    else null end as product,
                datediff(month,created_at,coalesce(cancelled_at,current_date()))+1 as months_active
        from "SEED_DATA"."DEV"."V_SUBSCRIPTION"
        )

    select customer_id,
        min(case when product = 'DS-01' then created_at else null end) as first_subscription_date_ds01,
        min(case when product = 'PDS-08' then created_at else null end) as first_subscription_date_pds08,
        min(created_at) as first_subscription_date,
        max(case when product = 'DS-01' then created_at else null end) as last_subscription_date_ds01,
        max(case when product = 'PDS-08' then created_at else null end) as last_subscription_date_pds08,
        max(created_at) as last_subscription_date,
        min(case when product = 'DS-01' then cancelled_at else null end) as first_cancel_date_ds01,
        min(case when product = 'PDS-08' then cancelled_at else null end) as first_cancel_date_pds08,
        min(cancelled_at) as first_cancel_date,
        max(case when product = 'DS-01' then cancelled_at else null end) as last_cancel_date_ds01,
        max(case when product = 'PDS-08' then cancelled_at else null end) as last_cancel_date_pds08,
        max(cancelled_at) as last_cancel_date,
        sum(months_active) as months_active_customer -- overlaps are considered as multiple 
    from subscription_product
    group by customer_id
    ),

    cancellation as 
    (
        select recurly_subscription_id,
            to_date(subscription_canceled_at) as cancelled_date,
            primary_reason as cancelled_primary_reason, 
            secondary_reason as cancelled_secondary_reason,
            reason_group
        from "SEED_DATA"."DEV"."V_CANCELLATION_TRANSACTION_HISTORY"
    ),
    
    pause as 
    (
        select subscription_uuid as subscription_id,
            version_started_at as pause_start_date, 
            version_ended_at_clean as pause_end_date,
            row_number() over(partition by subscription_uuid order by version_started_at desc) as pause_rank
        from "SEED_DATA"."DEV"."V_SUBSCRIPTION_PAUSE_HISTORY"
        qualify pause_rank = 1 
    
    ),
    
    next_bill as 
    (
        select subscription_id, next_bill_date
        from "SEED_DATA"."DEV"."V_SUBSCRIPTION_NEXT_BILL_DATE"
    ),
    
    sku_history as 
    (
        select subscription_uuid as subscription_id, 
            min(case when updated_plan_code ilike '%3mo%' then version_started_at else null end) as first_enroll_date_srp_3mo,
            max(case when updated_plan_code ilike '%3mo%' then version_started_at else null end) as last_enroll_date_srp_3mo,
            min(case when updated_plan_code ilike '%6mo%' then version_started_at else null end) as first_enroll_date_srp_6mo,
            min(case when updated_plan_code ilike '%6mo%' then version_started_at else null end) as last_enroll_date_srp_6mo,
            case when first_enroll_date_srp_3mo is not null then 1 else 0 end as SRP_3mo_ever_flag,
            case when first_enroll_date_srp_6mo is not null then 1 else 0 end as SRP_6mo_ever_flag
        from "SEED_DATA"."DEV"."V_SUBSCRIPTION_STATUS_HISTORY"
        group by subscription_uuid
    ),
    --- Need to update shipping_cost_data
    shipping_info as 
    (
        select * 
        from "SEED_DATA"."DEV"."V_SHIPPING_COSTS_DELIVERY_HISTORY"
        limit 10
    )
    
    select s.recharge_subscription_id,
        s.recurly_subscription_id,
        s.customer_id,
        s.customer_email,
        s.created_at,
        s.quantity,
        s.cancelled_at,
        cancelled_primary_reason,
        cancelled_secondary_reason,
        reason_group,
        case when is_recharge_native = 1 then 'Recharge'
            else 'Recurly'
            end as origination_platform,
        cus_sub.first_subscription_date_ds01,
        cus_sub.first_subscription_date_pds08,
        cus_sub.first_subscription_date,
        cus_sub.last_subscription_date_ds01,
        cus_sub.last_subscription_date_pds08,
        cus_sub.last_subscription_date,
        cus_sub.first_cancel_date_ds01,
        cus_sub.first_cancel_date_pds08,
        cus_sub.first_cancel_date,
        cus_sub.last_cancel_date_ds01,
        cus_sub.last_cancel_date_pds08,
        cus_sub.last_cancel_date,
        o.first_order_date_ds01,
        o.first_order_date_pds08,
        o.first_order_date,
        o.first_product,
        o.first_sku,
        o.last_order_date_ds01,
        o.last_order_date_pds08,
        o.last_order_date,
        o.last_product,
        o.last_sku,
        (datediff(month,created_at,coalesce(cancelled_at,current_date()))+1) as months_active,
        cus_sub.months_active_customer,
        p.pause_start_date,
        p.pause_end_date,
        nb.next_bill_date,
        sku.first_enroll_date_srp_3mo,
        sku.last_enroll_date_srp_3mo,
        sku.first_enroll_date_srp_6mo,
        sku.last_enroll_date_srp_6mo,
        sku.SRP_3mo_ever_flag,
        sku.SRP_6mo_ever_flag,
        case when sku ilike '%3mo%' then 1 else 0 end as SRP_3mo_currently,
        case when sku ilike '%6mo%' then 1 else 0 end as SRP_6mo_currently,
        case when s.created_at > first_cancel_date then 1
            else 0 end as reactivation_flag
            
    from "SEED_DATA"."DEV"."V_SUBSCRIPTION" as s
        left join cancellation as cancel on s.recurly_subscription_id = cancel.recurly_subscription_id
        left join customer_sub_info as cus_sub on cus_sub.customer_id = s.customer_id
        left join orders as o on o.recharge_subscription_id = s.recharge_subscription_id and o.recurly_subscription_id = s.recurly_subscription_id
        left join pause as p on s.recurly_subscription_id = p.subscription_id
        left join next_bill as nb on s.recurly_subscription_id = nb.subscription_id
        left join sku_history as sku on sku.subscription_id = s.recurly_subscription_id;