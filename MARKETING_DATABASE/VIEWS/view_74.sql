create view v_MergedRechargeAndRecurlySubscriptions_Meta_Dates
as

    -- Get all (UNIMPORTED) Rechage-Only Subscriptions
    select to_varchar(recharge_s.id) as recharge_subscription_id, null as recurly_subscription_id, 1 as is_recharge_native, 0 as is_recurly_native, 0 as is_imported,
        to_varchar(recharge_c.id) as customer_id, recharge_c.email as customer_email,
        recharge_s.status as current_status, recharge_s.created_at as created_at, null as activated_at, recharge_s.cancelled_at as cancelled_at, recharge_s.price as price, recharge_s.quantity as quantity
    from "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" recharge_s
    join "MARKETING_DATABASE"."RECHARGE"."CUSTOMER" recharge_c on recharge_c.id = recharge_s.customer_id
    where recharge_s.id not in (select recharge_subscription_id from "MARKETING_DATABASE"."RECHARGE"."RECHARGE_RECURLY_MAPPING")

    union all

    -- Get all (IMPORTED & MAPPED) Subscriptions
    select mapping.recharge_subscription_id, mapping.recurly_subscription_id, 1 as is_recharge_native, 0 as is_recurly_native, 1 as is_imported,
        to_varchar(recharge_c.id) as customer_id, recharge_c.email as customer_email,
        recurly.state as current_status, recharge_s.created_at as created_at, recurly.activated_at as activated_at, recurly.canceled_at as cancelled_at, recurly.total_recurring_amount as price, recurly.quantity as quantity
    from "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" recharge_s
    join "MARKETING_DATABASE"."RECHARGE"."CUSTOMER" recharge_c on recharge_c.id = recharge_s.customer_id
    join "MARKETING_DATABASE"."RECHARGE"."RECHARGE_RECURLY_MAPPING" as mapping on mapping.recharge_subscription_id = recharge_s.id
    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as recurly on mapping.recurly_subscription_id = recurly.uuid

    union all 

    -- Get all (UNIMPORTED) Recurly-Only Subscriptions
    select null as recharge_subscription_id, uuid as recurly_subscription_id, 0 as is_recharge_native, 1 as is_recurly_native, 0 as is_imported,
        to_varchar(account_code) as customer_id, email as customer_email,
        state as current_status, created_at as created_at, activated_at as activated_at, canceled_at as cancelled_at, total_recurring_amount as price, quantity as quantity
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS"
    where uuid not in (select recharge_subscription_id from "MARKETING_DATABASE"."RECHARGE"."RECHARGE_RECURLY_MAPPING")