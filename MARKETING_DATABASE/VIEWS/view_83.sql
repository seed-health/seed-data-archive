create view v_MergedRechargeAndRecurlySubscriptions
as

    -- Get all (UNIMPORTED) Rechage-Only Subscriptions
    select to_varchar(id) as recharge_subscription_id, null as recurly_subscription_id, 1 as is_recharge_native, 0 as is_recurly_native, 0 as is_imported
    from "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION"
    where id not in (select recharge_subscription_id from "MARKETING_DATABASE"."RECHARGE"."RECHARGE_RECURLY_MAPPING")

    union all

    -- Get all (IMPORTED & MAPPED) Subscriptions
    select mapping.recharge_subscription_id, mapping.recurly_subscription_id, 1 as is_recharge_native, 0 as is_recurly_native, 1 as is_imported
    from "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" as recharge
    join "MARKETING_DATABASE"."RECHARGE"."RECHARGE_RECURLY_MAPPING" as mapping on mapping.recharge_subscription_id = recharge.id
    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as recurly on mapping.recurly_subscription_id = recurly.uuid

    union all 

    -- Get all (UNIMPORTED) Recurly-Only Subscriptions
    select null as recharge_subscription_id, uuid as recurly_subscription_id, 0 as is_recharge_native, 1 as is_recurly_native, 0 as is_imported
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS"
    where uuid not in (select recharge_subscription_id from "MARKETING_DATABASE"."RECHARGE"."RECHARGE_RECURLY_MAPPING")