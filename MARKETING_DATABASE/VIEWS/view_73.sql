create or replace view MARKETING_DATABASE.PUBLIC."V_MERGEDRECHARGEANDRECURLYSUBSCRIPTIONS_FORCOHORTS_V2.1"(
	RECHARGE_SUBSCRIPTION_ID,
	RECURLY_SUBSCRIPTION_ID,
	IS_RECHARGE_NATIVE,
	IS_RECURLY_NATIVE,
	IS_IMPORTED,
	CUSTOMER_ID,
	CUSTOMER_EMAIL,
	CURRENT_STATUS,
	CREATED_AT,
	ACTIVATED_AT,
	CANCELLED_AT,
	PRICE,
	QUANTITY
) as

    -- Get all (UNIMPORTED) Rechage-Only Subscriptions
    select to_varchar(recharge_s.id) as recharge_subscription_id, null as recurly_subscription_id, 
        1 as is_recharge_native, 0 as is_recurly_native, 0 as is_imported,
        to_varchar(recharge_c.id) as customer_id, recharge_c.email as customer_email,
        recharge_s.status as current_status, recharge_s.created_at as created_at, null as activated_at, recharge_s.cancelled_at as cancelled_at, recharge_s.price as price, recharge_s.quantity as quantity
    from "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" recharge_s
    join "MARKETING_DATABASE"."RECHARGE"."CUSTOMER" recharge_c on recharge_c.id = recharge_s.customer_id
    where to_varchar(recharge_s.id) not in (

        select *
        from
        (
            select distinct trim(to_varchar(MAPPED_RECHARGE_SUBSCRIPTION_ID)) as _RECHARGE_SUBSCRIPTION_ID 
            from "MARKETING_DATABASE"."GOOGLE_SHEETS"."RECHARGE_RECURLY_MAPPING"
            where to_varchar(MAPPED_RECHARGE_SUBSCRIPTION_ID) is not null  
            union all
            select distinct trim(to_varchar(MAPPED_RECHARGE_SUBSCRIPTION_ID)) as _RECHARGE_SUBSCRIPTION_ID 
            from "MARKETING_DATABASE"."GOOGLE_SHEETS"."RECHARGE_RECURLY_MAPPING_NOV_12_2020"
            where to_varchar(RECHARGE_SUBSCRIPTION_ID) is not null  
            union all
            select distinct trim(to_varchar(RECHARGE_SUBSCRIPTION_ID)) as _RECHARGE_SUBSCRIPTION_ID 
            from "MARKETING_DATABASE"."RECHARGE"."RECHARGE_RECURLY_MAPPING"
            where to_varchar(RECHARGE_SUBSCRIPTION_ID) is not null   
        ) as a
        where _RECHARGE_SUBSCRIPTION_ID not in ('NULL', 'canceled', 'cancel')      
      
    )

    union all

    -- Get all (IMPORTED & MAPPED) Subscriptions
    select to_varchar(mapping._recharge_subscription_id) as recharge_subscription_id, to_varchar(mapping._recurly_subscription_id) as recurly_subscription_id, 
        1 as is_recharge_native, 0 as is_recurly_native, 1 as is_imported,
        to_varchar(recharge_c.id) as customer_id, recharge_c.email as customer_email,
        recurly.state as current_status, recharge_c.created_at as created_at, recurly.activated_at as activated_at, recurly.canceled_at as cancelled_at, recurly.total_recurring_amount as price, recurly.quantity as quantity
    from "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" recharge_s
    join "MARKETING_DATABASE"."RECHARGE"."CUSTOMER" recharge_c on recharge_c.id = recharge_s.customer_id
    join
    (
        select *
        from
        (
            select distinct to_varchar(MAPPED_RECHARGE_SUBSCRIPTION_ID) as _RECHARGE_SUBSCRIPTION_ID, to_varchar(MAPPED_RECURLY_SUBSCRIPTION_ID) as _RECURLY_SUBSCRIPTION_ID 
            from "MARKETING_DATABASE"."GOOGLE_SHEETS"."RECHARGE_RECURLY_MAPPING"
            where to_varchar(MAPPED_RECHARGE_SUBSCRIPTION_ID) is not null
            union all
            select distinct to_varchar(MAPPED_RECHARGE_SUBSCRIPTION_ID) as _RECHARGE_SUBSCRIPTION_ID, to_varchar(MAPPED_RECURLY_SUBSCRIPTION_ID) as _RECURLY_SUBSCRIPTION_ID 
            from "MARKETING_DATABASE"."GOOGLE_SHEETS"."RECHARGE_RECURLY_MAPPING_NOV_12_2020"
            where to_varchar(RECHARGE_SUBSCRIPTION_ID) is not null  
            union all
            select distinct to_varchar(RECHARGE_SUBSCRIPTION_ID) as _RECHARGE_SUBSCRIPTION_ID, to_varchar(RECURLY_SUBSCRIPTION_ID) as _RECURLY_SUBSCRIPTION_ID
            from "MARKETING_DATABASE"."RECHARGE"."RECHARGE_RECURLY_MAPPING"
            where to_varchar(RECHARGE_SUBSCRIPTION_ID) is not null   
        ) as a
        where _RECHARGE_SUBSCRIPTION_ID not in ('NULL', 'canceled', 'cancel') 
        and _RECURLY_SUBSCRIPTION_ID not in ('NULL', 'no need to map since it''s cancelled', 'expired', 'cancelled', 'canceled', 'cancel')
    ) as mapping on to_varchar(mapping._recharge_subscription_id) = to_varchar(recharge_s.id)
    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as recurly on mapping._recurly_subscription_id = recurly.uuid
  
    union all 

    -- Get all (UNIMPORTED) Recurly-Only Subscriptions
    select null as recharge_subscription_id, to_varchar(uuid) as recurly_subscription_id, 
        0 as is_recharge_native, 1 as is_recurly_native, 0 as is_imported,
        to_varchar(account_code) as customer_id, email as customer_email,
        state as current_status, created_at as created_at, activated_at as activated_at, canceled_at as cancelled_at, total_recurring_amount as price, quantity as quantity
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS"
    where uuid not in (
        select *
        from
        (
          select distinct to_varchar(MAPPED_RECURLY_SUBSCRIPTION_ID) as _RECURLY_SUBSCRIPTION_ID 
          from "MARKETING_DATABASE"."GOOGLE_SHEETS"."RECHARGE_RECURLY_MAPPING"
          where to_varchar(MAPPED_RECURLY_SUBSCRIPTION_ID) is not null
          union all
          select distinct to_varchar(MAPPED_RECURLY_SUBSCRIPTION_ID) as _RECURLY_SUBSCRIPTION_ID 
          from "MARKETING_DATABASE"."GOOGLE_SHEETS"."RECHARGE_RECURLY_MAPPING_NOV_12_2020"
          where to_varchar(RECHARGE_SUBSCRIPTION_ID) is not null  
          union all
          select distinct to_varchar(RECURLY_SUBSCRIPTION_ID) as _RECURLY_SUBSCRIPTION_ID 
          from "MARKETING_DATABASE"."RECHARGE"."RECHARGE_RECURLY_MAPPING"
          where to_varchar(RECURLY_SUBSCRIPTION_ID) is not null      
         ) as a
         where _RECURLY_SUBSCRIPTION_ID not in ('NULL', 'no need to map since it''s cancelled', 'expired', 'cancelled', 'canceled', 'cancel')
    );