create view MARKETING_DATABASE.PUBLIC.V_SUBSCRIPTION_MAPPING_v1_7
as

    -- Get all (UNIMPORTED) Rechage-Only Subscriptions
    select to_varchar(recharge_s.id) as recharge_subscription_id, 
            null as recurly_subscription_id, 
            1 as is_recharge_native, 
            0 as is_recurly_native, 
            0 as is_imported,
            to_varchar(recharge_c.id) as customer_id, 
            recharge_c.email as customer_email,
            recharge_s.status as current_status, 
            recharge_s.created_at as created_at, 
            null as activated_at, 
            recharge_s.cancelled_at as cancelled_at, 
            recharge_s.price as price, 
            recharge_s.quantity as quantity,
            recharge_c.billing_country,
            case when recharge_c.billing_country in ('United States', 'US') then 0 else 1 end as is_international,            
            recharge_s.sku,
            null as is_preorder
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
    select to_varchar(mapping._recharge_subscription_id) as recharge_subscription_id, 
            to_varchar(mapping._recurly_subscription_id) as recurly_subscription_id, 
            1 as is_recharge_native, 
            0 as is_recurly_native, 
            1 as is_imported,
            to_varchar(recharge_c.id) as customer_id, 
            recharge_c.email as customer_email,
            recurly.state as current_status, 
            recharge_s.created_at as created_at, 
            null as activated_at, -- precaution: we dont know the exact "activated at" for imporated subscriptions
            -- recurly.activated_at as activated_at, 
            recurly.canceled_at as cancelled_at, 
            recurly.total_recurring_amount as price, 
            recurly.quantity as quantity,
            recharge_c.billing_country,
            case when recharge_c.billing_country in ('United States', 'US') then 0 else 1 end as is_international,
            recharge_s.sku,
            null as is_preorder
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
    select null as recharge_subscription_id, 
            to_varchar(uuid) as recurly_subscription_id, 
            0 as is_recharge_native, 
            1 as is_recurly_native, 
            0 as is_imported,
            to_varchar(account_code) as customer_id, 
            email as customer_email,
            state as current_status, 
            created_at as created_at, 
            activated_at as activated_at, 
            canceled_at as cancelled_at, 
            total_recurring_amount as price, 
            quantity as quantity,            
            ship_address_country,
            case when ship_address_country in ('United States', 'US') then 0 else 1 end as is_international,
            adj_first.first_adjustment_product_code as sku,
            case when adj_first.first_adjustment_total = 0 then 1 else 0 end as is_preorder
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" s
    left join "MARKETING_DATABASE"."PUBLIC"."v_first_recurly_adjustment" adj_first on adj_first.subscription_id = s.uuid
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
    )
    
    union all
    
    -- Get all (UNIMPORTED) Recurly-Only Subscriptions ** THAT ARE GIFT KITS
    select null as recharge_subscription_id, 
            null as recurly_subscription_id, 
            0 as is_recharge_native, 
            1 as is_recurly_native, 
            0 as is_imported,
            to_varchar(acc.account_code) as customer_id, 
            acc.ACCOUNT_EMAIL as customer_email,
            'ACTIVE' as current_status, 
            inv.BILLED_DATE as created_at, 
            inv.BILLED_DATE as activated_at, 
            null as cancelled_at, 
            adj.adjustment_total as price, 
            adj.adjustment_quantity as quantity,            
            inv.ship_address_country,
            case when inv.ship_address_country in ('United States', 'US') then 0 else 1 end as is_international,
            adj.adjustment_product_code as sku,
            0 as is_preorder
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" adj
    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" inv on inv.id = adj.INVOICE_ID
    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ACCOUNTS" acc on acc.ACCOUNT_CODE = adj.ACCOUNT_CODE    
    where adj.subscription_id is null
      and adj.adjustment_description not ilike 'Shipping%' 
      and adj.adjustment_type = 'charge'      
      and adj.adjustment_product_code ilike '%gift%'