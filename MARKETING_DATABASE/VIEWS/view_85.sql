create or replace view MARKETING_DATABASE.PUBLIC.V_SUBSCRIPTION_MAPPING_DS_01(
	RECHARGE_SUBSCRIPTION_ID,
	RECURLY_SUBSCRIPTION_ID,
	IS_RECHARGE_NATIVE,
	IS_RECURLY_NATIVE,
	IS_IMPORTED,
	CUSTOMER_ID,
	CUSTOMER_EMAIL,
	FIRST_NAME,
	LAST_NAME,
	CURRENT_STATUS,
	CREATED_AT,
	ACTIVATED_AT,
	CANCELLED_AT,
	PRICE,
	QUANTITY,
	FULL_ADDRESS,
	BILLING_COUNTRY,
    BILLING_CITY,
    BILLING_STATE,
	IS_INTERNATIONAL,
	SKU,
	IS_PREORDER
) as

    -- Get all (UNIMPORTED) Rechage-Only Subscriptions
    select to_varchar(recharge_s.id) as recharge_subscription_id, 
            null as recurly_subscription_id, 
            1 as is_recharge_native, 
            0 as is_recurly_native, 
            0 as is_imported,
            to_varchar(recharge_c.id) as customer_id, 
            recharge_c.email as customer_email,
            recharge_c.first_name as first_name,
            recharge_c.last_name as last_name,
            recharge_s.status as current_status, 
            recharge_s.created_at as created_at, 
            null as activated_at, 
            recharge_s.cancelled_at as cancelled_at, 
            recharge_s.price as price, 
            recharge_s.quantity as quantity,
            recharge_c.BILLING_ADDRESS_1 as full_address,
            recharge_c.billing_country,
            recharge_c.billing_city as billing_city,
            recharge_c.billing_province as billing_state,
            case when recharge_c.billing_country in ('United States', 'US') then 0 else 1 end as is_international,            
            recharge_s.sku,
            null as is_preorder
    from "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" recharge_s
    join "MARKETING_DATABASE"."RECHARGE"."CUSTOMER" recharge_c on recharge_c.id = recharge_s.customer_id
    where to_varchar(recharge_s.id) not in (
        select distinct _RECHARGE_SUBSCRIPTION_ID
        from "MARKETING_DATABASE"."PRODUCTION_VIEWS"."V_MAPPING_CONSOLIDATED"
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
            recharge_c.first_name as first_name,
            recharge_c.last_name as last_name,
            recurly.state as current_status, 
            recharge_s.created_at as created_at, 
            null as activated_at, -- precaution: we dont know the exact "activated at" for imporated subscriptions
            -- recurly.activated_at as activated_at, 
            recurly.canceled_at as cancelled_at, 
            recurly.total_recurring_amount as price, 
            recurly.quantity as quantity,
            recharge_c.billing_address_1 as full_address,
            recharge_c.billing_country,
            recharge_c.billing_city as billing_city,
            recharge_c.billing_province as billing_state,
            case when recharge_c.billing_country in ('United States', 'US') then 0 else 1 end as is_international,
            recharge_s.sku,
            null as is_preorder
    from "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" recharge_s            
    join "MARKETING_DATABASE"."RECHARGE"."CUSTOMER" recharge_c on recharge_c.id = recharge_s.customer_id
    join
    (
        select distinct _RECHARGE_SUBSCRIPTION_ID, _RECURLY_SUBSCRIPTION_ID
        from "MARKETING_DATABASE"."PRODUCTION_VIEWS"."V_MAPPING_CONSOLIDATED"       
    ) as mapping on to_varchar(mapping._recharge_subscription_id) = to_varchar(recharge_s.id)
    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as recurly on mapping._recurly_subscription_id = recurly.uuid
  
    union all 

    -- Get all (UNIMPORTED) Recurly-Only Subscriptions
    select null as recharge_subscription_id, 
            to_varchar(uuid) as recurly_subscription_id, 
            0 as is_recharge_native, 
            1 as is_recurly_native, 
            0 as is_imported,
            to_varchar(s.account_code) as customer_id, 
            email as customer_email,
            a.account_first_name as first_name,
            a.account_last_name as last_name,
            state as current_status, 
            created_at as created_at, 
            activated_at as activated_at, 
            canceled_at as cancelled_at, 
            total_recurring_amount as price, 
            quantity as quantity,
            ship_address_street1 as full_address,
            ship_address_country,
            SHIP_ADDRESS_CITY,
            SHIP_ADDRESS_STATE,
            case when ship_address_country in ('United States', 'US') then 0 else 1 end as is_international,
            adj_last.last_adjustment_product_code as sku,
            case when adj_first.first_adjustment_total = 0 then 1 else 0 end as is_preorder
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" s LEFT JOIN "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ACCOUNTS" a ON s.account_code = a.account_code 
    left join "MARKETING_DATABASE"."PRODUCTION_VIEWS"."V_FIRST_RECURLY_ADJUSTMENT" adj_first on adj_first.subscription_id = s.uuid
    left join "MARKETING_DATABASE"."PRODUCTION_VIEWS"."V_LAST_RECURLY_ADJUSTMENT" adj_last on adj_last.subscription_id = s.uuid
    where (s.plan_name ilike '%DS-01%' or s.plan_name ilike 'Daily Synbiotic%')
          and uuid not in (
            select distinct _RECURLY_SUBSCRIPTION_ID
            from "MARKETING_DATABASE"."PRODUCTION_VIEWS"."V_MAPPING_CONSOLIDATED"
                        );