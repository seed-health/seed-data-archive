create or replace  view marketing_database.dbt_production.subscription_mapping
  
   as (
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
        recharge_c.billing_country,
        case when recharge_c.billing_country in ('United States', 'US') then 0 else 1 end as is_international,            
        recharge_s.sku,
        null as is_preorder
from "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" recharge_s
join "MARKETING_DATABASE"."RECHARGE"."CUSTOMER" recharge_c on recharge_c.id = recharge_s.customer_id
where to_varchar(recharge_s.id) not in (
    select distinct _RECHARGE_SUBSCRIPTION_ID
    from marketing_database.dbt_production.mapping_consolidated
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
        recharge_c.billing_country,
        case when recharge_c.billing_country in ('United States', 'US') then 0 else 1 end as is_international,
        recharge_s.sku,
        null as is_preorder
from "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" recharge_s            
join "MARKETING_DATABASE"."RECHARGE"."CUSTOMER" recharge_c on recharge_c.id = recharge_s.customer_id
join
(
    select distinct _RECHARGE_SUBSCRIPTION_ID, _RECURLY_SUBSCRIPTION_ID
    from marketing_database.dbt_production.mapping_consolidated       
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
        ship_address_country,
        case when ship_address_country in ('United States', 'US') then 0 else 1 end as is_international,
        adj_first.first_adjustment_product_code as sku,
        case when adj_first.first_adjustment_total = 0 then 1 else 0 end as is_preorder
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" s LEFT JOIN "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ACCOUNTS" a ON s.account_code = a.account_code 
left join marketing_database.dbt_production.first_recurly_adjustment adj_first on adj_first.subscription_id = s.uuid
where uuid not in (

    select distinct _RECURLY_SUBSCRIPTION_ID
    from marketing_database.dbt_production.mapping_consolidated

)
  );