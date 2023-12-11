create or replace view MARKETING_DATABASE.PUBLIC.V_SUBSCRIPTION_MAPPING_PDS_08(
	RECURLY_SUBSCRIPTION_ID,
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
	BILLING_COUNTRY,
    BILLING_CITY,
    BILLING_STATE,
    BILLING_ZIP,
	IS_INTERNATIONAL,
	SKU,
	IS_PREORDER
) as

    -- Get all Recurly-Only Subscriptions
    select to_varchar(uuid) as recurly_subscription_id, 
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
            ship_address_city,
            ship_address_state,
            ship_address_zip,
            case when ship_address_country in ('United States', 'US') then 0 else 1 end as is_international,
            adj_first.first_adjustment_product_code as sku,
            case when adj_first.first_adjustment_total = 0 then 1 else 0 end as is_preorder
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" s 
    LEFT JOIN "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ACCOUNTS" a ON s.account_code = a.account_code 
    left join "MARKETING_DATABASE"."PRODUCTION_VIEWS"."V_FIRST_RECURLY_ADJUSTMENT" adj_first on adj_first.subscription_id = s.uuid
    where (s.plan_name ilike '%PDS-08%' or s.plan_name ilike 'Pediatric Daily Synbiotic%')
          ;