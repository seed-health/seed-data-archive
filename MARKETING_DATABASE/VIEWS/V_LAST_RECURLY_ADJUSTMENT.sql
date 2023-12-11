create or replace view MARKETING_DATABASE.PRODUCTION_VIEWS.V_LAST_RECURLY_ADJUSTMENT(
	SUBSCRIPTION_ID,
	LAST_ADJUSTMENT_START_AT,
	LAST_ADJUSTMENT_PRODUCT_CODE,
	LAST_ADJUSTMENT_QUANTITY,
	LAST_ADJUSTMENT_TOTAL,
	LAST_ADJUSTMENT_TOTAL_TAX,
	LAST_ADJUSTMENT_TOTAL_DISCOUNT
) as
    select adj_LAST_sku.subscription_id, 
            adj_LAST_sku.adjustment_start_at as LAST_adjustment_start_at, 
            adj_LAST_sku.adjustment_product_code as LAST_adjustment_product_code,
            adj_LAST_sku.adjustment_quantity as LAST_adjustment_quantity,
            adj_LAST_sku.adjustment_total as LAST_adjustment_total,
            adj_LAST_sku.adjustment_tax as LAST_adjustment_total_tax,
            adj_LAST_sku.adjustment_discount as LAST_adjustment_total_discount

    from
    (
      select MAX(adjustment_start_at) as adjustment_start_at, subscription_id
      from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
      where adjustment_description not ilike 'Shipping%' 
      and adjustment_type = 'charge' 
      and subscription_id is not null
      group by subscription_id
    ) as adj_LAST
    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as adj_LAST_sku 
        on adj_LAST_sku.adjustment_start_at = adj_LAST.adjustment_start_at
        and adj_LAST_sku.subscription_id = adj_LAST.subscription_id
    where adj_LAST_sku.adjustment_description not ilike 'Shipping%' 
    and adj_LAST_sku.adjustment_type = 'charge' 
    and adj_LAST_sku.subscription_id is not null    
    order by adj_LAST_sku.adjustment_start_at, adj_LAST_sku.subscription_id;