create view "MARKETING_DATABASE"."PUBLIC"."v_first_recurly_adjustment"
as
    select adj_first_sku.subscription_id, 
            adj_first_sku.adjustment_start_at as first_adjustment_start_at, 
            adj_first_sku.adjustment_product_code as first_adjustment_product_code,
            adj_first_sku.adjustment_quantity as first_adjustment_quantity,
            adj_first_sku.adjustment_total as first_adjustment_total,
            adj_first_sku.adjustment_tax as first_adjustment_total_tax,
            adj_first_sku.adjustment_discount as first_adjustment_total_discount

    from
    (
      select min(adjustment_start_at) as adjustment_start_at, subscription_id
      from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
      where adjustment_description not ilike 'Shipping%' 
      and adjustment_type = 'charge' 
      and subscription_id is not null
      group by subscription_id
    ) as adj_first
    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as adj_first_sku 
        on adj_first_sku.adjustment_start_at = adj_first.adjustment_start_at
        and adj_first_sku.subscription_id = adj_first.subscription_id
    where adj_first_sku.adjustment_description not ilike 'Shipping%' 
    and adj_first_sku.adjustment_type = 'charge' 
    and adj_first_sku.subscription_id is not null    
    order by adj_first_sku.adjustment_start_at, adj_first_sku.subscription_id