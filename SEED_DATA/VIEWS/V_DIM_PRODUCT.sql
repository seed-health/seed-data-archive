create or replace view SEED_DATA.DEV.V_DIM_PRODUCT 
AS
with sku_build_from_orders as 
        (
            select 
                distinct
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
                    end as product_desc
            from "SEED_DATA"."DEV"."V_ORDER_HISTORY" as o 
        )


select * from sku_build_from_orders