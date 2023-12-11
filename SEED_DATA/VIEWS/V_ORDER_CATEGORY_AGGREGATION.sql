create or replace view SEED_DATA.DEV.V_ORDER_CATEGORY_AGGREGATION as 
with all_orders as 
(
    select *,
        case when sku ilike '%syn%' or sku ilike 'ds01%' then 'DS-01'
                                when sku ilike '%pds%' then 'PDS-08'
                                else null end as product,
                case when sku ilike '%wk%' then 'Welcome Kit'
                    when sku ilike '%rf' then 'Refill'
                    when sku ilike '%2mo%' then 'Refill - 2 Months'
                    when sku ilike '%3mo%' then 'Refill - 3 Months'
                    when SKU ilike '%6mo%' then 'Refill - 6 Months'
                    else null end as sku_clean,
                    rank() over(partition by subscription_id order by invoice_date) as rank_inv
    from v_order_history 
    where sku_clean is not null or product is not null
),

final_table as 
(
    select invoice_id, to_date(invoice_date) as invoice_date, min(case when rank_inv = 1 then 1 else 2 end) invoice_rank, 
        count(distinct product) as product_count, max(case when quantity = 1 then 1 else 2 end) as quantity
    from all_orders
    group by 1, 2
)

select to_date(invoice_date) as invoice_date,case when invoice_rank = 1 then 'new' else 'ongoing' end as invoice_category,
    case when product_count = 1 then 'Single Product' else 'Multiple Product' end as prodcut_category
    ,case when quantity = 1 then 'Single Quantity' else 'Multiple Quantity' end as quantity_category
    ,count(invoice_id) as invoice_count
from final_table
group by 1,2,3,4
order by 1, 5 desc;