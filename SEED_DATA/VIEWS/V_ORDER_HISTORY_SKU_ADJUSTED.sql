create or replace view SEED_DATA.DEV.V_ORDER_HISTORY_SKU_ADJUSTED(
	SUBSCRIPTION_ID,
	INVOICE_ID,
	INVOICE_DATE,
	INVOICED_QUANTITY,
	PRODUCT,
	SKU_CLEAN
) as

with all_orders as 
(
    with orders as 
    (    select subscription_id,
            invoice_id,
            row_number() over (partition by subscription_id order by invoice_date) as invoice_ranking,
            case when invoice_ranking > 1 then dateadd(day,2,invoice_date) else invoice_date end as invoice_date,
            quantity as invoiced_quantity,
            case when sku ilike '%syn%' or sku ilike 'ds01%' then 'DS-01'
                            when sku ilike '%pds%' then 'PDS-08'
                            else null end as product,
            case when sku ilike '%wk' then 'Welcome Kit'
                when sku ilike '%wk-3mo%' then 'Welcome kit - 3 Months'
                when sku ilike '%wk-6mo%' then 'Welcome kit - 6 Months'
                when sku ilike '%rf' then 'Refill'
                when sku ilike '%2mo%' then 'Refill - 2 Months'
                when sku ilike '%3mo%' then 'Refill - 3 Months'
                when SKU ilike '%6mo%' then 'Refill - 6 Months'
                else null end as sku_clean
        from SEED_DATA.DEV.V_ORDER_HISTORY
        where sku_clean is not null -- removing non mainstream sku
            --and base_price <> 0 and total_amount_paid <> 0  -- removing 0$ value and paid 0$ transactions
            and subscription_id is not null -- removing invoice with no subscription mapping
    ),
    
    recursive_months AS (
      SELECT
        subscription_id,
        invoice_id,
        DATEADD(day,1*30,invoice_date),
        invoiced_quantity,
        product,
        sku_clean
      FROM orders
      WHERE (sku_clean ilike '%2 Months%' or sku_clean ilike '%3 Months%' or sku_clean ilike '%6 Months%') and (DATEADD(MONTH,1,invoice_date) <= current_date())
    
      UNION ALL
    
     SELECT
        subscription_id,
        invoice_id,
        DATEADD(day,2*30,invoice_date),
        invoiced_quantity,
        product,
        sku_clean
      FROM orders
      WHERE (sku_clean ilike '%3 Months%' or sku_clean ilike '%6 Months%') and (DATEADD(MONTH,2,invoice_date) <= current_date())
      
      UNION ALL
    
     SELECT
        subscription_id,
        invoice_id,
        DATEADD(day,3*30,invoice_date),
        invoiced_quantity,
        product,
        sku_clean
      FROM orders
      WHERE sku_clean ilike '%6 Months%' and (DATEADD(MONTH,3,invoice_date) <= current_date())
    
       UNION ALL
    
     SELECT
        subscription_id,
        invoice_id,
        DATEADD(day,4*30,invoice_date),
        invoiced_quantity,
        product,
        sku_clean
      FROM orders
      WHERE sku_clean ilike '%6 Months%' and (DATEADD(MONTH,4,invoice_date) <= current_date())
    
       UNION ALL
    
     SELECT
        subscription_id,
        invoice_id,
        DATEADD(day,5*30,invoice_date),
        invoiced_quantity,
        product,
        sku_clean
      FROM orders
      WHERE sku_clean ilike '%6 Months%' and (DATEADD(MONTH,5,invoice_date) <= current_date())
    )
    
    
    SELECT subscription_id,invoice_id,invoice_date,invoiced_quantity,product,sku_clean 
    FROM orders
    UNION ALL 
    select *
    from recursive_months
)

select * from all_orders;