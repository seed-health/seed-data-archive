create or replace view V_TRANSACTION_HISTORY as
select
adj.day,
//adj.discount_amount,
//adj.tax,
//adj.total_price,
adj.quantity,
adj.sku,
adj.title,
adj.price,
inv.ship_address_country,
inv.ship_address_state,
//inv.ship_address_city,
adj.account_code
from (
  select to_date(t.date) as day,
          a.adjustment_discount as discount_amount,
          t.tax_amount as tax,
          t.amount as total_price,
          adjustment_quantity as quantity,
          adjustment_product_code as sku,
          adjustment_description as title,
          t.amount-t.tax_amount as price,
          a.invoice_id,
          t.account_code
  from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
  join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
  where t.type = 'purchase'
      and t.status = 'success'
      and a.adjustment_description not ilike '%shipping%'
  order by day
) as adj join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as inv on adj.INVOICE_ID = inv.id

union all 

select 
to_date(dateadd(hour, -4, c.processed_at)) as day,
cli.quantity as total_subscription_quantity,
cli.SKU as sku,
cli.title as title,
cli.price as price_total,
c.shipping_address_country as country,
c.SHIPPING_ADDRESS_PROVINCE as state,
TO_VARCHAR(c.customer_id) as customer_id
from "MARKETING_DATABASE"."RECHARGE"."CHARGE" as c
join "MARKETING_DATABASE"."RECHARGE"."CHARGE_LINE_ITEM" as cli on cli.charge_id = c.id
where day is not null

order by day asc