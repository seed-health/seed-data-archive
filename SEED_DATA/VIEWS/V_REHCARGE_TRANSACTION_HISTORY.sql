create or replace view SEED_DATA.DEV.V_REHCARGE_TRANSACTION_HISTORY as 

with recharge_charge_line_item as
(
select 
  cli.charge_id
, cli.index
, cli.subscription_id
, cli.quantity
, cli.sku
, case when cli.sku ilike '%3MO' then cli.quantity*3 else cli.quantity end as adjusted_quantity
, sum(adjusted_quantity) over(partition by cli.charge_id) as adjusted_quantity_total
, (adjusted_quantity / adjusted_quantity_total) as norm_factor
from "MARKETING_DATABASE"."RECHARGE"."CHARGE_LINE_ITEM" as cli
where sku not ilike '12345'-- Filtering out what looks like a test SKU
)

,recharge_discount_code as
(
select 
  cdc.charge_id
, d.code as discount_code
from "MARKETING_DATABASE"."RECHARGE"."CHARGE_DISCOUNT_CODE" as cdc
left join "MARKETING_DATABASE"."RECHARGE"."DISCOUNT" as d on cdc.discount_id = d.id
)

,recharge_rev as
(
select 
  c.id as transaction_id
, c.customer_id as customer_id
, c.email as customer_email
, cli.subscription_id
--, to_date(dateadd(hour, -4, c.processed_at)) as order_date ---- This is the old code that we can revisit / needs to be UTC
, c.processed_at as order_date_ts
, cli.sku as sku
, cli.quantity as quantity
, (c.total_line_items_price*cli.norm_factor) as base_price
, (c.total_price*cli.norm_factor) as total_amount_paid
, (c.total_tax*cli.norm_factor) as tax
, (c.total_discounts*cli.norm_factor) as discount
, dc.discount_code as promotion_code
, (csl.price*cli.norm_factor)  as shipping_cost
, (c.total_refunds*cli.norm_factor) as refund_amount

from "MARKETING_DATABASE"."RECHARGE"."CHARGE" as c
join recharge_charge_line_item as cli on cli.charge_id = c.id
left join "MARKETING_DATABASE"."RECHARGE"."CHARGE_SHIPPING_LINE" as csl on csl.charge_id = c.id
left join recharge_discount_code as dc on dc.charge_id = c.id

where processed_at is not null --- need to have processed date / for recharge, it has to be successfully processed
)

,recharge_final as (
select 
  rc.transaction_id
, rc.customer_id
, rc.subscription_id
, rc.order_date_ts
, to_date(rc.order_date_ts) as order_date
, rc.sku
, rc.promotion_code
, ifnull(sum(rc.quantity),0) as quantity
, ifnull(sum(rc.base_price),0) as base_price ---- look at potentially using differnt price column
, ifnull(sum(rc.total_amount_paid),0) as total_amount_paid
, ifnull(sum(rc.tax),0) as tax
, ifnull(sum(rc.discount),0) as discount
, ifnull(sum(rc.shipping_cost),0) as shipping_cost
, ifnull(sum(rc.refund_amount),0) as refund_amount
from recharge_rev as rc
group by 1,2,3,4,5,6,7 )

select * from recharge_final;