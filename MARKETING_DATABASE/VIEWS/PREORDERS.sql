create or replace view MARKETING_DATABASE.PRODUCTION_VIEWS.PREORDERS
as

select
to_date(map.created_at) as subscription_created_at,
to_date(map.activated_at) as subscription_activated_at,
to_date(map.cancelled_at) as subscription_canceled_at,
orders_adj.*

from(
select o.email_address as email,
a.account_code as acct_code, 
a.subscription_id as subscription_id, 
a.invoice_number as invoice_number, 
to_date(a.adjustment_created_at) as adj_date,
to_date(o.order_date) as ocx_order_date,
to_date(o.order_received_date) as ocx_order_received,
to_date(o.ship_date) as ocx_order_shipped,
a.adjustment_total as total, 
a.adjustment_discount as discount, 
a.adjustment_quantity as quantity, 
a.adjustment_description as description,
a.adjustment_product_code as sku,
o.order_status as order_status
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a 
left join "MARKETING_DATABASE"."OCEANX_PRODUCTION"."VW_RPT_ORDER_v1.0" as o on a.invoice_number = o.order_number
where a.adjustment_description not ilike '%shipping%') as orders_adj
join "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING" as map on 
orders_adj.subscription_id = map.RECURLY_SUBSCRIPTION_ID and
orders_adj.acct_code = map.customer_id
where subscription_created_at > '2021-03-24'
order by orders_adj.acct_code asc,subscription_created_at desc,orders_adj.subscription_id desc