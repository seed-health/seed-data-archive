create view ocx_customer_view as (

select 

a.order_item_id as ID,
a.email_address as Email,
a.order_status as Delivery,
a.shipto_address_1 as Delivery_address,
a.shipto_city as Delivery_address_city,
a.shipto_state as Delivery_address_State,
a.shipto_country as Delivery_address_country,
min(a.ship_date) as first_ship_date,
max(a.ship_date) as last_ship_date,
a.order_status as current_delivery_status,
a.product_name as product_name,
a.tracking_number as tracking_number,
avg(a.order_ship_amt) as shipping_cost,
count(order_returned_date) as packages_returned

from "MARKETING_DATABASE"."OCEANX"."VW_RPT_ORDER" as a
group by a.order_item_id,
a.email_address,
a.order_status,
a.shipto_address_1,
a.shipto_city,
a.shipto_state,
a.shipto_country,
a.order_status,
a.product_name,
a.tracking_number
  
  )