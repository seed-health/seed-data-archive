create or replace view MARKETING_DATABASE.PUBLIC.FULFILLMENT_REPORT
as

select 
received.day as day,
shipped.pcode as product_code,
received.qty as received_qty,
processed.qty as processed_qty,
shipped.qty as shipped_qty

from (

  select sum(product_qty) as qty, to_date(order_received_date) as day, upper(product_code) as pcode
  from "MARKETING_DATABASE"."OCEANX_PRODUCTION"."VW_RPT_ORDER_v1.0"
  group by day, pcode
  order by day desc, pcode) as received 
  
  left join (
  
  select sum(product_qty) as qty, to_date(ship_date) as day, upper(product_code) as pcode
  from "MARKETING_DATABASE"."OCEANX_PRODUCTION"."VW_RPT_ORDER_v1.0"
  group by day, pcode
  order by day desc, pcode) as shipped on shipped.day = received.day and shipped.pcode = received.pcode
  
  join (
    
  select sum(product_qty) as qty, to_date(order_processed_date) as day, upper(product_code) as pcode
  from "MARKETING_DATABASE"."OCEANX_PRODUCTION"."VW_RPT_ORDER_v1.0"
  group by day, pcode
  
  ) as processed on processed.day = shipped.day and processed.pcode = shipped.pcode
  
  order by day desc, product_code