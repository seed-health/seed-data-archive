create or replace view "MARKETING_DATABASE"."OCEANX_PRODUCTION"."VW_RPT_ORDER_v1.0"
as

  select *
  from "MARKETING_DATABASE"."OCEANX_PRODUCTION"."EXPORT_VW_RPT_ORDER"
  order by order_item_id