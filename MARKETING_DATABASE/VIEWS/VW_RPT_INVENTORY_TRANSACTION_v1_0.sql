create or replace view "MARKETING_DATABASE"."OCEANX_PRODUCTION"."VW_RPT_INVENTORY_TRANSACTION_v1.0"
as

  select *
  from "MARKETING_DATABASE"."OCEANX_PRODUCTION"."EXPORT_VW_RPT_INVENTORY_TRANSACTION"
  order by INVENTORY_TRANSACTION_ID, TRANSACTION_DATETIME