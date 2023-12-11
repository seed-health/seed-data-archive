create or replace view SEED_DATA.DEV.V_SHIPPING_FINALIZED_SKU as 

WITH updated_invoices as (
select 
*
, CONCAT('SEED-',I.invoice_number) as updated_invoice_number
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as I
where billed_date > '2021-12-15'
 )

, updated_sku_adjustments as (    
select 
  i.updated_invoice_number as order_number
, a.ADJUSTMENT_PLAN_CODE as SKU
, a.ADJUSTMENT_QUANTITY as quantity
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a 
left join updated_invoices as i on a.invoice_id = i.id
where 
adjustment_created_at > '2021-12-31'
and adjustment_description not ilike '%shipping%'
and a.adjustment_plan_code in ('syn-wk','syn-rf','syn-rf-2mo','syn-rf-3mo','syn-rf-6mo','pds-wk','pds-rf','pds-rf-2mo')
)
     
, finalized_sku as (
select 
*
from updated_sku_adjustments 
pivot(sum(quantity) for SKU in ('syn-wk','syn-rf','syn-rf-2mo','syn-rf-3mo','syn-rf-6mo','pds-wk','pds-rf','pds-rf-2mo')) as p 
order by order_number
)

select * from finalized_sku;