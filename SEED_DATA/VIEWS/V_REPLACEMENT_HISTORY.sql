create or replace view SEED_DATA.DEV.V_REPLACEMENT_HISTORY(
	INVOICE_ID,
	INVOICE_NUMBER,
	SKU_DESCRIPTION,
	ADJUSTMENT_PLAN_CODE,
	SKU,
	ITEM_DESCRIPTION,
	INVOICE_DATE,
    ORDER_SHIPPED_DATE,
   replacement_flag,
	TOTAL_DISCOUNT,
	TOTAL_TAX,
	TOTAL_ADJ_PRICE,
	BASE_PRICE,
	TOTAL_QUANTITY
) as 

with base as(
select 
      invoice_id
    , invoice_number
    , ADJUSTMENT_DESCRIPTION as SKU_DESCRIPTION
    , adjustment_plan_code
    , CASE when sku_description ilike '%Daily Synbiotic' then 'syn-wk'
        when sku_description ilike '%Daily Synbiotic—Refill' then 'syn-rf'
        when sku_description ilike '%Daily Synbiotic—Refill (2 month)%' then 'syn-rf-2mo'
        when sku_description ilike '%Daily Synbiotic—Refill (3 month)%' then 'syn-rf-3mo'
        when sku_description ilike '%Daily Synbiotic—Refill (6 month)%' then 'syn-rf-6mo'
        when sku_description ilike '%Pediatric Daily Synbiotic' then 'pds-wk'
        when sku_description ilike '%Pediatric Daily Synbiotic—Refill' then 'pds-rf'
        when sku_description ilike '%Pediatric Daily Synbiotic—Refill (2 month)' then 'pds-rf-2mo'
        else 'others' end as SKU
    , case when adjustment_description ilike '%Gift%' then 'N'
           when adjustment_description iLIKE '%Trial%' then 'N'
           when adjustment_description iLIKE'%Preorder%' then 'N'
           else 'Y' end as replacement_flag
    , adjustment_tax_code as item_description
    , to_date(ADJUSTMENT_CREATED_AT) as INVOICE_DATE
    , adjustment_discount as total_discount
    , adjustment_tax as total_tax
    , adjustment_total as total_adj_price
    , adjustment_amount as base_price
    , adjustment_quantity as total_quantity

from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
where adjustment_amount = 0
  and  ADJUSTMENT_ORIGIN  not ilike 'shipping%' 
  
  ),

shipping as
( select 
 distinct split_part(order_number, '-',  2) as order_number,
 order_shipped_date
 
 from "SEED_DATA"."DEV"."SHIPMENT_HISTORY"

)

select 
    base.INVOICE_ID,
	base.INVOICE_NUMBER,
	base.SKU_DESCRIPTION,
	base.ADJUSTMENT_PLAN_CODE,
	base.SKU,
	base.ITEM_DESCRIPTION,
	base.INVOICE_DATE,
    ORDER_SHIPPED_DATE,
    base.replacement_flag,
	base.TOTAL_DISCOUNT,
	base.TOTAL_TAX,
	base.TOTAL_ADJ_PRICE,
	base.BASE_PRICE,
	base.TOTAL_QUANTITY


from base
left join shipping on base.invoice_number = shipping.order_number