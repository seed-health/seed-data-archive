create or replace view SEED_DATA.DEV.V_NON_REVENUE_SHIPMENTS as

with base as (
select 
      invoice_id
    , invoice_number
    , SKU_DESCRIPTION 
    , CASE when SKU_DESCRIPTION ilike '%Daily Synbiotic' then 'syn-wk'
        when SKU_DESCRIPTION ilike '%Daily Synbiotic—Refill' then 'syn-rf'
        when SKU_DESCRIPTION ilike '%Daily Synbiotic—Refill (2 month)%' then 'syn-rf-2mo'
        when SKU_DESCRIPTION ilike '%Daily Synbiotic—Refill (3 month)%' then 'syn-rf-3mo'
        when SKU_DESCRIPTION ilike '%Daily Synbiotic—Refill (6 month)%' then 'syn-rf-6mo'
        when SKU_DESCRIPTION ilike '%Pediatric Daily Synbiotic' then 'pds-wk'
        when SKU_DESCRIPTION ilike '%Pediatric Daily Synbiotic—Refill' then 'pds-rf'
        when SKU_DESCRIPTION ilike '%Pediatric Daily Synbiotic—Refill (2 month)' then 'pds-rf-2mo'
        when SKU_DESCRIPTION is null then 'Other'
        else SKU end as SKU
    , case when SKU_DESCRIPTION ilike '%Gift%' then 'N'
           when SKU_DESCRIPTION iLIKE '%Trial%' then 'N'
           when SKU_DESCRIPTION iLIKE'%Preorder%' then 'N'
           when TOTAL_AMOUNT_PAID > 0 then 'N'
           else 'Y' end as replacement_flag
    , to_date(INVOICE_DATE) as INVOICE_DATE
    , discount as total_discount
    , tax as total_tax
    , TOTAL_SHIPPING_Cost
    , amount_refunded
    , credit_applied
    , TOTAL_AMOUNT_PAID as TOTAL_AMOUNT_PAID
    , base_price as base_price
    , quantity as total_quantity
    ,AMOUNT_PAID_BY_TRANSACTION
  

from SEED_DATA.DEV.ORDER_HISTORY
 where AMOUNT_PAID_BY_TRANSACTION = 0 
 
    )

,shipping as
( select 
 distinct split_part(order_number, '-',  2) as order_number,
 max(order_shipped_date) as order_shipped_date
 
 from "SEED_DATA"."DEV"."SHIPMENT_HISTORY"
 group by 1

)

select 
    base.INVOICE_ID,
	base.INVOICE_NUMBER,
	base.SKU_DESCRIPTION,
	base.SKU,
	base.INVOICE_DATE,
    ORDER_SHIPPED_DATE,
    base.replacement_flag,
    base.total_discount as total_discount,
    base.total_tax as total_tax,
    base.TOTAL_SHIPPING_Cost as Total_Shipping_Cost,
    base.amount_refunded as Amount_Refunded,
    base.credit_applied as credit_applied,
    base.TOTAL_AMOUNT_PAID as TOTAL_AMOUNT_PAID,
    base.base_price as base_price,
    base.total_quantity as total_quantity,
    base.AMOUNT_PAID_BY_TRANSACTION

from base
left join shipping on base.invoice_number = shipping.order_number