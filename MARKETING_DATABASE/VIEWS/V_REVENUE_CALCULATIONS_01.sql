create or replace view MARKETING_DATABASE.PRODUCTION_VIEWS.V_REVENUE_CALCULATIONS_01(
	INVOICE_ID,
	INVOICE_NUMBER,
	SKU_DESCRIPTION,
	SKU,
	ITEM_DESCRIPTION,
	INVOICE_DATE,
	TOTAL_DISCOUNT,
	TOTAL_TAX,
	TOTAL_ADJ_PRICE,
	BASE_PRICE,
	TOTAL_QUANTITY,
	TOTAL_SHIPPING_AMOUNT,
	TOTAL_SHIPPING_TAX_AMOUNT,
	TOTAL_CREDIT_APPLIED,
	TOTAL_REFUND_AMOUNT,
	REFUND_DATE,
	GROSS_REVENUE,
	ADJ_GROSS_REVENUE,
	ADJ_TOTAL_PAID,
	ADJ_SUBTOTAL_PAID,
	NET_VALUE,
	INVOICE_FLAG,
	ORDER_SHIPPED_DATE,
	SHIPTO_CITY,
	SHIPTO_STATE,
	SHIPTO_COUNTRY_CODE,
	SHIPTO_ZIP,
	FULLFILMENT_PARTNER
) as 

with 
credit_final as (   

    select 
      applied_to_invoice_number as invoice_applied
    , sum(amount) as credit_amount
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."CREDIT_PAYMENTS"
    where action != 'write_off'
    group by invoice_applied   
)
    
, nonship_adj as (

    select 
      to_date(date) as date
    , t.transaction_id as transaction_id
    , t.account_code as customer_id
    , t.subscription_id as subscription_id
    , a.adjustment_plan_code as sku
    , a.adjustment_description as sku_description
    , a.adjustment_tax_code as item_description
    , a.adjustment_coupon_code as promotion_code
    , t.invoice_id
    , a.invoice_number
    ----- measures
    , null as total_price
    , ifnull(sum(a.adjustment_quantity),0) as quantity
    , ifnull(sum(a.adjustment_amount),0) as base_price
    , ifnull(sum(a.adjustment_total),0) as adj_total_price
    , ifnull(sum(a.adjustment_tax),0) as tax
    , ifnull(sum(a.adjustment_discount),0) as discount_amount

from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
----- only for successful purchases and adjustment descriptions does not include shipping
where lower(t.type) = 'purchase'
and lower(t.status) = 'success'
and a.adjustment_description not ilike '%shipping%' 
group by 1,2,3,4,5,6,7,8,9,10
)

, shipping_adj as ( 
 -- Shipping totals will NEVER match up to DGT
    select 
      t.invoice_id
    , ADJUSTMENT_plan_CODE as sku
    , adjustment_amount as shipping
    , adjustment_tax as shipping_tax

from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
where t.type = 'purchase'
and t.status = 'success'
and  adjustment_description ilike '%shipping%'

)
 
, double_invoices_clean as (
 
    select 
      invoice_id
    , invoice_number
    , sku_description
    , SKU
    , item_description
    , min(date) as invoice_date
    , sum(discount_amount) as total_discount
    , sum(tax) as total_tax
    , sum(adj_total_price) as total_adj_price
    --, sum(total_price) as total_price
    , sum(base_price) as base_price
    , sum(quantity) as total_quantity
    
from nonship_adj 
group by invoice_id,invoice_number,sku_description,SKU,item_description
)

, replacements as (
    select 
      invoice_id
    , invoice_number
    , ADJUSTMENT_DESCRIPTION as SKU_DESCRIPTION
    , CASE when sku_description ilike '%Daily Synbiotic' then 'syn-wk'
        when sku_description ilike '%Daily Synbiotic—Refill' then 'syn-rf'
        when sku_description ilike '%Daily Synbiotic—Refill (2 month)%' then 'syn-rf-2mo'
        when sku_description ilike '%Daily Synbiotic—Refill (3 month)%' then 'syn-rf-3mo'
        when sku_description ilike '%Daily Synbiotic—Refill (6 month)%' then 'syn-rf-6mo'
        when sku_description ilike '%Pediatric Daily Synbiotic' then 'pds-wk'
        when sku_description ilike '%Pediatric Daily Synbiotic—Refill' then 'pds-rf'
        when sku_description ilike '%Pediatric Daily Synbiotic—Refill (2 month)' then 'pds-rf-2mo'
        else 'others' end as SKU
    , adjustment_tax_code as item_description
    , to_date(ADJUSTMENT_CREATED_AT) as INVOICE_DATE
    , adjustment_discount as total_discount
    , adjustment_tax as total_tax
    , adjustment_total as total_adj_price
    , adjustment_amount as base_price
    , adjustment_quantity as total_quantity
                        
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
where adjustment_description ilike '%(Replacement)%'
)

, double_invoices_clean_final as (

select * from double_invoices_clean

UNION ALL

select * from replacements 
)    

, double_shipping_clean as (

select 
      invoice_id
    , sku
    , sum(shipping) as total_shipping
    , sum(shipping_tax) as total_shipping_tax
 
from shipping_adj
group by 1,2
)   

, refunds_update as
(
select 
  t.original_transaction_id as transaction_id ---- normalized
, t.invoice_id
, min(t.date) as refund_date
, ifnull(sum(t.amount),0) as refund_amount ----- aggregated the data 
--, t.date as refund_date
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id

----- only for successful refunds and adjustment descriptions does not include shipping
where t.type = 'refund'
and t.status = 'success'
and adjustment_description not ilike '%shipping%'

group by 1,2
)

, refunds as (

        with tbl1 as (
        select 
          t.date as refund_date
        , t.amount as refund_amount
        , t.original_transaction_id
        
         from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
         join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
         
         where t.type = 'refund'
         and t.status = 'success' 
         and adjustment_description not ilike '%shipping%'
         )
         
        , tbl2 as (
        select 
        distinct 
         t.transaction_id
        ,t.invoice_id
        
        from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS"  as t
        join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
        
        where t.type = 'purchase'
        and t.status = 'success'
        and adjustment_description not ilike '%shipping%' 
        and adjustment_description not ilike '%(Replacement)%'
        and adjustment_description not ilike '%preorder%'
       ) 

        select 
          tbl2.invoice_id
        , min(refund_date) as refund_date
        , sum(tbl1.refund_amount) as refund_amount
        from tbl1 
        join tbl2 on tbl2.transaction_id = tbl1.original_transaction_id
        group by 1
 )
            
, stord_shipment_cleaning as (

select 
     distinct
      order_number
    , destination_postal_code
    , ship_date
    , row_number() over(partition by order_number order by ship_date desc) as ranking
from "MARKETING_DATABASE"."STORD"."SHIPMENTS" as s
qualify ranking = 1
)
    
,oceanx_stord as (

select 
  distinct 
  order_number_updated
, order_shipped_date
, shipto_city
, shipto_state
, shipto_country_code
, shipto_zip
, fullfilment_partner
from (

select 
     distinct case when order_number ilike 'SEED%' then right(order_number,len(order_number)-5) else order_number end as order_number_updated
    , to_date(order_shipped_date) as order_shipped_date
    , UPPER(shipto_city) AS shipto_city
    , UPPER(shipto_state) AS shipto_state
    , UPPER(shipto_country_code) AS shipto_country_code
    , UPPER(shipto_zip) AS shipto_zip
    , 'ocean_x' as fullfilment_partner
from "MARKETING_DATABASE"."OCEANX_DATALAKE"."VW_DATA_SHIPMENT"

UNION ALL

select 
    distinct case when o.order_number ilike 'SEED%' 
                then right(o.order_number,len(o.order_number)-5) else o.order_number 
                end as order_number_updated
  , to_date(SHIP_DATE) as order_shipped_date
  , UPPER(dest_city) as shipto_city
  , UPPER(dest_state) as shipto_state
  , UPPER(dest_country) as shipto_country_code
  , UPPER(DESTINATION_POSTAL_CODE) as shipto_zip
  , 'stord' as fullfilment_partner
from "MARKETING_DATABASE"."STORD"."ORDERS" as o
left join stord_shipment_cleaning as s on o.order_number = s.order_number
where
o.order_type = 'sales' 
and o.order_status = 'shipped' )
where order_number_updated not like '%SAMPLE%'
)

, final_data as ( 
select
      double_invoices_clean_final.*
    , COALESCE(double_shipping_clean.total_shipping,0) as total_shipping_amount
    , COALESCE(double_shipping_clean.total_shipping_tax,0) as total_shipping_tax_amount
    , COALESCE(credit_final.credit_amount, 0) as total_credit_applied
    , COALESCE(refunds.refund_amount,0) as total_refund_amount
    , to_date(refunds.refund_date) as refund_date
    --, double_invoices_clean_final.total_price 
    , double_invoices_clean_final.base_price
        - double_invoices_clean_final.total_tax 
        - COALESCE(double_shipping_clean.total_shipping,0) 
        - COALESCE(double_shipping_clean.total_shipping_tax,0) 
        + double_invoices_clean_final.total_discount 
        + COALESCE(credit_final.credit_amount, 0) as gross_revenue
        
    , double_invoices_clean_final.base_price
        - double_invoices_clean_final.total_tax 
        - COALESCE(double_shipping_clean.total_shipping_tax,0) 
        + double_invoices_clean_final.total_discount 
        + COALESCE(double_shipping_clean.total_shipping, 0) as adj_gross_revenue
        
    , double_invoices_clean_final.base_price
        + double_invoices_clean_final.total_tax 
        + COALESCE(double_shipping_clean.total_shipping,0) 
        + COALESCE(double_shipping_clean.total_shipping_tax,0) 
        - double_invoices_clean_final.total_discount 
        - COALESCE(credit_final.credit_amount, 0) as adj_total_paid
        
    , double_invoices_clean_final.base_price
        + COALESCE(double_shipping_clean.total_shipping,0) 
        - double_invoices_clean_final.total_discount 
        - COALESCE(credit_final.credit_amount, 0) as adj_subtotal_paid
        
    ,gross_revenue/total_quantity as Net_Value
    
    , CASE WHEN net_value != 49.99 and double_invoices_clean_final.SKU not ilike '%3 month%' then 1 
        WHEN net_value != 147.97 and net_value != 149.97 and double_invoices_clean_final.SKU ilike '%3month' then 1
             else 0 end as invoice_flag

             
from double_invoices_clean_final 
left join double_shipping_clean 
on double_invoices_clean_final.invoice_id = double_shipping_clean.invoice_id 
and double_invoices_clean_final.sku = double_shipping_clean.sku 
left join credit_final
on double_invoices_clean_final.invoice_number = credit_final.invoice_applied
left join refunds on double_invoices_clean_final.invoice_id = refunds.invoice_id
--left join refunds_update on double_invoices_clean_final.invoice_id = refunds_update.invoice_id
order by invoice_number
    )

/*select 
sku,
  sum(TOTAL_DISCOUNT) as TOTAL_DISCOUNT
, sum(TOTAL_TAX) as TOTAL_TAX
, sum(TOTAL_PRICE) as TOTAL_PRICE
, sum(BASE_PRICE) as BASE_PRICE
, sum(TOTAL_QUANTITY) as TOTAL_QUANTITY
, sum(TOTAL_SHIPPING_AMOUNT) as TOTAL_SHIPPING_AMOUNT
, sum(TOTAL_SHIPPING_TAX_AMOUNT) as TOTAL_SHIPPING_TAX_AMOUNT
, sum(TOTAL_CREDIT_APPLIED) as TOTAL_CREDIT_APPLIED
, sum(TOTAL_REFUND_AMOUNT) as TOTAL_REFUND_AMOUNT
, sum(GROSS_REVENUE) as GROSS_REVENUE
, sum(ADJ_GROSS_REVENUE) as ADJ_GROSS_REVENUE
, sum(NET_VALUE) as NET_VALUE
, sum(adj_total_paid) as adj_total_paid
, sum(adj_subtotal_paid) as adj_subtotal_paid
from final_data
left join oceanx_stord as ocx_strd 
on final_data.invoice_number = ocx_strd.order_number_updated
where invoice_number = 3786521
group by 1 */

/*select 
date_trunc('month',final_data.INVOICE_DATE) as invoice_month,
count(distinct case when final_data.refund_date is not null then final_data.invoice_number end) as refund_invoices,
count(distinct final_data.invoice_number) as invoices
from final_data
left join oceanx_stord as ocx_strd 
on final_data.invoice_number = ocx_strd.order_number_updated
group by 1
order by 1 desc*/

select 
final_data.*
,ocx_strd.order_shipped_date
,UPPER(ocx_strd.shipto_city) AS shipto_city
,UPPER(ocx_strd.shipto_state) AS shipto_state
,UPPER(ocx_strd.shipto_country_code) AS shipto_country_code
,UPPER(ocx_strd.shipto_zip) AS shipto_zip
,UPPER(ocx_strd.fullfilment_partner) AS fullfilment_partner
from final_data
left join oceanx_stord as ocx_strd 
on final_data.invoice_number = ocx_strd.order_number_updated
--where invoice_number = 4192417
--where invoice_date = '2023-05-01'
--and invoice_number = 4191608
order by invoice_date
--limit 10
;