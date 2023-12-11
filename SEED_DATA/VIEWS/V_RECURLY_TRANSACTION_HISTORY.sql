create or replace view SEED_DATA.DEV.V_RECURLY_TRANSACTION_HISTORY(
	TRANSACTION_ID,
	CUSTOMER_ID,
	SUBSCRIPTION_ID,
	ORDER_DATE_TS,
	ORDER_DATE,
	SKU,
	PROMOTION_CODE,
	QUANTITY,
	BASE_PRICE,
	TOTAL_AMOUNT_PAID,
	TAX,
	DISCOUNT,
	SHIPPING_COST,
	REFUND_AMOUNT
) as 

with non_shipping_rev as
(
select 
  t.transaction_id as transaction_id
, t.account_code as customer_id
, t.subscription_id as subscription_id
, t.date as order_date
, a.adjustment_plan_code as sku
, a.adjustment_coupon_code as promotion_code
, ifnull(sum(a.adjustment_quantity),0) as quantity
, ifnull(sum(a.adjustment_amount),0) as base_price
, ifnull(sum(a.adjustment_total),0) as total_amount_paid
, ifnull(sum(a.adjustment_tax),0) as tax
, ifnull(sum(a.adjustment_discount),0) as discount

from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id

----- only for successful purchases and adjustment descriptions does not include shipping
where lower(t.type) = 'purchase'
and lower(t.status) = 'success'
and a.adjustment_description not ilike '%shipping%' 

group by 1,2,3,4,5,6
)

,shipping_rev as
(
select 
  transaction_id
, ifnull(sum(a.adjustment_amount),0) as shipping_cost  ----- aggregated the data 

from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id

----- only for successful purchases and adjustment descriptions includes shipping
where t.type = 'purchase'
and t.status = 'success'
and  adjustment_description ilike '%shipping%'

group by 1
),

refund_rev as
(
select 
  t.original_transaction_id as transaction_id ---- normalized
, ifnull(sum(t.amount),0) as refund_amount ----- aggregated the data 
--, t.date as refund_date
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id

----- only for successful refunds and adjustment descriptions does not include shipping
where t.type = 'refund'
and t.status = 'success'
and adjustment_description not ilike '%shipping%'

group by 1
)

,recurly_final as (
select 
  nsr.transaction_id
, nsr.customer_id
, nsr.subscription_id
, nsr.order_date as order_date_ts
, to_date(nsr.order_date) as order_date
, nsr.sku
, nsr.promotion_code
, ifnull(sum(nsr.quantity),0) as quantity
, ifnull(sum(nsr.base_price),0) as base_price
, ifnull(sum(nsr.total_amount_paid),0) as total_amount_paid
, ifnull(sum(nsr.tax),0) as tax
, ifnull(sum(nsr.discount),0) as discount
, ifnull(sum(sr.shipping_cost),0) as shipping_cost
, ifnull(sum(rr.refund_amount),0) as refund_amount

from non_shipping_rev as nsr ----- non-shipping revenue
left join shipping_rev as sr on nsr.transaction_id = sr.transaction_id ---- joining to shipping revenue transactions
left join refund_rev as rr on nsr.transaction_id = rr.transaction_id ---- joining to refund revenue transactions

group by 1,2,3,4,5,6,7 )

select * from recurly_final;