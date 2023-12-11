create or replace view SEED_DATA.DEV.V_RECURLY_ORDER_HISTORY as 

with recurly_rev as
(
with non_shipping_rev as (
select 
  i.id as invoice_id
, i.invoice_number
, t.transaction_id as transaction_id
, i.account_code as customer_id
, a.subscription_id as subscription_id
, i.billed_date as invoice_date
, coalesce(t.date,invoice_closed_at) as paid_date 
--- ^^ Note not all invoices need to have paid date as they could be paid from credits, hence invoice_closed_at
, adjustment_plan_code as sku
, adjustment_description as sku_description
, adjustment_quantity as quantity
, a.adjustment_amount as base_price
, a.adjustment_total as total_amount_paid
, a.adjustment_tax as tax
, a.adjustment_discount as discount
, a.adjustment_coupon_code as promotion_code
, DIV0((base_price*quantity),(sum(base_price*quantity)) over(partition by i.id)) as norm_factor
--, (base_price*quantity)/(sum(base_price*quantity) over(partition by i.id)) as norm_factor
, round((t.amount*norm_factor),2) as amount_paid_by_transaction

from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as i
left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t on i.id = t.invoice_id
left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on i.id = a.invoice_id
where (i.status = 'paid' or (i.invoice_type = 'immediate_change' and invoice_total > 0)) -- Accounting for adjusted invoices
and (t.status is null or(t.status = 'success' and t.type = 'purchase'))
and not id = '5ac475d833aa768239e223479eb1bdd6' -- This user has 6 successfully paid transactions for 1 invoice number
and adjustment_description not ilike '%shipping%'
--and i.invoice_number = 1488259
),

shipping_rev as
(
select 
  i.id as invoice_id
, sum(a.adjustment_total) as total_shipping_cost
, sum(a.adjustment_amount) as shipping_cost_wo_tax
, sum(a.adjustment_tax) as shipping_cost_tax
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as i
left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t on i.id = t.invoice_id
left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on i.id = a.invoice_id
where i.status = 'paid' and (t.status is null or(t.status = 'success' and t.type = 'purchase'))
and not id = '5ac475d833aa768239e223479eb1bdd6' -- This user has 6 successfully paid transactions for 1 invoice number
and  adjustment_description ilike '%shipping%'
group by 1
),

refund_rev as
(
select 
  original_invoice_number as applied_to_invoice_number
, sum(abs(invoice_total)) as refunded_amount
from IO06230_RECURLY_SEED_SHARE.CLASSIC.INVOICES_SUMMARY as i
where 
((i.invoice_type = 'termination' or i.invoice_type = 'refund') and i.status in ('closed','open')) 
or 
(i.invoice_type = 'immediate_change' and i.status in ('closed','open') and invoice_total < 0)
group by 1
),

credit_rev as
(
select 
  applied_to_invoice_number as applied_to_invoice_number
, sum(amount) as credit_amount
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."CREDIT_PAYMENTS"
where action = 'payment'
group by 1
),

final_table as
(
select 
  nsr.*
, round((sr.total_shipping_cost*norm_factor),2) as total_shipping_cost
, round((sr.shipping_cost_wo_tax*norm_factor),2) as shipping_cost_wo_tax
, round((sr.shipping_cost_tax*norm_factor),2) as shipping_cost_tax
, round((rr.refunded_amount*norm_factor),2) as amount_refunded
, round((cr.credit_amount*norm_factor),2) as credit_applied
from non_shipping_rev as nsr
left join shipping_rev as sr on nsr.invoice_id = sr.invoice_id
left join refund_rev as rr on nsr.invoice_number = rr.applied_to_invoice_number
left join credit_rev as cr on nsr.invoice_number = cr.applied_to_invoice_number
)
select * from final_table
)

select * from recurly_rev;