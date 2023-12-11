create or replace view SEED_DATA.DEV.V_ORDER_HISTORY_COGS_UPDATE(
	ORDER_DATE,
	ORDER_DATE_MONTH,
	PRODUCT,
	SUBSCRIPTION_ID,
    CUSTOMER_ID,
	SKU,
	INVOICE_REFUND_FLAG,
	QUANTITY,
	BASE_PRICE,
	TAX,
	DISCOUNT,
	AMOUNT_PAID_BY_TRANSACTION,
	TOTAL_SHIPPING_COST,
	SHIPPING_COST_WO_TAX,
	SHIPPING_COST_TAX,
	AMOUNT_REFUNDED,
	CREDIT_APPLIED,
	TOTAL_AMOUNT_PAID,
	COGS,
	TOTAL_AMOUNT_PAID_LESS_COGS,
	PRODUCT_MARGIN_PERC
) as 

/**** First build the orders data ****/
with orders as (

select 
  to_date(sku_adj.invoice_date) as order_date 
, date_trunc('month',to_date(sku_adj.invoice_date)) as order_date_month 
, o.subscription_id
, o.customer_id
, o.sku
, case when o.sku ilike '%syn%' or sku ilike 'ds01%' then 'DS-01'
       when o.sku ilike '%pds%' then 'PDS-08'
           else null end as product
-- accounting for Multi SKU
, case when sku ilike '%2mo%' then 2
       when sku ilike '%3mo%' then 3
       when sku ilike '%6mo%' then 6
       else 1 end as factor 
, ifnull(sum(o.quantity),0) as quantity
, ifnull(sum(o.base_price)/factor,0) as base_price
, ifnull(sum(o.total_amount_paid)/factor,0) as total_amount_paid
, ifnull(sum(o.tax)/factor,0) as tax
, ifnull(sum(o.discount)/factor,0) as discount
, ifnull(sum(o.amount_paid_by_transaction)/factor,0) as amount_paid_by_transaction
, ifnull(sum(o.total_shipping_cost)/factor,0) as total_shipping_cost
, ifnull(sum(o.shipping_cost_wo_tax)/factor,0) as shipping_cost_wo_tax
, ifnull(sum(o.shipping_cost_tax)/factor,0) as shipping_cost_tax
, ifnull(sum(o.amount_refunded)/factor,0) as amount_refunded
, ifnull(sum(o.credit_applied)/factor,0) as credit_applied

from SEED_DATA.DEV.ORDER_HISTORY as o 
    join SEED_DATA.DEV.V_ORDER_HISTORY_SKU_ADJUSTED as sku_adj on o.invoice_id = sku_adj.invoice_id and o.subscription_id = sku_adj.subscription_id -- Adding SKU adjusted table
where 
to_date(sku_adj.INVOICE_DATE) <= to_date(current_date()-1)
group by 1,2,3,4,5,6)



, orders_filter as (
select *
from orders
where amount_refunded < (total_amount_paid+total_shipping_cost) or amount_refunded = 0 -- filtering out for fully refunded invoices
) 



/**** Aggregare Orders Data ****/
, orders_agg as (
select 
  o.order_date_month
, o.product
, sum(o.quantity) as quantity
from orders_filter as o
group by 1,2
)


/**** Aggregare Orders Data Monhthly (excluding Product) ****/
, orders_agg_month as (
select 
  oa.order_date_month
, sum(oa.quantity) as quantity
from orders_agg as oa
group by 1
)

/**** Use above query to get perc of total quantity sold by product ****/
, orders_perc_total as (
select 
  oa.order_date_month
, oa.product
, sum(oa.quantity) as quantity
, DIV0NULL(sum(oa.quantity),sum(oam.quantity)) as perc_total_qty
from orders_agg as oa
left join orders_agg_month as oam
on oa.order_date_month = oam.order_date_month
where oa.product in ('DS-01','PDS-08')
group by 1,2
)

/**** Use above query to get perc of total quantity sold by product ****/
, orders_perc_total_agg as (
select 
  opt.order_date_month
, ifnull(max(case when opt.product = 'DS-01' then opt.perc_total_qty end ),0) as perc_total_qty_ds01
, ifnull(max(case when opt.product = 'DS-01' then 1-opt.perc_total_qty end ),0) as perc_total_qty_pds08
from orders_perc_total as opt
group by 1
)

/**** Bring in the COGS data ****/
, cogs as (
select 
  month_year
, case when account_no_name_org ilike '%DS-01%' THEN 'DS-01' 
     when account_no_name_org ilike '%PDS-08%' THEN 'PDS-08' 
     else null end as product
, sum(value) as cogs
from 
SEED_DATA.DEV.V_PROFIT_LOSS_MONTHLY
where ACCOUNT_GROUP in ('Product COGS','Non-Reoccurring COGS','Selling Expenses')
group by 1,2
)

/**** Model COGS data for non-product / product data ****/
, cogs_modeled as (
select
  month_year
, product 
, ifnull(sum(cogs),0) as cogs
from (
select
  c.month_year
, c.product
, ifnull(sum(c.cogs),0) as cogs
from cogs as c
where c.product in ('DS-01', 'PDS-08')
group by 1,2

union all

select
  c.month_year
, 'DS-01' as product
, sum(c.cogs)*max(opta.perc_total_qty_ds01) as cogs
from cogs as c
left join orders_perc_total_agg as opta
on c.month_year = opta.order_date_month
where c.product not in ('DS-01', 'PDS-08')
group by 1,2

union all 

select
  c.month_year
, 'PDS-08' as product
, sum(c.cogs)*max(opta.perc_total_qty_pds08) as cogs
from cogs as c
left join orders_perc_total_agg as opta
on c.month_year = opta.order_date_month
where c.product not in ('DS-01', 'PDS-08')
group by 1,2
) group by 1,2
)

/**** Aggregare COGS Data, join aggregate quantity data, and build COGS per Qty sold ****/
, cogs_agg as (
select 
  oa.order_date_month
, oa.product
, sum(oa.quantity) as quantity
, ifnull(sum(c.cogs),0) as cogs
, DIV0NULL(sum(c.cogs),sum(oa.quantity)) as total_cogs_per_qty
, ifnull(total_cogs_per_qty,avg(total_cogs_per_qty) over(partition by oa.product order by oa.order_date_month rows between 12 preceding and current row)) as total_cogs_per_qty_extra -- Extrapolating for missing data using 12 months moving average
from orders_agg as oa
left join cogs_modeled as c
on oa.order_date_month = c.month_year
and oa.product = c.product
where oa.product in ('DS-01', 'PDS-08')
group by 1,2
)


/**** Bring back data back at the order history / subscriber level and calculate COGS ****/
, orders_cogs_build as (
select 
  o.order_date_month
, o.order_date
, o.product
, o.subscription_id
, o.customer_id
, o.sku
, ifnull(sum(o.quantity),0) as quantity
, ifnull(sum(base_price),0) as base_price
, ifnull(sum(total_amount_paid),0) as total_amount_paid
, ifnull(sum(tax),0) as tax
, ifnull(sum(discount),0) as discount
, ifnull(sum(amount_paid_by_transaction),0) as amount_paid_by_transaction
, ifnull(sum(total_shipping_cost),0) as total_shipping_cost
, ifnull(sum(shipping_cost_wo_tax),0) as shipping_cost_wo_tax
, ifnull(sum(shipping_cost_tax),0) as shipping_cost_tax
, ifnull(sum(amount_refunded),0) as amount_refunded
, ifnull(sum(credit_applied),0) as credit_applied
, max(ca.total_cogs_per_qty_extra) * ifnull(sum(o.quantity),0) as cogs
, ifnull(sum(total_amount_paid),0) - (max(ca.total_cogs_per_qty_extra) * ifnull(sum(o.quantity),0)) as total_amount_paid_less_cogs


from
orders as o
left join cogs_agg as ca
on o.order_date_month = ca.order_date_month
and o.product = ca.product
group by 1,2,3,4,5,6
)



/**** Complete build and calculate LTV ****/
select 
  ocb.order_date
, ocb.order_date_month
, ocb.product
, ocb.subscription_id
, ocb.customer_id
, ocb.sku
, case when amount_refunded < (total_amount_paid+total_shipping_cost) or amount_refunded = 0 then 'not_fully_refunded' else 'fully_refunded' end as invoice_refund_flag
, ifnull(sum(ocb.quantity),0) as quantity
, ifnull(sum(base_price),0) as base_price
, ifnull(sum(tax),0) as tax
, ifnull(sum(discount),0) as discount
, ifnull(sum(amount_paid_by_transaction),0) as amount_paid_by_transaction
, ifnull(sum(total_shipping_cost),0) as total_shipping_cost
, ifnull(sum(shipping_cost_wo_tax),0) as shipping_cost_wo_tax
, ifnull(sum(shipping_cost_tax),0) as shipping_cost_tax
, ifnull(sum(amount_refunded),0) as amount_refunded
, ifnull(sum(credit_applied),0) as credit_applied
, ifnull(sum(total_amount_paid),0) as total_amount_paid
, ifnull(sum(ocb.cogs),0) as cogs
, ifnull(sum(ocb.total_amount_paid_less_cogs),0) as total_amount_paid_less_cogs ---- LTV
, DIV0NULL(sum(ocb.total_amount_paid_less_cogs),sum(ocb.total_amount_paid)) as product_margin_perc

from 
orders_cogs_build as ocb
--where ocb.order_date_month between '2023-06-01' and '2023-07-01'

group by 1,2,3,4,5,6,7
order by 1 desc;