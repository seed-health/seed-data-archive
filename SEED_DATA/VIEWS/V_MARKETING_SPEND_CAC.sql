create or replace view SEED_DATA.DEV.V_MARKETING_SPEND_CAC(
	MONTH_YEAR,
	PRODUCT,
	TOTAL_SPEND
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
    join SEED_DATA.DEV.V_ORDER_HISTORY_SKU_ADJUSTED as sku_adj on o.invoice_id = sku_adj.invoice_id -- Adding SKU adjusted table
where 
to_date(sku_adj.INVOICE_DATE) <= to_date(current_date()-1)
group by 1,2,3,4,5,6 )


/**** Aggregare Orders Data ****/
, orders_agg as (
select 
  o.order_date_month
, o.product
, sum(o.quantity) as quantity
from orders as o
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
/**** Bring in the CAC data ****/
, cac as (
select 
  month_year,
  account_group,
  sum(value) as total_spend_cac
from 
SEED_DATA.DEV.V_PROFIT_LOSS_MONTHLY
where ACCOUNT_GROUP in ('Acquisition Marketing','Paid Partnerships','Brand','Creative, Design + Production')
group by 1,2
)

,paid_acq_ratio as 
(
select month as month_year,DS_01_SPEND_PERC,PDS_08_SPEND_PERC
from MARKETING_DATABASE.GOOGLE_SHEETS.CAC_SPEND
)

,cac_modeled as (

select month_year,
        product,
        sum(total_spend) as total_spend
from 
(
--- Before PDS Launch
select
  c.month_year
, 'DS-01' as product
, sum(c.total_spend_cac) as total_spend
from cac as c
where c.month_year < '2022-04-01' 
group by 1,2

union all 

--- Post PDS Launch
select
  c.month_year
, 'DS-01' as product
, sum(c.total_spend_cac)*max(opta.perc_total_qty_ds01) as total_spend
from cac as c
left join orders_perc_total_agg as opta
on c.month_year = opta.order_date_month
where account_group in ('Brand','Creative, Design + Production') and c.month_year >= '2022-04-01'
group by 1,2

union all 

select
  c.month_year
, 'PDS-08' as product
, sum(c.total_spend_cac)*max(opta.perc_total_qty_pds08) as total_spend
from cac as c
left join orders_perc_total_agg as opta
on c.month_year = opta.order_date_month
where account_group in ('Brand','Creative, Design + Production') and c.month_year >= '2022-04-01'
group by 1,2

union all 

select
  c.month_year
, 'DS-01' as product
, sum(c.total_spend_cac)*max(par.DS_01_SPEND_PERC) as total_spend
from cac as c
left join paid_acq_ratio as par
on c.month_year = par.month_year
where account_group in ('Acquisition Marketing','Paid Partnerships') and c.month_year >= '2022-04-01'
group by 1,2

union all 

select
  c.month_year
, 'PDS-08' as product
, sum(c.total_spend_cac)*max(par.PDS_08_SPEND_PERC) as total_spend
from cac as c
left join paid_acq_ratio as par
on c.month_year = par.month_year
where account_group in ('Acquisition Marketing','Paid Partnerships') and c.month_year >= '2022-04-01'
group by 1,2
) 
group by 1,2
)

select * from cac_modeled
order by 1,2;