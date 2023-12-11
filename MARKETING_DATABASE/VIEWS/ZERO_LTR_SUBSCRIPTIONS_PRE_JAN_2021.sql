create or replace  view marketing_database.dbt_production.zero_ltr_subscriptions_pre_jan_2021
  
   as (
    select map.recharge_subscription_id, 
        map.recurly_subscription_id, 
        zeroifnull(LTR_recharge) + zeroifnull(LTR_recurly) as LTR
from marketing_database.dbt_production.subscription_mapping as map
left join (

    select s.id as recharge_subscription_id, sum(c.total_price) as LTR_recharge
    from "MARKETING_DATABASE"."RECHARGE"."CHARGE" as c
    join "MARKETING_DATABASE"."RECHARGE"."CHARGE_LINE_ITEM" as cli on c.id = cli.charge_id
    left join "MARKETING_DATABASE"."RECHARGE"."CHARGE_SHIPPING_LINE" csl on csl.charge_id = c.id
    left join "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" s on s.customer_id = c.customer_id
    where c.status = 'SUCCESS'
    group by recharge_subscription_id
    order by recharge_subscription_id

) as _recharge on map.recharge_subscription_id = _recharge.recharge_subscription_id
left join (

    select a.subscription_id as recurly_subscription_id, sum(a.ADJUSTMENT_TOTAL) as LTR_recurly
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a
    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as i on i.id = a.invoice_id
    where i.status in ('paid')
    and i.invoice_doc_type in ('charge')
    group by subscription_id
    order by subscription_id

) as _recurly on map.recurly_subscription_id = _recurly.recurly_subscription_id
where LTR <= 0
-- TIM & SURAJ - Only return Zero LTR Subscriptions before Oct 2020
and map.created_at < '2021-01-01'
-- and _recharge.recharge_subscription_id is null AND _recurly.recurly_subscription_id is null
order by map.recharge_subscription_id, map.recurly_subscription_id
  );