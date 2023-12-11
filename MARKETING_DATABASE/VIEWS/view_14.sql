create or replace view MARKETING_DATABASE.PUBLIC.PRE_ORDERS_JUNE_2021 as


select b.date, a.*, b.invoice_number

from (
select first_adjustment_product_code as first_sku,
s.email, s.uuid
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s
join "MARKETING_DATABASE"."PUBLIC"."d_first_recurly_adjustment" as a 
on s.uuid = a.subscription_id
where s.created_at > '2021-03-26') as a left join

(
select a.subscription_id as subscription_id,a.invoice_number as invoice_number, to_date(a.adjustment_created_at) as date
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a
join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as i on i.id = a.invoice_id
where i.status in ('paid')
and i.invoice_doc_type in ('charge') 
and adjustment_description not ilike '%shipping%') as b on a.uuid = b.subscription_id

order by uuid;