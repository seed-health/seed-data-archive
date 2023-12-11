create or replace view MARKETING_DATABASE.PUBLIC.PRE_ORDERS_APRIL_2021 as

select first_adjustment_product_code as first_sku,
s.*
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s
join "MARKETING_DATABASE"."PUBLIC"."d_first_recurly_adjustment" as a 
on s.uuid = a.subscription_id
where s.created_at > '2021-03-26'