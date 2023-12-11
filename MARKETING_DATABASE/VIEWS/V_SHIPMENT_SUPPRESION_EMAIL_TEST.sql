create or replace view MARKETING_DATABASE.GOOGLE_SHEETS.V_SHIPMENT_SUPPRESION_EMAIL_TEST(
	EMAIL,
    SUPPRESED_FLAG,
    CREATED_AT,
    CANCELLED_AT,
    SKU,
    BILLING_COUNTRY,
    PRODUCT
) as

with ds01 as
(
select lower(customer_email) as email_clean,created_at,cancelled_at,sku,billing_country, 'DS01' as product
from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_DS_01"
where cancelled_at is null or cancelled_at >= '2022-12-23'
),

pds08 as 
(select lower(customer_email) as email_clean,created_at,cancelled_at,sku,billing_country, 'PDS08' as product
from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_PDS_08"
where cancelled_at is null or cancelled_at >= '2022-12-23'),

all_subs as 
(
select * from ds01
UNION ALL
select * from pds08
)

select email,suppressed_flag, created_at, cancelled_at, SKU, Billing_country,product
from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPMENT_EMAIL_TEST" as t left join all_subs
on t.email = all_subs.email_clean