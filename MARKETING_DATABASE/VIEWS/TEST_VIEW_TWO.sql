create or replace view marketing_database.public.test_view_two as 

select 
recurly.transaction_date,
recurly.subscription_id, 
recurly.total_quantity,
recurly.email,
recurly.adjustment_id,
to_date(testview.first_charged_date) as first_charged,
to_date(testview.cancelled_at) as cancelled,
testview.total_quantity as original_quantity
from (
      select 
      to_date(t.date) as transaction_date,
      ac.account_email as email,
      to_varchar(a.subscription_id) as subscription_id, 
      uuid as adjustment_id,
      a.adjustment_quantity as total_quantity
      from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
      join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
      join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ACCOUNTS" as ac on a.account_code = ac.account_code
      where t.type = 'purchase'
      and t.status = 'success'
      and adjustment_description not ilike '%shipping%'
  ) as recurly join "MARKETING_DATABASE"."PUBLIC"."TEST_VIEW" as testview on testview.recurly_subscription_id = recurly.subscription_id
  order by recurly.subscription_id, transaction_date