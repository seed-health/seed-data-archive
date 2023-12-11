create or replace view marketing_database.public.filterview as 

  select plan_name as plan_type, 
  sub_plan.email as email, 
  sub_plan.uuid as subscription_id, 
  quantity as subscription_quantity,
  credits.credits_used as credits_used, 
  zeroifnull(cx_activity.cx_conversations) as cx_conversations,
  zeroifnull(first_discount.first_adjustment_discount) as discounts_at_signup,
  campaigns.campaign_names as campaign_names
  from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as sub_plan
  left join (
   select account_code, count(adjustment_description) as credits_used
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
    where adjustment_description ilike '%credit%'
    group by account_code
  ) as credits on credits.account_code = sub_plan.account_code
  left join (
  select c.CONVERSATION_COUNTS_ALL as cx_conversations,
  e.email as email
  from "MARKETING_DATABASE"."KUSTOMER"."CUSTOMER" as c
  join "MARKETING_DATABASE"."KUSTOMER"."CUSTOMER_EMAIL" as e on c.id = e.customer_id
) as cx_activity on cx_activity.email = sub_plan.email left join (

   select 
   adj_first.adjustment_created_at as adjustment_created_at,
   adj_first.account_code as account_code,
   details.adjustment_discount as first_adjustment_discount
   from (
   select min(to_date(adjustment_created_at)) as adjustment_created_at, 
   account_code
      from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
      where adjustment_description not ilike 'Shipping%' 
      and adjustment_type = 'charge' 
      and subscription_id is not null
      group by account_code
    ) as adj_first join (
    select to_date(adjustment_created_at) as adjustment_created_at,
      adjustment_discount, 
      account_code
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
    where adjustment_description not ilike '%shipping%'
    and adjustment_discount > 0
    ) as details on adj_first.account_code = details.account_code
    and adj_first.adjustment_created_at = details.adjustment_created_at
    order by account_code desc

) as first_discount on first_discount.account_code = credits.account_code left join (

select lists.email as email, listagg(lists.campaign_name, ', ') as campaign_names
from (
  select p.email as email, c.name as campaign_name
  from "MARKETING_DATABASE"."KLAVIYO"."CAMPAIGN" as c
  left join "MARKETING_DATABASE"."KLAVIYO"."EVENT" as e on c.id = e.campaign_id
  left join "MARKETING_DATABASE"."KLAVIYO"."PERSON" as p on e.person_id = p.id
  where e.type = 'Received Email'
  order by p.email
) as lists group by lists.email


) as campaigns on campaigns.email = cx_activity.email