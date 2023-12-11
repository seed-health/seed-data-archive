create or replace view MARKETING_DATABASE.PUBLIC.FILTERVIEW_TWO as 


select
  e.plan_name as plan_name, 
  e.email as email, 
  e.subscription_id as subscription_id, 
  e.subscription_quantity as subscription_quantity,
  e.credits_used as credits_used,
  e.account_code as account_code,
  e.cx_conversations,
  e.first_adjustment_discount as first_adjustment_discount,
  f.campaign_names as campaign_names

from (
select 

  c.plan_name as plan_name, 
  c.email as email, 
  c.subscription_id as subscription_id, 
  c.subscription_quantity as subscription_quantity,
  c.credits_used as credits_used,
  c.account_code as account_code,
  c.cx_conversations,
  d.first_adjustment_discount as first_adjustment_discount


from (

    select 
    a.plan_name as plan_name, 
    a.email as email, 
    a.subscription_id as subscription_id, 
    a.subscription_quantity as subscription_quantity,
    zeroifnull(a.credits_used) as credits_used,
    a.account_code as account_code,
    b.cx_conversations as cx_conversations

  from (

    select 
    plan_name as plan_name, 
    sub_plan.email as email, 
    sub_plan.uuid as subscription_id,
    sub_plan.account_code,
    quantity as subscription_quantity,
    credits.credits_used as credits_used
  //  zeroifnull(cx_activity.cx_conversations) as cx_conversations,
  //  zeroifnull(first_discount.first_adjustment_discount) as discounts_at_signup,
  //  campaigns.campaign_names as campaign_names
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as sub_plan
     left join (
     select account_code, count(adjustment_description) as credits_used
      from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
      where adjustment_description ilike '%credit%'
      group by account_code
    ) as credits on credits.account_code = sub_plan.account_code

  ) as a left join (


select c.CONVERSATION_COUNTS_ALL as cx_conversations,
  e.email as email
  from "MARKETING_DATABASE"."KUSTOMER"."CUSTOMER" as c
  join "MARKETING_DATABASE"."KUSTOMER"."CUSTOMER_EMAIL" as e on c.id = e.customer_id



) as b on a. email = b.email
  
  
) as c left join (

  select 
   adj_first.adjustment_created_at as adjustment_created_at,
   adj_first.subscription_id as subscription_id,
   details.adjustment_discount as first_adjustment_discount
   from (
   select min(to_date(adjustment_created_at)) as adjustment_created_at, 
   subscription_id
      from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
      where adjustment_description not ilike 'Shipping%' 
      and adjustment_type = 'charge' 
      and subscription_id is not null
      group by subscription_id
    ) as adj_first join (
    select to_date(adjustment_created_at) as adjustment_created_at,
      adjustment_discount, 
      subscription_id
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
    where adjustment_description not ilike '%shipping%'
    and adjustment_discount > 0
    ) as details on adj_first.subscription_id = details.subscription_id
    and adj_first.adjustment_created_at = details.adjustment_created_at
    order by subscription_id desc  
  
) as d on c.subscription_id = d.subscription_id

  ) as e left join (

  select lists.email as email, listagg(lists.campaign_name, ', ') as campaign_names
from (
  select p.email as email, c.name as campaign_name
  from "MARKETING_DATABASE"."KLAVIYO"."CAMPAIGN" as c
  left join "MARKETING_DATABASE"."KLAVIYO"."EVENT" as e on c.id = e.campaign_id
  left join "MARKETING_DATABASE"."KLAVIYO"."PERSON" as p on e.person_id = p.id
  where e.type = 'Received Email'
  order by p.email
) as lists group by lists.email
  
  
  
  ) as f on e.email = f.email;