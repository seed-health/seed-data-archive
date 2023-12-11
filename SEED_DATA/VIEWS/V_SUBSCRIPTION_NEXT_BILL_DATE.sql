create or replace view SEED_DATA.DEV.V_SUBSCRIPTION_NEXT_BILL_DATE as 

select 
  uuid as subscription_id
, current_term_ends_at as next_bill_date_ts
, to_date(current_term_ends_at) as next_bill_date
from IO06230_RECURLY_SEED_SHARE.CLASSIC.SUBSCRIPTIONS
where state = 'active' or state = 'paused'

;