create view recurly_customer_view as 

(
select 
a.Subscription_ID as pk_Subscription_ID,
ac.account_first_name as first_name,
ac.account_last_name as last_name,
c.email as email,
c.state as active,
max(c.created_at) as start_time, --(created_at),
max(c.activated_at) as activated_at,
max(c.canceled_at) as end_time, --(cancelled_at),
case 
  when c.state = 'active' then datediff(day, c.activated_at, current_date()) 
  when c.state = 'canceled' then datediff(day, c.activated_at, c.canceled_at) 
end as Total_subscription_length, -- (length)
c.plan_name as product_name,
a.item_code as product_id,
sum(a.adjustment_quantity) as quantity,
sum(c.quantity) as TOTAL_shipped_quantity,
sum(a.adjustment_total /*follow up with ameeqa*/ ) as Total_subscription_value,
sum(a.adjustment_discount) as discounts_applied,
ac.account_phone as phone,
ac.account_address1 as address,
ac.account_city as city,
ac.account_state as state,
ac.account_country as country,
ac.account_postal_code as zipcode

from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a
    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as b on b.id = a.invoice_id
    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as c on c.ACCOUNT_CODE = a.ACCOUNT_CODE
    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ACCOUNTS" as ac on c.account_code = ac.account_code
    where b.status in ('paid')
    and b.invoice_doc_type in ('charge')
    group by pk_Subscription_ID, 

    ac.account_phone,ac.account_address1,ac.account_city,ac.account_state,ac.account_country,ac.account_postal_code,
    ac.account_first_name, ac.account_last_name,c.email, c.state, c.plan_name, a.item_code, c.activated_at, c.canceled_at
	order by Subscription_ID
  
  )