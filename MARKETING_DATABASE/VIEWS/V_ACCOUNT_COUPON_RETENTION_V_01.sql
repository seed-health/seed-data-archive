create or replace view MARKETING_DATABASE.PUBLIC.V_ACCOUNT_COUPON_RETENTION_V_01(
    COUPON_NAME,
	CYCLE,
	ACCOUNT_COUNT,
	ACCOUNT_QUANTITY,
	DS01_QUANTITY,
	PDS08_QUANTITY,
	FLAG
) as

with coupon_redemption as 
(select *, row_number() over (PARTITION BY account_email order by applied_at desc) as rn
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."COUPON_REDEMPTIONS" as cr 
left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."COUPONS" as c on cr.coupon_id = c.id
left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ACCOUNTS" as a on cr.account_code = a.account_code
),

account_coupon as 
(
select account_email, name, case when name ilike 'sid%' or name ilike '%sheer%' then 'Sheer_ID' else 'Other' end as coupon_name
from coupon_redemption
where rn = 1
  ),
  
 /* select ar.*,ac.coupon_name as coupon_name from "MARKETING_DATABASE"."PUBLIC"."V_ACCOUNT_DAILY_RETENTION_V_01" as ar 
  left join account_coupon as ac on ar.customer_email = ac.account_email */
/*  
  create or replace view MARKETING_DATABASE.PUBLIC.V_ACCOUNT_MONTHLY_RETENTION_V_01(
	COHORT_ID,
	CYCLE,
	ACCOUNT_COUNT,
	ACCOUNT_QUANTITY,
	DS01_QUANTITY,
	PDS08_QUANTITY,
	FLAG
) as
*/

account_monthly_retention as 
(select customer_email,account_active_flag,account_start_date,account_active_date,account_quantity,DS01_quantity,
 pds08_Quantity, datediff(day,account_start_date,account_active_date) as active_day, ac.coupon_name as coupon_name
from "MARKETING_DATABASE"."PUBLIC"."V_ACCOUNT_DAILY_RETENTION_V_01" as ar left join account_coupon as ac on ar.customer_email = ac.account_email
where mod(active_day,30) = 0
order by account_start_date,active_day
),

account_coupon_agg as
(
select coupon_name,Active_day as cycle, 
    count(distinct customer_email) as account_count, sum(account_quantity) as account_quantity_agg,
    sum(ds01_quantity) as ds01_quantity_agg,sum(pds08_quantity) as pds08_quantity_agg,
    row_number() over(partition by coupon_name order by cycle desc) as rn
from account_monthly_retention
group by coupon_name,cycle
order by coupon_name,cycle
)

select coupon_name,cycle,account_count,account_quantity_agg as account_quantity,
    ds01_quantity_agg as ds01_quantity,pds08_quantity_agg as pds08_quantity,
    case when rn = 1 then 'running_cohort' end as flag
from account_coupon_agg;