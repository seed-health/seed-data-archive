create or replace view MARKETING_DATABASE.PUBLIC.V_ACCOUNT_DAILY_RETENTION_COUPON_V_01(
	CUSTOMER_EMAIL,
	DS01_ACTIVE_FLAG,
	DS_01_START_DATE,
	DS01_ACTIVE_DATE,
	DS01_QUANTITY,
	PDS08_ACTIVE_FLAG,
	PDS_08_START_DATE,
	PDS08_ACTIVE_DATE,
	PDS08_QUANTITY,
	ACCOUNT_ACTIVE_FLAG,
	ACCOUNT_START_DATE,
	ACCOUNT_ACTIVE_DATE,
	ACCOUNT_QUANTITY,
    COUPON_NAME
) as

with coupon_redemption as 
(select *, row_number() over (PARTITION BY account_email order by applied_at desc) as rn
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."COUPON_REDEMPTIONS" as cr 
left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."COUPONS" as c on cr.coupon_id = c.id
left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ACCOUNTS" as a on cr.account_code = a.account_code
),

account_coupon as 
(
select account_email, name as coupon_name
from coupon_redemption
where rn = 1
  )
  
  select ar.*,ac.coupon_name as coupon_name from "MARKETING_DATABASE"."PUBLIC"."V_ACCOUNT_DAILY_RETENTION_V_01" as ar 
  left join account_coupon as ac on ar.customer_email = ac.account_email