create view "MARKETING_DATABASE"."PUBLIC".V_SUBMASTER_DOH

AS

select left(to_date(activated_at),7) as cohort_month_year,first_discount_percentage, count(distinct coalesce(recharge_subscription_id,recurly_subscription_id)) as subscription_count from "SEED_DATA"."DEV"."V_SUBSCRIPTION_MASTER" where first_discount_percentage is not null group by 1,2 order by 1,2 ASC