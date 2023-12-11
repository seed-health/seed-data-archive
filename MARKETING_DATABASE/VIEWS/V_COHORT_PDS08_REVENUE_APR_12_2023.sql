create or replace view MARKETING_DATABASE.PUBLIC.V_COHORT_PDS08_REVENUE_APR_12_2023(
	COHORT_ID,
	"2022-04",
	"2022-05",
	"2022-06",
	"2022-07",
	"2022-08",
	"2022-09",
	"2022-10",
	"2022-11",
	"2022-12",
	"2023-01",
	"2023-02",
	"2023-03",
	"2023-04",
    "2023-05"
) as

with pds08_revenue as 
(
   select RECURLY_SUBSCRIPTION_ID as _subscription_id,
          min(charged_date) over (partition by _subscription_id) as first_charged_at,
          charged_date as charged_at,
          charged_amount
    from
    (

        select *
        from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_PDS_08" as cohort
        join 
        (
            select to_varchar(s.uuid) as subscription_id,
                   t.date as charged_date,
                   (ifnull(t.amount,0) - ifnull(a.adjustment_tax,0)) as charged_amount
            from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
              join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
              left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s on t.subscription_id = s.uuid
            where t.type = 'purchase'
              and t.status = 'success'
              and adjustment_description not ilike '%shipping%'
              and (s.plan_name ilike '%PDS-08%' or s.plan_name ilike 'Pediatric Daily Synbiotic%')
              and adjustment_plan_code not ilike '%syn%'
              
        ) as recurly on lower(to_varchar(recurly.subscription_id)) = lower(to_varchar(cohort.recurly_subscription_id))


    ) as a
  
  )
  
  
  -- fix names
select cohort_id, "'2022-04'" as "2022-04","'2022-05'" as "2022-05","'2022-06'" as "2022-06", "'2022-07'" as "2022-07", "'2022-08'" as "2022-08", "'2022-09'" as "2022-09", "'2022-10'" as "2022-10", "'2022-11'" as "2022-11", "'2022-12'" as "2022-12", 
                "'2023-01'" as "2023-01", "'2023-02'" as "2023-02", "'2023-03'" as "2023-03", "'2023-04'" as "2023-04", "'2023-05'" as "2023-05"
from
(   
    select *
    from 
    (
        select left(first_charged_at, 7) as cohort_id, charged_amount, left(charged_at, 7) as charged_at_month
        from pds08_revenue
        where cohort_id is not null and cohort_id >= '2022-04'
    ) as pds08_revenue_agg 
                    pivot (
                        sum(charged_amount) for charged_at_month in 
                        ('2022-04','2022-05','2022-06','2022-07','2022-08','2022-09','2022-10','2022-11','2022-12','2023-01','2023-02','2023-03','2023-04','2023-05')      
                    )
    order by cohort_id
    
);