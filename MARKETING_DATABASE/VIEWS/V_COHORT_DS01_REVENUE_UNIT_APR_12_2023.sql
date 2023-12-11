create or replace view MARKETING_DATABASE.PUBLIC.V_COHORT_DS01_REVENUE_UNIT_APR_12_2023(
	COHORT_ID,
	"2018-10",
	"2018-11",
	"2018-12",
	"2019-01",
	"2019-02",
	"2019-03",
	"2019-04",
	"2019-05",
	"2019-06",
	"2019-07",
	"2019-08",
	"2019-09",
	"2019-10",
	"2019-11",
	"2019-12",
	"2020-01",
	"2020-02",
	"2020-03",
	"2020-04",
	"2020-05",
	"2020-06",
	"2020-07",
	"2020-08",
	"2020-09",
	"2020-10",
	"2020-11",
	"2020-12",
	"2021-01",
	"2021-02",
	"2021-03",
	"2021-04",
	"2021-05",
	"2021-06",
	"2021-07",
	"2021-08",
	"2021-09",
	"2021-10",
	"2021-11",
	"2021-12",
	"2022-01",
	"2022-02",
	"2022-03",
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

with ds01_revenue as 
(
   select RECHARGE_SUBSCRIPTION_ID,
          RECURLY_SUBSCRIPTION_ID,
  
          case when RECHARGE_SUBSCRIPTION_ID is null then RECURLY_SUBSCRIPTION_ID
            else RECHARGE_SUBSCRIPTION_ID
            end as _subscription_id,

          min(charged_date) over (partition by _subscription_id) as first_charged_at,
          charged_date as charged_at,
          charged_unit,

          is_recharge_native,
          is_imported,
          is_recurly_native

    from
    (

        select *
        from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_DS_01" as cohort
        join 
        (
            select to_varchar(s.uuid) as subscription_id,
                   t.date as charged_date,
                   case when adjustment_plan_code ilike '%3mo%' then ifnull(a.adjustment_quantity,0)*3 
                        when adjustment_plan_code ilike '%6mo%' then ifnull(a.adjustment_quantity,0)*6
                        else (ifnull(a.adjustment_quantity,0)) end as charged_unit
            from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
              join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
              left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s on t.subscription_id = s.uuid
            where t.type = 'purchase'
              and t.status = 'success'
              and adjustment_description not ilike '%shipping%'
              and (s.plan_name ilike '%DS-01%' or s.plan_name ilike 'Daily Synbiotic%')
              and adjustment_plan_code not ilike 'pds%'
 
        ) as recurly on lower(to_varchar(recurly.subscription_id)) = lower(to_varchar(cohort.recurly_subscription_id))

        union all

        select *
        from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_DS_01" as cohort
        -- left join 
        join
        (
            
             select to_varchar(s.id) as subscription_id, 
                   to_date(dateadd(hour, -4, c.processed_at)) as charged_date, 
                   case when cli.sku ilike '%3MO%' then ifnull(cli.quantity,0)*3
                        else ifnull(cli.quantity,0) end as charged_unit
             from "MARKETING_DATABASE"."RECHARGE"."CHARGE" as c
                join "MARKETING_DATABASE"."RECHARGE"."CHARGE_LINE_ITEM" as cli on cli.charge_id = c.id
                join "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" s on cli.subscription_id = s.id
             
        ) as recharge on lower(to_varchar(recharge.subscription_id)) = lower(to_varchar(cohort.recharge_subscription_id))

        order by charged_date

    ) as a
  
  )
  
  -- fix names
select cohort_id, "'2018-10'" as "2018-10", "'2018-11'" as "2018-11", "'2018-12'" as "2018-12", "'2019-01'" as "2019-01", "'2019-02'" as "2019-02", "'2019-03'" as "2019-03", "'2019-04'" as "2019-04", "'2019-05'" as "2019-05", "'2019-06'" as "2019-06", "'2019-07'" as "2019-07", "'2019-08'" as "2019-08", "'2019-09'" as "2019-09", "'2019-10'" as "2019-10", "'2019-11'" as "2019-11", "'2019-12'" as "2019-12", "'2020-01'" as "2020-01", "'2020-02'" as "2020-02", "'2020-03'" as "2020-03", "'2020-04'" as "2020-04", "'2020-05'" as "2020-05", "'2020-06'" as "2020-06", "'2020-07'" as "2020-07", "'2020-08'" as "2020-08", "'2020-09'" as "2020-09", "'2020-10'" as "2020-10", "'2020-11'" as "2020-11", "'2020-12'" as "2020-12",
"'2021-01'" as "2021-01", "'2021-02'" as "2021-02", "'2021-03'" as "2021-03", "'2021-04'" as "2021-04", "'2021-05'" as "2021-05", "'2021-06'" as "2021-06","'2021-07'" as "2021-07", "'2021-08'" as "2021-08", "'2021-09'" as "2021-09","'2021-10'" as "2021-10", "'2021-11'" as "2021-11", "'2021-12'" as "2021-12", "'2022-01'" as "2022-01", "'2022-02'" as "2022-02","'2022-03'" as "2022-03", "'2022-04'" as "2022-04","'2022-05'" as "2022-05","'2022-06'" as "2022-06", "'2022-07'" as "2022-07", "'2022-08'" as "2022-08", "'2022-09'" as "2022-09", "'2022-10'" as "2022-10", "'2022-11'" as "2022-11", "'2022-12'" as "2022-12", "'2023-01'" as "2023-01", "'2023-02'" as "2023-02", "'2023-03'" as "2023-03", "'2023-04'" as "2023-04","'2023-05'" as "2023-05"
from
(
  select *
    from 
    (
        select left(first_charged_at, 7) as cohort_id, charged_unit, left(charged_at, 7) as charged_at_month
        from ds01_revenue
        where cohort_id is not null
    ) as ds01_revenue_agg 
                    pivot (
                        sum(charged_unit) for charged_at_month in 
                        ('2018-10','2018-11','2018-12','2019-01','2019-02','2019-03','2019-04','2019-05','2019-06','2019-07','2019-08','2019-09','2019-10','2019-11','2019-12','2020-01','2020-02','2020-03','2020-04','2020-05','2020-06','2020-07','2020-08','2020-09','2020-10','2020-11','2020-12','2021-01','2021-02','2021-03','2021-04','2021-05','2021-06','2021-07','2021-08','2021-09','2021-10','2021-11','2021-12','2022-01','2022-02','2022-03','2022-04','2022-05','2022-06','2022-07','2022-08','2022-09','2022-10','2022-11','2022-12','2023-01','2023-02','2023-03','2023-04','2023-05')      
                    )
    order by cohort_id
);