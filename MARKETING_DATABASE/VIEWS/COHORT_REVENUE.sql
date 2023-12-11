create or replace  view marketing_database.dbt_production.cohort_revenue
  
   as (
    with all_revenue as
(
    select RECHARGE_SUBSCRIPTION_ID,
            RECURLY_SUBSCRIPTION_ID,
  
            case when RECHARGE_SUBSCRIPTION_ID is null then
                RECURLY_SUBSCRIPTION_ID
            else
                RECHARGE_SUBSCRIPTION_ID
            end as _subscription_id,

            min(charged_date) over (partition by _subscription_id) as first_charged_at,
            charged_date as charged_at,
            charged_amount,

            is_recharge_native,
            is_imported,
            is_recurly_native

    from
    (

        select *
        from marketing_database.dbt_production.subscription_mapping as cohort
        join 
        (
            select to_varchar(s.uuid) as subscription_id,
                    t.date as charged_date,
                    (ifnull(t.amount,0) - ifnull(a.adjustment_tax,0) - ifnull(a.adjustment_discount,0)) as charged_amount
            from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a
            join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s on s.uuid = a.subscription_id
            join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" i on a.invoice_id = i.id
            join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" t on t.invoice_id = i.id
            where a.invoice_type in ('purchase', 'renewal')
            and i.status in ('closed', 'paid')
            and t.type = 'purchase' and t.status = 'success' and t.test = 'FALSE'
            and a.adjustment_product_code != 'free_shipping'
            and ifnull(charged_amount,0) > 0
        ) as recurly on lower(to_varchar(recurly.subscription_id)) = lower(to_varchar(cohort.recurly_subscription_id))

        union all

        select *
        from marketing_database.dbt_production.subscription_mapping as cohort
        join
        (
            select to_varchar(s.id) as subscription_id, 
                    c.processed_at as charged_date, 
                    (ifnull(c.total_price,0.0) - ifnull(c.total_tax,0.0) - ifnull(csl.price,0.0)) as charged_amount
            from "MARKETING_DATABASE"."RECHARGE"."CHARGE" as c
            join "MARKETING_DATABASE"."RECHARGE"."CHARGE_LINE_ITEM" as cli on c.id = cli.charge_id
            left join "MARKETING_DATABASE"."RECHARGE"."CHARGE_SHIPPING_LINE" csl on csl.charge_id = c.id
            join "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" s on cli.subscription_id = s.id
            where c.status = 'SUCCESS'
            and ifnull(charged_amount,0) > 0
        ) as recharge on lower(to_varchar(recharge.subscription_id)) = lower(to_varchar(cohort.recharge_subscription_id))

        order by charged_date

    ) as a

)

-- fix names
select cohort_id, "'2018-06'" as "2018-06", "'2018-07'" as "2018-07", "'2018-08'" as "2018-08", "'2018-09'" as "2018-09", "'2018-10'" as "2018-10", "'2018-11'" as "2018-11", "'2018-12'" as "2018-12", "'2019-01'" as "2019-01", "'2019-02'" as "2019-02", "'2019-03'" as "2019-03", "'2019-04'" as "2019-04", "'2019-05'" as "2019-05", "'2019-06'" as "2019-06", "'2019-07'" as "2019-07", "'2019-08'" as "2019-08", "'2019-09'" as "2019-09", "'2019-10'" as "2019-10", "'2019-11'" as "2019-11", "'2019-12'" as "2019-12", "'2020-01'" as "2020-01", "'2020-02'" as "2020-02", "'2020-03'" as "2020-03", "'2020-04'" as "2020-04", "'2020-05'" as "2020-05", "'2020-06'" as "2020-06", "'2020-07'" as "2020-07", "'2020-08'" as "2020-08", "'2020-09'" as "2020-09", "'2020-10'" as "2020-10", "'2020-11'" as "2020-11", "'2020-12'" as "2020-12", "'2021-01'" as "2021-01", "'2021-02'" as "2021-02", "'2021-03'" as "2021-03", "'2021-04'" as "2021-04", "'2021-05'" as "2021-05", "'2021-06'" as "2021-06","'2021-07'" as "2021-07", "'2021-08'" as "2021-08", "'2021-09'" as "2021-09","'2021-10'" as "2021-10", "'2021-11'" as "2021-11", "'2021-12'" as "2021-12", "'2022-01'" as "2022-01", "'2022-02'" as "2022-02","'2022-03'" as "2022-03"
from
(

    select *
    from 
    (
        select left(first_charged_at, 7) as cohort_id, charged_amount, left(charged_at, 7) as charged_at_month
        from all_revenue
    ) as all_revenue_agg 
                    pivot (
                        sum(charged_amount) for charged_at_month in 
                        ('2018-06','2018-07','2018-08','2018-09','2018-10','2018-11','2018-12','2019-01','2019-02','2019-03','2019-04','2019-05','2019-06','2019-07','2019-08','2019-09','2019-10','2019-11','2019-12','2020-01','2020-02','2020-03','2020-04','2020-05','2020-06','2020-07','2020-08','2020-09','2020-10','2020-11','2020-12','2021-01','2021-02','2021-03','2021-04','2021-05','2021-06','2021-07','2021-08','2021-09','2021-10','2021-11','2021-12','2022-01','2022-02','2022-03')      
                    )
    order by cohort_id
)
  );