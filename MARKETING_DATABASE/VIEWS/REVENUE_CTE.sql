create or replace view marketing_database.public.revenue_cte as 
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
        from "MARKETING_DATABASE"."PUBLIC"."V_MERGEDRECHARGEANDRECURLYSUBSCRIPTIONS_FORCOHORTS_V2.1" as cohort
        -- left join
        join 
        (
            select to_varchar(s.uuid) as subscription_id,
                    -- a.adjustment_start_at as charged_date,
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
        from "MARKETING_DATABASE"."PUBLIC"."V_MERGEDRECHARGEANDRECURLYSUBSCRIPTIONS_FORCOHORTS_V2.1" as cohort
        -- left join 
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