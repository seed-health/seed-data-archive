create or replace view MARKETING_DATABASE.PUBLIC.ORDER_REPORT_1_0
as

    select quantities.day,
          zeroifnull("'Daily Synbiotic'") as WK,
          dgt_gains.dgt_gain,
          zeroifnull("'Daily Synbiotic—Refill'") as RF,
          zeroifnull("'Daily Synbiotic—Refill (3 month)'") as three_mo,
          zeroifnull("'Daily Synbiotic—Refill (2 month)'") as two_mo,
          zeroifnull("'(Replacement) Vial)'") as replacement_vial,
          zeroifnull("'(Replacement) Jar + Vial'") as replacement_jar_and_vial,
          zeroifnull("'(Replacement) Daily Synbiotic'") as replacement_wk,
          zeroifnull("'(Replacement) Jar'") as replacement_jar,
          zeroifnull("'(Replacement) Daily Synbiotic—Refill'") as replacement_rf,
          zeroifnull("'(Replacement) Jar Lid'") as replacement_jar_lid,
          wk_refunds.wk_refund_amount,
          wk_refunds.wk_refund_quantity,
          rf_refunds.rf_refund_amount,
          rf_refunds.rf_refund_quantity
    from
    (

        select *
        from
        (
            select to_date(t.date) as day, 
                  sum(adjustment_quantity) as quantity,
                  adjustment_description as sku
            from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
            join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
            where t.type = 'purchase'
            and t.status = 'success'
            and sku not ilike '%shipping%'
            group by day, sku
        ) as new_entry

            pivot (
                sum(quantity) for sku in 
                    ('Daily Synbiotic',
                    'Daily Synbiotic—Refill', 
                    'Daily Synbiotic—Refill (3 month)', 
                    'Daily Synbiotic—Refill (2 month)',
                    '(Replacement) Vial)',
                    '(Replacement) Jar + Vial',
                    '(Replacement) Daily Synbiotic',
                    '(Replacement) Jar',
                    '(Replacement) Daily Synbiotic—Refill',
                    '(Replacement) Jar Lid')
               )  order by day


    ) as quantities
    right join
    (
        select to_date(map.created_at) as day, 
            sum(quantity) as dgt_gain
        from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING" as map
        where map.created_at is not null
        group by day
    ) as dgt_gains on dgt_gains.day = quantities.day
    left join -- just in case there are days with no refunds
    (
       select to_date(t.date) as day, 
           sum(amount) as wk_refund_amount, 
           sum(adjustment_quantity) as wk_refund_quantity
       from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
       join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
       where t.type = 'refund'
       and t.status = 'success'
       and adjustment_description not ilike '%shipping%'
       and adjustment_description  = 'Daily Synbiotic'
       group by day, adjustment_description
    ) as wk_refunds on dgt_gains.day = wk_refunds.day
    left join
    (
         select to_date(t.date) as day, 
             sum(amount) as rf_refund_amount, 
             sum(adjustment_quantity) as rf_refund_quantity
         from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
         join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
         where t.type = 'refund'
         and t.status = 'success'
         and adjustment_description not ilike '%shipping%'
         and adjustment_description  = 'Daily Synbiotic—Refill'
         group by day, adjustment_description
    ) as rf_refunds on dgt_gains.day = rf_refunds.day
    where dgt_gains.day > '2021-01-31'
    order by dgt_gains.day;