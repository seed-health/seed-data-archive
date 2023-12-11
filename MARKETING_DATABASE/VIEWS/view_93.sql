create or replace view MARKETING_DATABASE.PUBLIC.OPS_DAILY_QUANTS
as

select 
quantities.day,
zeroifnull("'Daily Synbiotic'") as WK,
zeroifnull("'Daily Synbiotic—Refill'") as RF,
zeroifnull("'Daily Synbiotic—Refill (3 month)'") as three_mo,
zeroifnull("'Daily Synbiotic—Refill (2 month)'") as two_mo,
zeroifnull("'(Replacement) Vial)'") as replacement_vial,
zeroifnull("'(Replacement) Jar + Vial'") as replacement_jar_and_vial,
zeroifnull("'(Replacement) Daily Synbiotic'") as replacement_wk,
zeroifnull("'(Replacement) Jar'") as replacement_jar,
zeroifnull("'(Replacement) Daily Synbiotic—Refill'") as replacement_rf,
zeroifnull("'(Replacement) Jar Lid'") as replacement_jar_lid

from (
    
    
    
    select*

    from(
        select to_date(t.date) as day, 
        sum(adjustment_quantity) as quantity,
        adjustment_description as sku
        from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
        join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
        where t.type = 'purchase'
        and t.status = 'success'
        and sku not ilike '%shipping%'
        group by day, sku) as new_entry

            pivot (sum(quantity) for sku in 
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
                  )

                  order by day 
  
  ) as quantities