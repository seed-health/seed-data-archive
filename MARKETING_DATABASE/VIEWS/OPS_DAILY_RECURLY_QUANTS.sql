create or replace view MARKETING_DATABASE.PRODUCTION_VIEWS.OPS_DAILY_RECURLY_QUANTS
as

select 
non_replacement_quantities.day,
zeroifnull("'syn-wk'") as WK,
zeroifnull("'syn-rf'") as RF,
zeroifnull("'syn-rf-3mo'") as three_mo,
zeroifnull("'syn-rf-2mo'") as two_mo,
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
          adjustment_product_code as sku
          from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
          join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
          where t.type = 'purchase'
          and t.status = 'success'
          and sku not ilike '%shipping%'
          group by day, sku) as non_replacement

              pivot (sum(quantity) for sku in 
                     ('syn-wk',
                      'syn-rf', 
                      'syn-rf-3mo', 
                      'syn-rf-2mo'
                     )
                    )

          order by day 
  
  ) as non_replacement_quantities left join (
  
      select*

        from(
        select to_date(a.adjustment_created_at) as day, 
        sum(adjustment_quantity) as quantity,
        adjustment_description as sku
        from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a 
        where sku ilike '%replacement%'
        group by day, sku) as replacement
        
            pivot (sum(quantity) for sku in 
                   ('(Replacement) Vial)',
                    '(Replacement) Jar + Vial',
                    '(Replacement) Daily Synbiotic',
                    '(Replacement) Jar',
                    '(Replacement) Daily Synbiotic—Refill',
                    '(Replacement) Jar Lid'
                   )
                  )

         order by day 
  
  ) as replacement_quants on replacement_quants.day = non_replacement_quantities.day
  order by non_replacement_quantities.day desc;