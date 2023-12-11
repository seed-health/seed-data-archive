create or replace view MARKETING_DATABASE.PUBLIC.ORDER_REPORT_3_0 as

select 
non_replacement_quantities.day as day,
dgt_gains.dgt_gain as dgt_gains,
zeroifnull("'syn-wk'") as WK,
zeroifnull("'syn-rf'") as RF,
zeroifnull("'syn-rf-3mo'") as three_mo,
zeroifnull("'syn-rf-2mo'") as two_mo,
zeroifnull("'(Replacement) Vial'") as replacement_vial,
zeroifnull("'(Replacement) Jar + Vial'") as replacement_jar_and_vial,
zeroifnull("'(Replacement) Daily Synbiotic'") as replacement_wk,
zeroifnull("'(Replacement) Jar'") as replacement_jar,
zeroifnull("'(Replacement) Daily Synbiotic—Refill'") as replacement_rf,
zeroifnull("'(Replacement) Jar Lid'") as replacement_jar_lid,
zeroifnull(wk_refunds.wk_refund_amount) as wk_refund_amount,
zeroifnull(wk_refunds.wk_refund_quantity) as wk_refund_quantity,
zeroifnull(rf_refunds.rf_refund_amount) as rf_refund_amount,
zeroifnull(rf_refunds.rf_refund_quantity) as rf_refund_quantity

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
          and adjustment_description not ilike '%shipping%'
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
                   ('(Replacement) Vial',
                    '(Replacement) Jar + Vial',
                    '(Replacement) Daily Synbiotic',
                    '(Replacement) Jar',
                    '(Replacement) Daily Synbiotic—Refill',
                    '(Replacement) Jar Lid'
                   )
                  )

         order by day 
  
  ) as replacement_quants on replacement_quants.day = non_replacement_quantities.day   
  
      right join
    (
        select to_date(map.created_at) as day, 
            sum(quantity) as dgt_gain
        from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING" as map
        where map.created_at is not null
        and to_date(map.created_at) > '2021-01-31'
        group by day
    ) as dgt_gains on dgt_gains.day = non_replacement_quantities.day
  
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
       and adjustment_product_code  = 'syn-wk'
       group by day, adjustment_product_code
    ) as wk_refunds on dgt_gains.day = wk_refunds.day

    left join (
    
       select to_date(t.date) as day, 
             sum(amount) as rf_refund_amount, 
             sum(adjustment_quantity) as rf_refund_quantity
         from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
         join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
         where t.type = 'refund'
         and t.status = 'success'
         and adjustment_description not ilike '%shipping%'
         and adjustment_product_code  = 'syn-rf'
         group by day, adjustment_product_code
    ) as rf_refunds on dgt_gains.day = rf_refunds.day  
order by non_replacement_quantities.day desc;