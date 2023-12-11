create or replace view MARKETING_DATABASE.PUBLIC.ORDER_REPORT_supply_chain_1_0(
	DAY,
	DS_WK,
	DS_RF,
	DS_RF_3mo,
	DS_RF_6mo,
	DS_RF_2mo,
	PDS_WK,
	PDS_RF,
	PDS_RF_3mo,
	PDS_RF_6mo,
	replacement_vial,
	replacement_jar_and_vial,
	replacement_jar,
	replacement_jar_lid,
	replacement_DS_WK,
	replacement_DS_RF,
    replacement_DS_RF_3mo,
    replacement_PDS_WK_empty,
    replacement_PDS_WK_full,
    replacement_PDS_RF,
    replacement_PDS_RF_3mo
) as

select 
non_replacement_quantities.day as day,
zeroifnull("'syn-wk'") as DS_WK,
zeroifnull("'syn-rf'") as DS_RF,
zeroifnull("'syn-rf-3mo'") as DS_RF_3mo,
zeroifnull("'syn-rf-6mo'") as DS_RF_6mo,
zeroifnull("'syn-rf-2mo'") as DS_RF_2mo,
zeroifnull("'pds-wk'") as PDS_WK,
zeroifnull("'pds-rf'") as PDS_RF,
zeroifnull("'pds-rf-3mo'") as PDS_RF_3mo,
zeroifnull("'pds-rf-2mo'") as PDS_RF_6mo,


zeroifnull("'(Replacement) Vial'") as replacement_vial,
zeroifnull("'(Replacement) Jar + Vial'") as replacement_jar_and_vial,
zeroifnull("'(Replacement) Jar'") as replacement_jar,
zeroifnull("'(Replacement) Jar Lid'") as replacement_jar_lid,

zeroifnull("'(Replacement) Daily Synbiotic'") as replacement_DS_WK,
zeroifnull("'(Replacement) Daily Synbiotic—Refill'") as replacement_DS_RF,
zeroifnull("'(Replacement) Daily Synbiotic—Refill (3 month)'") as replacement_DS_RF_3mo,


zeroifnull("'(PDS08 Replacement) Welcome Kit, Empty'") as replacement_PDS_WK_empty,
zeroifnull("'(PDS08 Replacement) Welcome Kit, Full'") as replacement_PDS_WK_full,
zeroifnull("'(PDS08 Replacement) Replacement, Refill, Monthly'") as replacement_PDS_RF,
zeroifnull("'(PDS08 Replacement) Refill, 3 Month'") as replacement_PDS_RF_3mo

from (
    
    
    
    select*

      from(
          select to_date(t.date) as day, 
          sum(adjustment_quantity) as quantity,
          adjustment_plan_code as sku
          from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
          join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
          where t.type = 'purchase'
          and t.status = 'success'
          and adjustment_description not ilike '%shipping%'
          group by day, sku) as non_replacement

              pivot (sum(quantity) for sku in 
                     ('syn-wk',
                      'syn-rf',
                      'syn-rf-2mo',
                      'syn-rf-3mo', 
                      'syn-rf-6mo',
                      'pds-wk',
                      'pds-rf',
                      'pds-rf-2mo',
                      'pds-rf-3mo'
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
                    '(Replacement) Jar',
                    '(Replacement) Jar Lid',
                    '(Replacement) Daily Synbiotic',
                    '(Replacement) Daily Synbiotic—Refill',
                    '(Replacement) Daily Synbiotic—Refill (3 month)',
                    '(PDS08 Replacement) Welcome Kit, Empty',
                    '(PDS08 Replacement) Welcome Kit, Full',
                    '(PDS08 Replacement) Replacement, Refill, Monthly',
                    '(PDS08 Replacement) Refill, 3 Month'  
                   )
                  )

         order by day 
  
  ) as replacement_quants on replacement_quants.day = non_replacement_quantities.day   
 ;