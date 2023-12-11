create or replace view MARKETING_DATABASE.PUBLIC.ORDER_REPORT_SC_V1(
	DAY,
	DS_WK,
	DS_RF,
	DS_RF_3MO,
	DS_RF_6MO,
	DS_RF_2MO,
	PDS_WK,
	PDS_RF,
	PDS_RF_3MO,
	PDS_RF_6MO,
	REPLACEMENT_VIAL,
	REPLACEMENT_JAR_AND_VIAL,
	REPLACEMENT_JAR,
	REPLACEMENT_JAR_LID,
	REPLACEMENT_DS_WK,
	REPLACEMENT_DS_RF,
	REPLACEMENT_DS_RF_3MO,
	REPLACEMENT_PDS_WK_EMPTY,
	REPLACEMENT_PDS_WK_FULL,
	REPLACEMENT_PDS_RF,
	REPLACEMENT_PDS_RF_3MO
) as

with sku as 
    ( 
      select*
      from(
          //select to_date(t.date) as day, 
          select to_date(t.date) as day, //lets make it PDT
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
      ),
      
 replacement_sku as 
     ( 
       select *
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
                    '(PDS08 Replacement) Replacement, Refill, Monthly' ,
                    '(PDS08 Replacement) Refill, 3 Month'
                   )
                  )

         order by day 
  
     )/*,
     
     subs as 
     (
            select 
            case 
              when is_recharge_native = 1 then to_date(dateadd(hour, -4, map.created_at))
              when is_imported = 1 then to_date(dateadd(hour, -4, map.created_at))
              else to_date(map.created_at) end
              as day,
            sum(quantity) as total_subs_gained
            from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_DS_01" as map
            group by day
            order by day
     
     )*/
     
     select sku.day as day,
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
            from sku 
            left join replacement_sku on sku.day = replacement_sku.day;