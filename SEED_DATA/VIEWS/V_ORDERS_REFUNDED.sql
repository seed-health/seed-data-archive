create or replace view "SEED_DATA"."DEV"."V_ORDERS_REFUNDED" as 


     select billed_date,
            invoice_number
            
            from
          (select 
            ORIGINAL_INVOICE_NUMBER as invoice_number,
            billed_date     
          from 
          "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as i
          where INVOICE_TYPE = 'refund'
          
          union all
          
         select invoice_number as invoice_number,
                                  billed_date
         from  "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as i
          where INVOICE_TYPE = 'refund')    order by 2 desc