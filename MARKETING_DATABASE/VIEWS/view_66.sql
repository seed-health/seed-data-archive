create view v_customer_oceanx_returns as

select distinct r.RETURN_DATE as Date,
                  a.account_email as Email,
                  a.account_code as AccountCode,
                  'OCEANX' as DataSource,
                  o.order_status as Action,
                  'OCEANX.VW_RPT_RETURN' as SourceTableName, 
                  'ORDER_NUMBER' as SourceColumnName, 
                  to_varchar(o.order_number) as SourceTableID,
                  to_varchar(r.RETURN_REASON_CODE) as MiscData1,
                  to_varchar(r.RETURN_REASON_DESC) as MiscData2,
                  to_varchar(r.WAREHOUSE_ID) as MiscData3,
                  to_varchar(r.PRODUCT_CODE) as MiscData4
      from "MARKETING_DATABASE"."OCEANX"."VW_RPT_RETURN" as r
      join "MARKETING_DATABASE"."OCEANX"."VW_RPT_ORDER" o on o.ORDER_NUMBER = r.ORDER_NUMBER
      join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ACCOUNTS" as a on a.ACCOUNT_EMAIL = o.EMAIL_ADDRESS