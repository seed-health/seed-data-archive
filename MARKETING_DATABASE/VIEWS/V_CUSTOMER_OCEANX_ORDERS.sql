create view v_customer_oceanx_orders as 
 select o.order_date as Date,
            a.account_email as Email,
            a.account_code as AccountCode,
            'OCEANX' as DataSource,
            o.order_status as Action,
            'OCEANX.VW_RPT_ORDER' as SourceTableName, 
            'ORDER_NUMBER' as SourceColumnName, 
            to_varchar(o.order_number) as SourceTableID,
            to_varchar(o.order_status) as Delivery_status,
            to_varchar(o.shipto_address_1) as delivery_address,
            to_varchar(o.shipto_city) as delivery_city,
            to_varchar(o.shipto_state) as delivery_state,
           -- to_varchar(min(o.ship_date)) as first_shipdate,
            --to_varchar(max(o.ship_date)) as last_shipdate,
            to_varchar(o.tracking_number) as tracking_number,
            to_varchar(o.product_name) as product_name,
            to_varchar(o.order_ship_amt) as shipping_cost
           
            /* missing - current_delivery_status, packages, returned */
      from "MARKETING_DATABASE"."OCEANX"."VW_RPT_ORDER" o
      join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ACCOUNTS" as a on a.ACCOUNT_EMAIL = o.EMAIL_ADDRESS