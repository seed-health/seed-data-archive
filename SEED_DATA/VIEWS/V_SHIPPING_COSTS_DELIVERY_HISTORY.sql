create or replace view SEED_DATA.DEV.V_SHIPPING_COSTS_DELIVERY_HISTORY(
	ORDER_NUMBER,
	SHIPPING_COST,
	SHIPPING_WEIGHT,
	INTL_DOMESTIC,
	DELIVERED_DATE
) as 

----- need to add the remaining files and complete the request from Passport
 
 with shipping_costs_delv_domestic as 
(
        select 
          shipment_guid
        , max(shipping_cost) as shipping_cost 
        , max(shipping_weight) as shipping_weight
        , max(to_date(delivered_date)) as delivered_date
        from 
        
        (select order_number as shipment_guid,shipping_cost,weight as shipping_weight,to_date(delivered_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_MISSING"  ---- ADDED BY CP ON 6/9

        UNION ALL
        
        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_JAN_2022"

        UNION ALL 

        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_FEB_22"

        UNION ALL 

        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_MAR_2022"

        UNION ALL 

        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_APR_2022"

        UNION ALL 

        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_MAY_2022"

        UNION ALL 

        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_JUN_2022"

        UNION ALL 

        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_JUL_2022"

        UNION ALL 

        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_AUG_2022"
      
        UNION ALL 

        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_SEPT_22"
      
        UNION ALL 

        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_OCT_22"
      
        UNION ALL 
      
        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_NOV_2022"
      
        UNION ALL 
      
        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_DEC_2022"
      
        UNION ALL 
      
        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_JAN_2023"

        UNION ALL 
      
        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_FEB_2023" ---- ADDED BY CP ON 6/8

        UNION ALL 
      
        select shipment_guid,shipping_cost,shipping_weight,to_date(carrier_status_date) as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."SHIPPING_WCD_MAR_2023"---- ADDED BY CP ON 6/8
         
        union all 
         
         select 
         distinct o.ORDER_NUMBER as shipment_guid
         , null as shipping_cost
         , null as shipping_weight
         , to_date(ACTUAL_DELIVERY_AT) as delivered_date 
         from 
        "MARKETING_DATABASE"."STORD"."SHIPMENTS" as o
         left join "MARKETING_DATABASE"."STORD"."ORDERS"  as s 
         on o.order_number = s.order_number
         where DELIVERY_STATUS = 'delivered' 
         --and to_date(ship_date) >= '2023-04-06' 
         and upper(dest_country) = 'US'
        
         -----ADDED BY CM on 7/6
        )
        group by 1
        )

, shipping_costs_delv_international as 
(
          select 
           order_id
         , max(shipping_cost) as shipping_cost
         , max(shipping_weight) as shipping_weight
         , max(to_date(delivered_date)) as delivered_date
         from 
        (
        select 
          case when order_id like 'SEED-%' THEN order_id 
               else CONCAT('SEED-',ltrim(order_id, 'SEED'))
               end as order_id 
        , total_shipping_cost as shipping_cost
        , BILLABLE_WEIGHT_OZ_ as shipping_weight
        , left(DELIVERY_DATE_GMT,10) as delivered_date 
        from "MARKETING_DATABASE"."PASSPORT"."SHIPPING_COSTS" 
        where DELIVERY_DATE_GMT is not null 
          and LENGTH(order_id) in (11,12)
          and _file in ('seed_passport_1Jan23_1May23_invoiced_data.csv','seed_passport_1Jan22_31Dec22_invoiced_data .csv','Seed invoicing.csv'))
        WHERE delivered_date <> 'NULL'
        group by 1    
    
      union all 
       
         select distinct o.ORDER_NUMBER as shipment_guid, null as shipping_cost, null as shipping_weight, to_date(ACTUAL_DELIVERY_AT) as delivered_date from 
        "MARKETING_DATABASE"."STORD"."SHIPMENTS" as o
         left join "MARKETING_DATABASE"."STORD"."ORDERS"  as s 
         on o.order_number = s.order_number
         where DELIVERY_STATUS = 'delivered' and to_date(ship_date) >= '2023-05-01' and upper(dest_country) <> 'US'
         
         -----ADDED BY CM on 7/6
)
, shipping_costs_delv_combined as
(
        SELECT 
         distinct
          shipment_guid as order_number
        , shipping_cost
        , shipping_weight
        , 'Domestic' as Intl_domestic
        , to_date(delivered_date) as delivered_date
  
        FROM shipping_costs_delv_domestic
        
        

union all 

        SELECT 
        distinct
          order_id as order_number
        , shipping_cost
        , shipping_weight
        , 'International' as Intl_domestic
        , delivered_date
          
        FROM shipping_costs_delv_international
      
)

   
select  * from  shipping_costs_delv_combined;