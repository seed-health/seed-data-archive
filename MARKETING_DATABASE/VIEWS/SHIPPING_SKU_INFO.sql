create or replace view MARKETING_DATABASE.DBT_PRODUCTION.SHIPPING_SKU_INFO (
	ORDER_NUMBER,
	ORDER_DATE,
	ORDER_SHIPPED_DATE,
	DELIVERED_DATE,
	INTL_DOMESTIC,
	COUNTRY,
	CITY,
	STATE,
	ZIP_CODE,
	SHIP_SERVICE_CODE,
	SHIPPING_COST,
	SHIPPING_WEIGHT,
	FULFILLMENT_PARTNER,
	SYN_WK,
	SYN_RF,
	SYN_RF_2MO,
	SYN_RF_3MO,
	SYN_RF_6MO,
	PDS_WK,
	PDS_RF,
	PDS_RF_2MO
) as (

WITH updated_invoices as 
(   select 
      *
    , CONCAT('SEED-',invoice_number) as updated_invoice_number
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY"
    where billed_date > '2021-12-15'
    )

, updated_sku_adjustments as 
(    
    select 
      i.updated_invoice_number as order_number
    , a.ADJUSTMENT_PLAN_CODE as SKU
    , a.ADJUSTMENT_QUANTITY as quantity
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a 
    left join updated_invoices as i on a.invoice_id = i.id
    where adjustment_created_at > '2021-12-31'
    and adjustment_description not ilike '%shipping%' 
    and a.adjustment_plan_code 
        in ('syn-wk','syn-rf','syn-rf-2mo','syn-rf-3mo','syn-rf-6mo','pds-wk','pds-rf','pds-rf-2mo')
     )

, finalized_sku as 
(
     select 
      *
     from updated_sku_adjustments 
        pivot(sum(quantity) for SKU 
            in ('syn-wk','syn-rf','syn-rf-2mo','syn-rf-3mo','syn-rf-6mo','pds-wk','pds-rf','pds-rf-2mo'))
        as p 
        order by order_number
      )
    
, shipping_wcd_domestic as 
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
        )
        group by 1
        )
  
        
, shipping_wcd_international as 
(

/*with passport_google as 
(    select 
          order_id
        , total_shipping_cost as shipping_cost
        , BILLABLE_WEIGHT_OZ_ as shipping_weight
        , DELIVERY_DATE_GMT as delivered_date
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."PASSPORT_SHIPPING" where DELIVERY_DATE_GMT is not null )
        
    , passport_google_clean as
    (     
         select 
           order_id
         , shipping_cost
         , shipping_weight
         , to_date(delivered_date) as delivered_date 
         from passport_google
         ) */
   -- with passport_pipeline as 
   -- (
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
          and _file in ('seed_passport_1Jan23_1May23_invoiced_data.csv','seed_passport_1Jan22_31Dec22_invoiced_data .csv'))
        WHERE delivered_date <> 'NULL'
        group by 1
 --       )
        
   /* , passport_pipeline_clean as
    (     
         select 
           order_id
         , shipping_cost
         , shipping_weight
         , to_date(delivered_date) as delivered_date 
         from passport_google
         ) */
                    
        --select * from passport_google_clean
        --union
 --       select * from passport_pipeline_clean
        
        )

, oceanx_stord as 
  (  
  select 
   distinct
     order_number
   , order_date
   , order_shipped_date
   , upper(shipto_city) as shipto_city
   , upper(shipto_state) as shipto_state
   , upper(shipto_country_code) as shipto_country_code
   , shipto_zip
   , ship_service_code
   , 'oceanx' as fulfillment_partner
   from "MARKETING_DATABASE"."OCEANX_DATALAKE"."VW_DATA_SHIPMENT"
             
   UNION ALL
   
   select
     distinct
      o.order_number as order_number
    , o.ORDER_CREATED_DATETIME as order_date
    , s.SHIP_DATE as order_shipped_date
    , upper(dest_city) as shipto_city
    , upper(dest_state) as shipto_state
    , upper(dest_country) as shipto_country_code
    , DESTINATION_POSTAL_CODE as shipto_zip
    , service_method as ship_service_code
    , 'stord' as fulfillment_partner
    from "MARKETING_DATABASE"."STORD"."ORDERS" as o 
    left join "MARKETING_DATABASE"."STORD"."SHIPMENTS" as s on o.order_number = s.order_number
    where o.order_type = 'sales'
)    
  
, final_us as 
       (
        select 
          ship.order_number
        , order_date
        , order_shipped_date
        , to_date(delivered_date) as delivered_date
        , 'Domestic' as Intl_domestic
        , SHIPTO_COUNTRY_CODE as country
        , SHIPTO_CITY as city
        , SHIPTO_STATE as state
        , SHIPTO_ZIP as zip_code
        , ship_service_code
        , shipping_cost
        , shipping_weight
        , fulfillment_partner
        , finalized_sku."'syn-wk'" as syn_wk
        , finalized_sku."'syn-rf'" as syn_rf
        , finalized_sku."'syn-rf-2mo'" as syn_rf_2mo
        , finalized_sku."'syn-rf-3mo'" as syn_rf_3mo
        , finalized_sku."'syn-rf-6mo'" as syn_rf_6mo
        , finalized_sku."'pds-wk'" as pds_wk
        , finalized_sku."'pds-rf'" as pds_rf
        , finalized_sku."'pds-rf-2mo'" as pds_rf_2mo

      from oceanx_stord as ship 
      left join finalized_sku on ship.order_number = finalized_sku.order_number
      left join shipping_wcd_domestic on ship.order_number = shipping_wcd_domestic.shipment_guid
      where ship.order_date > '2021-12-31' and (shipto_country_code ilike '%US%' and shipto_country_code ilike '%us%')
        )
  
, final_international as 
        (
          select 
            ship.order_number as order_number
          , order_date
          , order_shipped_date
          , to_date(delivered_date) as delivered_date
          , 'International' as Intl_domestic
          , SHIPTO_COUNTRY_CODE as country
          , SHIPTO_CITY as city
          , SHIPTO_STATE as state
          , SHIPTO_ZIP as zip_code
          , ship_service_code
          , shipping_cost
          , shipping_weight
          , fulfillment_partner
          , finalized_sku."'syn-wk'" as syn_wk
          , finalized_sku."'syn-rf'" as syn_rf
          , finalized_sku."'syn-rf-2mo'" as syn_rf_2mo
          , finalized_sku."'syn-rf-3mo'" as syn_rf_3mo
          , finalized_sku."'syn-rf-6mo'" as syn_rf_6mo
          , finalized_sku."'pds-wk'" as pds_wk
          , finalized_sku."'pds-rf'" as pds_rf
          , finalized_sku."'pds-rf-2mo'" as pds_rf_2mo

      from oceanx_stord as ship 
      left join finalized_sku on ship.order_number = finalized_sku.order_number
      left join shipping_wcd_international on ship.order_number = shipping_wcd_international.order_id
      where ship.order_date > '2021-12-31' 
      and (shipto_country_code not ilike '%US%' and shipto_country_code not ilike '%us%')
        )
      
, final_data as
      (
      select * from final_us
      UNION ALL 
      select * from final_international
      )
   select * from final_data where order_number like 'SEED-%' and to_date(order_date) <= to_date(current_date())
  );