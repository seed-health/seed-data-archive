create or replace view SEED_DATA.DEV.V_SHIPMENT_HISTORY(
	ORDER_NUMBER,
	ORDER_DATE_TS,
	ORDER_DATE,
	ORDER_SHIPPED_DATE_TS,
	ORDER_SHIPPED_DATE,
	INTL_DOMESTIC,
	COUNTRY,
	CITY,
	STATE,
	ZIP_CODE,
	SHIP_SERVICE_CODE,
	FULFILLMENT_PARTNER,
    ORIGIN_FACILITY_NAME,
	REGION,
	SYN_WK,
	SYN_RF,
	SYN_RF_2MO,
	SYN_RF_3MO,
	SYN_RF_6MO,
	PDS_WK,
	PDS_RF,
	PDS_RF_2MO,
	DELIVERED_DATE,
	SHIPPING_COST,
	SHIPPING_WEIGHT,
	DELIVERED_DATE_PRIOR_TO_SHIPPED_DATE_FLAG,
	CARRIER_GROUP
) as 
---- oceanx + stord shipping data
with oceanx_stord as 
  (  
  select 
   distinct
     order_number
   , order_date
   , order_shipped_date
    -----added fulfilment orgin at the request of shipping 7-26-23
   , 'OCEANX' as ORIGIN_FACILITY_NAME
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
        -----added fulfilment orgin at the request of shipping 7-26-23
    , s.ORIGIN_FACILITY_NAME as ORIGIN_FACILITY_NAME
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

--- build combined shipping data
, build_shipping as (
        
select 
  distinct
  order_number
, order_date as order_date_ts
, to_date(order_date) as order_date
, order_shipped_date as order_shipped_date_ts
, to_date(order_shipped_date) as order_shipped_date
, case when shipto_country_code ilike '%US%' and shipto_country_code ilike '%us%' then 'Domestic'
       when shipto_country_code not ilike '%US%' and shipto_country_code not ilike '%us%' then 'International'
       end as Intl_domestic
, SHIPTO_COUNTRY_CODE as country
, SHIPTO_CITY as city
, SHIPTO_STATE as state
, SHIPTO_ZIP as zip_code
, ship_service_code
, fulfillment_partner
, ORIGIN_FACILITY_NAME

from oceanx_stord 
where to_date(order_date) > '2021-12-31'
)

----- finalize build with shipping costs, delivery dates, and sku identification
, final_build as (
select 
  distinct
  final.order_number
, final.order_date_ts
, final.order_date
, final.order_shipped_date_ts
, final.order_shipped_date
, final.Intl_domestic
, final.country
, final.city
, final.state
, final.zip_code
, final.ship_service_code
, final.fulfillment_partner
, final.ORIGIN_FACILITY_NAME  
, case when final.country in ('US','PR','AS','GU','MP','MH','FM') then SRM.Region else CAM.AREA end as Region
, fs."'syn-wk'" as syn_wk
, fs."'syn-rf'" as syn_rf
, fs."'syn-rf-2mo'" as syn_rf_2mo
, fs."'syn-rf-3mo'" as syn_rf_3mo
, fs."'syn-rf-6mo'" as syn_rf_6mo
, fs."'pds-wk'" as pds_wk
, fs."'pds-rf'" as pds_rf
, fs."'pds-rf-2mo'" as pds_rf_2mo
, max(scdh.delivered_date) as delivered_date  ---- lastest devliery date by order number 
, max(scdh.shipping_cost) as shipping_cost
, max(scdh.shipping_weight) as shipping_weight
from build_shipping as final
---- join to shipping costs and delivery information
left join SEED_DATA.DEV.V_SHIPPING_COSTS_DELIVERY_HISTORY as scdh on final.order_number = scdh.order_number
---- join to finalized sku build
left join SEED_DATA.DEV.V_SHIPPING_FINALIZED_SKU as fs on final.order_number = fs.order_number
left join  "MARKETING_DATABASE"."GOOGLE_SHEETS"."STATE_REGION_MAPPING"   as SRM on final.state = SRM.state_code 
left join  "MARKETING_DATABASE"."GOOGLE_SHEETS"."COUNTRY_AND_AREA_MAPPING"  As CAM on final.COUNTRY = CAM.COUNTRY_CODE 

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22

)

select *, case when delivered_date < order_shipped_date then 'Y' else 'N' end as delivered_date_prior_to_shipped_date_flag,

case when  ship_service_code in ('FedEx Home Delivery','Stord - FedEx SmartPost','FedEx Ground','Stord - FedEx Standard Overnight','fedex_international_','FedEx 2 Day','SMART_POST') then 'FEDEX'
     when  ship_service_code in ('ups_next_day_air_sav','Stord - UPS Worldwide Expedited','UPS Mail Innovations Parcel Select Lightweight','ups_next_day_air','ups_2nd_day_air','ups_ground','UPSSaver') then 'UPS'
     when  ship_service_code in ('GROUND_HOME_DELIVERY','usps_first_class_mai','First','USPS First-Class Package Service','usps_parcel_select','usps_priority_mail_i','First Class','usps_priority_mail_e','usps_priority_mail','USPS Priority Mail','Priority','FirstClassPackageInternationalService','usps_first_class_pac','amazon_usps_priority') then 'USPS'
     when  ship_service_code in ('FedEx Home Delivery','Stord - FedEx SmartPost','FedEx Ground','Stord - FedEx Standard Overnight','fedex_international_','FedEx 2 Day','SMART_POST') then 'FEDEX'
     when  ship_service_code in ('apc_priority_ddu_del','PriorityDdpDelcon','apc_priority_ddp_del') then 'APC'
     when  ship_service_code in ('Stord - Passport Priority DDP More Than 64oz','PStord - Passport Priority DDP Less Than 64oz') then 'PASSPORT'
     when  ship_service_code in ('osm_parcel_select','osm_parcel_select_li') then 'OSM'
     when  ship_service_code in ('Stord - DHL Parcel Expedited Max, Parcel Plus Expedited Max','DHLPacketInternational','DHLParcelExpeditedMax') then 'DHL'
     when  ship_service_code in ('ontrac_ground_servic') then 'ONTRAC'
     else 'OTHER' end as Carrier_Group
   
     from final_build 
     
     where order_number like 'SEED-%' 
     and to_date(order_date) <= to_date(current_date()-1) 
     and ORDER_SHIPPED_DATE is not null;