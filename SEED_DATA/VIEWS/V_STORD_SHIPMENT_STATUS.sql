create or replace view SEED_DATA.DEV.V_STORD_SHIPMENT_STATUS as 
With LS as (
  
select distinct ORDER_NUMBER,
       max(_modified) as LS_TS
  
  from
"MARKETING_DATABASE"."STORD"."SHIPMENTS"   
  group by 1
           ) 

, base as (
select 
   ORDER_NUMBER
  ,DELIVERY_STATUS
  ,to_date(ACTUAL_DELIVERY_AT) as ACTUAL_DELIVERY_AT
  ,to_date(ship_date) as shipped_date
  ,_modified as LS_TS

  from
"MARKETING_DATABASE"."STORD"."SHIPMENTS" 
)   
Select
 LS.ORDER_NUMBER
,DELIVERY_STATUS
,SHIPPED_DATE
,ACTUAL_DELIVERY_AT
--count(ls.order_number), count(distinct ls.order_number)
from LS
Left join Base
using(ORDER_NUMBER,LS_TS)
where order_number like 'SEED-%'