create or replace view MARKETING_DATABASE.PUBLIC.PO_SUMMARY
as

/*refills*/
select*
from (

select REFILL_POUCH as material, date_ as eta, PO_NUMBER as po_number, arrived_total as total
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_RF"
where arrived_total > 0 

union

select REFILL_POUCH_MAILER as material, date_ as eta, PO_NUMBER_1 as po_number, arrived_total_1 as total
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_RF"
where arrived_total_1 > 0 

union

select GREEN_CELL_CORN_FOAM as material, date_ as eta, PO_NUMBER_2 as po_number, arrived_total_2 as total
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_RF"
where arrived_total_2 > 0 

union

/*welcome kit*/


select GREEN_GLASS_JAR_LUMI_ as material, DATE_ as day, PO_NUMBER as po, arrived_total as arriving
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_WK"
where arrived_total > 0

union

select JAR_CAP_SEAL_LUMI_ as material, DATE_ as day, PO_NUMBER_1 as po, arrived_total_1 as arriving
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_WK"
where arrived_total_1 > 0

union

select GREEN_GLASS_TRAVEL_VIAL_LUMI_ as material, DATE_ as day, PO_NUMBER_2 as po, arrived_total_2 as arriving
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_WK"
where arrived_total_2 is not null

union

select TRAVEL_VIAL_CAP_SEAL_LUMI_ as material, DATE_ as day, PO_NUMBER_3 as po, arrived_total_3
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_WK"
where arrived_total_3 > 0

union 

select PAPERFOAM_TRAY as material, DATE_ as day, PO_NUMBER_4 as po, arrived_total_4
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_WK"
where arrived_total_4 > 0

union

select ALGAE_PAPER_WELCOME_BOX as material, DATE_ as day, PO_NUMBER_5 as po, arrived_total_5
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_WK"
where arrived_total_5 > 0 

union

select WELCOME_BOX_STICKER as material, DATE_ as day, PO_NUMBER_6 as po, arrived_total_6
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_WK"
where arrived_total_6 > 0

union

select  WELCOME_BOOKLET_ALGAE_PAPER_ as material, DATE_ as day, PO_NUMBER_7 as po, arrived_total_7
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_WK"
where arrived_total_7 > 0 

union

select  JAR_LABEL as material, DATE_ as day, PO_NUMBER_8 as po, arrived_total_8
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_WK"
where arrived_total_8 > 0

/*blended*/
union

select DESSICANT_DESSICARE_ as material, DATE_ as day, PO_NUMBER as po, arrived_total_kg_
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_BLENDED"
where arrived_total_kg_ > 0

union

select MXP_222_200_B_ as material, DATE_ as day, PO_NUMBER_1 as po, arrived_total_kg_1
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_BLENDED"
where arrived_total_kg_1 > 0

union 

select VESALE_BLEND_100_B_ as material, DATE_ as day, PO_NUMBER_2 as po, arrived_total_kg_2
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_BLENDED"
where arrived_total_kg_2 > 0

union

select B_LONGUM_120_B_ as material, DATE_ as day, PO_NUMBER_3 as po, arrived_total_kg_3
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_BLENDED"
where arrived_total_kg_3 > 0

union 

select B_INFANTIS_M_63_80_B_ as material, DATE_ as day, PO_NUMBER_4 as po, arrived_total_kg_4
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_BLENDED"
where arrived_total_kg_4 > 0

union


select L_PLANTARUM_400_B_ as material, DATE_ as day, PO_NUMBER_5 as po, arrived_total_kg_5
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_BLENDED"
where arrived_total_kg_5 > 0

union

select ADM_BLEND_50_B_ as material, DATE_ as day, PO_NUMBER_6 as po, arrived_total_kg_6
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_BLENDED"
where arrived_total_kg_6 > 0

union 

select B_BREVE_300_B_ as material, DATE_ as day, PO_NUMBER_7 as po, arrived_total_kg_7
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_BLENDED"
where arrived_total_kg_7 > 0

union

select L_RHAMNOSUS_500_B_ as material, DATE_ as day, PO_NUMBER_8 as po, arrived_total_kg_8
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_BLENDED"
where arrived_total_kg_8 > 0

union


select B_LACTIS_500_B_ as material, DATE_ as day, PO_NUMBER_9 as po, arrived_total_kg_9
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_BLENDED"
where arrived_total_kg_9 > 0

union 

select L_CASEI_300_B_ as material, DATE_ as day, PO_NUMBER_10 as po, arrived_total_kg_10
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_BLENDED"
where arrived_total_kg_10 > 0

union

select B_LONGUM_120_B_ as material, DATE_ as day, PO_NUMBER_11 as po, arrived_total_kg_11
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_BLENDED"
where arrived_total_kg_11 > 0

union

select POMELLA as material, DATE_ as day, PO_NUMBER_12 as po, arrived_total_kg_12
from "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_BLENDED"
where arrived_total_kg_12 > 0 ) as agg join "MARKETING_DATABASE"."SUPPLYCHAIN"."SUPPLYCHAIN_MAP" as map on map.raw_material = agg.material