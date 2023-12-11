create or replace view marketing_database.public.shipping_averages as

select a.adjustment_product_code as sku, avg(s.shipping_cost) as avg_shipping_cost, 'USPS' as carrier 
  from "MARKETING_DATABASE"."GOOGLE_SHEETS"."USPS_SHIPPING" as s
  join (select*
       from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
       where adjustment_description not ilike '%shipping%') as a on s.order_ = a.invoice_number
  group by sku

  
  union 
  

  select a.adjustment_product_code as sku, avg(s.total) as avg_shipping_cost, 'UPS' as carrier
  from "MARKETING_DATABASE"."GOOGLE_SHEETS"."UPS_SHIPPING" as s
  join (select*
       from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
       where adjustment_description not ilike '%shipping%') as a on substr(s.package_reference_number_1,1,6) = a.invoice_number
  group by sku

  
  union 
  
  select a.adjustment_product_code as sku, avg(s.total_charge) as avg_shipping_cost, 'OSM' as carrier
  from "MARKETING_DATABASE"."GOOGLE_SHEETS"."OSM_SHIPPING" as s
  join (select*
       from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS"
       where adjustment_description not ilike '%shipping%') as a on s.order_number = a.invoice_number
  group by sku