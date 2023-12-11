CREATE or replace VIEW MARKETING_DATABASE.GOOGLE_ANALYTICS_WEB_METRICS.GOOGLE_ANALYTICS_ECOMMERCE_TRANSACTIONS as 
  select date, 
  source_medium, 
  sum(transaction_revenue) as Revenue, 
  sum(transaction_tax) as Tax, 
  sum(transaction_shipping) as Shipping, 
  sum(refund_amount) as Refund,
  sum(item_quantity) as Quantity
  from "MARKETING_DATABASE"."GOOGLE_ANALYTICS_ECOMMERCE_CONVERSIONS"."GOOGLE_ANALYTICS_ECOMMERCE_CONVERSIONS" 
  -- where date = '2021-01-26'
  group by date, source_medium
  order by date desc;