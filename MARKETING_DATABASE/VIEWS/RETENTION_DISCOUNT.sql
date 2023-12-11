create or replace view MARKETING_DATABASE.PUBLIC.RETENTION_DISCOUNT(
	RECURLY_SUBSCRIPTION_ID,
	CUSTOMER_ID,
	CUSTOMER_EMAIL,
	FIRST_NAME,
	LAST_NAME,
	CURRENT_STATUS,
	CREATED_AT,
	CANCELLED_AT,
	PRICE,
	QUANTITY,
	BILLING_COUNTRY,
	IS_INTERNATIONAL,
	SKU,
	IS_PREORDER,
    first_plan_code_discount,
    coupon_code,
    coupon_name,
    first_discount_percent
) as

with all_transactions_ordered as 
(select *, row_number () over(partition by t.subscription_id order by date) as sub_order_rnk,t.subscription_id as final_sub_id
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
 where t.type = 'purchase' and t.status = 'success' and adjustment_description not ilike '%shipping%' and ADJUSTMENT_AMOUNT > 0
),

first_transaction_discount as (
select FINAL_SUB_ID as subscription_id,ADJUSTMENT_PLAN_CODE as first_plan_code_discount,ato.ADJUSTMENT_COUPON_CODE as coupon_code,
       c.name as coupon_name, round((ADJUSTMENT_DISCOUNT*100/ADJUSTMENT_AMOUNT),2) as first_discount_percent
from all_transactions_ordered as ato left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."COUPONS" as c on ato.ADJUSTMENT_COUPON_CODE = c.COUPON_CODE
where sub_order_rnk = 1
 ),
 
 all_sub_info as (
 select RECURLY_SUBSCRIPTION_ID,CUSTOMER_ID,CUSTOMER_EMAIL,FIRST_NAME,LAST_NAME,CURRENT_STATUS,
        created_at,cancelled_at,price,quantity,billing_country,is_international,SKU,is_preorder
 from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_DS_01" as ds 
 union all 
 select RECURLY_SUBSCRIPTION_ID,CUSTOMER_ID,CUSTOMER_EMAIL,FIRST_NAME,LAST_NAME,CURRENT_STATUS,
        created_at,cancelled_at,price,quantity,billing_country,is_international,SKU,is_preorder
 from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_PDS_08" as pds 
 where recurly_subscription_id is not null
 )
 
 select a.*,fd.first_plan_code_discount,fd.coupon_code,fd.coupon_name,fd.first_discount_percent
 from all_sub_info as a left join first_transaction_discount as fd on a.recurly_subscription_id=fd.subscription_id