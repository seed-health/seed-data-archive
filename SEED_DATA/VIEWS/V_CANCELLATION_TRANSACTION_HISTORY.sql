create or replace view SEED_DATA.DEV.V_CANCELLATION_TRANSACTION_HISTORY(
	SUBSCRIPTION_ID,
	RECURLY_SUBSCRIPTION_ID,
	SUBSCRIPTION_CANCELED_AT,
	SKU,
	QUANTITY,
	CANCELATION_FORM_CREATED_AT,
	PRIMARY_REASON_ID,
	PRIMARY_REASON,
	SECONDARY_REASON_ID,
	SECONDARY_REASON,
	REASON_GROUP,
	DATE_MATCH_FLAG
) as 

--- Cancellation form was created 2022-02-23 onwards hence only recurly data is sufficent				
				
with cancellation as				
(				
select 
  user_cancel.subscription_id
, subscription_uuid as recurly_subscription_id
, recurly_sub.canceled_at as subscription_canceled_at
, recurly_sub.plan_code as sku
, recurly_sub.quantity as quantity
, user_cancel.created as cancelation_form_created_at
, reason_id as primary_reason_id
, map.reason as primary_reason
, secondary_reason_id
, sr.reason as secondary_reason
, rank() over(partition by user_cancel.subscription_id order by user_cancel.created desc) as created_rank	
---- ADDING RECURLY SUBSCRIPTION ID	
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as recurly_sub	
---- ADDING SUBSCRIPTION INFO	
left join "MARKETING_DATABASE"."SEED_CORE_PUBLIC"."SEED_ECOMMERCE_SUBSCRIPTION" as user_sub	
on user_sub.subscription_uuid = recurly_sub.uuid	
left join "MARKETING_DATABASE"."SEED_CORE_PUBLIC"."SEED_ECOMMERCE_USERSUBSCRIPTIONCANCELLATIONREASON" as user_cancel	
on user_sub.id = user_cancel.subscription_id				
---- ADDING PRIMARY REASON				
left join "MARKETING_DATABASE"."SEED_CORE_PUBLIC"."SEED_ECOMMERCE_CANCELLATIONREASON" as map				
on user_cancel.reason_id = map.ID				
---- ADDING SECONDARY REASON				
left join (SELECT ID, REASON				
FROM "MARKETING_DATABASE"."SEED_CORE_PUBLIC"."SEED_ECOMMERCE_CANCELLATIONREASON" where IS_SECONDARY_REASON = 'TRUE')as SR				
on user_cancel.SECONDARY_REASON_ID = SR.ID													
							
qualify created_rank = 1		
)				
     
, cancellation_final as (
select 
  c.subscription_id
, c.recurly_subscription_id
, c.subscription_canceled_at
, c.sku
, c.quantity
, c.cancelation_form_created_at
, c.primary_reason_id
, c.primary_reason
, c.secondary_reason_id
, c.secondary_reason
, coalesce(scrg.REASON_GROUP,'Other') as reason_group	
, case when to_date(c.subscription_canceled_at) = to_date(c.cancelation_form_created_at) then TRUE else FALSE end as date_match_flag

from cancellation as c				
---- ADDING CANCEL REASONS GROUPS				
left join MARKETING_DATABASE.GOOGLE_SHEETS.SUB_CANCEL_REASONS_GROUPS as scrg				
on c.primary_reason = scrg.PRIMARY_REASON and				
ifnull(c.SECONDARY_REASON,'none') = ifnull(scrg.SECONDARY_REASON,'none')				

) 

select * from cancellation_final;