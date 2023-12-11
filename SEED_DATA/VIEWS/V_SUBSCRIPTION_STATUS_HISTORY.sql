create or replace view SEED_DATA.DEV.V_SUBSCRIPTION_STATUS_HISTORY as 

--- Final code		
with sub_history_plan_clean as		
(
        select 
          *
        , case when plan_code like 'syn-wk%' then 'syn-wk'
                else plan_code end as updated_plan_code
        --,lag(updated_plan_code) over(partition by subscription_uuid order by version_started_at) as lag_plan_code
        , row_number() over(partition by subscription_uuid,subscription_state,updated_plan_code order by version_started_at) as row_no_agg		
        , row_number() over(partition by subscription_uuid order by version_started_at) as row_no_sub		
        , (row_no_sub - row_no_agg) as final_rank	
        , to_date(min(subscription_activated_at) over (partition by subscription_uuid)) as subscription_activated_date		
        , to_date(max(subscription_expires_at) over (partition by subscription_uuid)) as subscription_expires_date	
        , coalesce(version_ended_at, '3000-01-01') as version_ended_at_clean -- To account for same plan cancelled over different time		

        from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTION_HISTORY"		
        --where subscription_uuid = '59314b1626fdf2331a986a4b9fb0a550'		
        --where plan_code not ilike '%pds%'and plan_code not ilike '%ds01-trial%'		
)	
		
, final_table as		
(		
        select 
          subscription_uuid		
        , subscription_state	
        , updated_plan_code	
        , subscription_activated_date	
        , subscription_expires_date	
        , final_rank
        , to_date(min(version_started_at)) as version_started_at	
        , to_date(max(version_ended_at_clean)) as version_ended_at		
        
        from sub_history_plan_clean		
        group by 1,2,3,4,5,6		
        order by 1,7		
)		
		
select 
  subscription_uuid	
, subscription_state	
, updated_plan_code		
, subscription_activated_date
, subscription_expires_date	
, version_started_at	
, case when version_ended_at like '3000-01-01' then null
        else version_ended_at end as version_ended_at		

from final_table	;