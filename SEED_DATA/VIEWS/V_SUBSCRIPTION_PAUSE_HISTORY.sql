create or replace view SEED_DATA.DEV.V_SUBSCRIPTION_PAUSE_HISTORY(
	SUBSCRIPTION_UUID,
	SUBSCRIPTION_STATE,
	UPDATED_PLAN_CODE,
	SUBSCRIPTION_ACTIVATED_DATE,
	SUBSCRIPTION_EXPIRES_DATE,
	FINAL_RANK,
    PAUSED_AT_TS_FROM_SITE,
    PAUSED_AT_FROM_SITE,
	VERSION_STARTED_AT_TS,
	VERSION_ENDED_AT_TS,
	VERSION_STARTED_AT,
	VERSION_ENDED_AT,
	VERSION_ENDED_AT_CLEAN,
    PAUSED_AT_CLEAN,
    PAUSED_AT_CLEAN_TS,
    FLAG
) as 

with subscription_pause_history as		
(		
--- Final code		
    with sub_history_plan_clean as		
    (
            select 
              *
            , case when plan_code ilike 'syn-wk%' and plan_code ilike '%tuc%' then 'syn-wk'
                    else plan_code end as updated_plan_code		
            --, lag(updated_plan_code) over(partition by subscription_uuid order by version_started_at) as lag_plan_code	
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
            , subscription_api_id
            , min(version_started_at) as version_started_at_ts
            , max(version_ended_at_clean) as version_ended_at_ts
            , to_date(min(version_started_at)) as version_started_at
            , to_date(max(version_ended_at_clean)) as version_ended_at	
            
            
            from sub_history_plan_clean		
            group by 1,2,3,4,5,6,7		
            order by 1,8		
    )		
		
        select 
         *
        , case when version_ended_at like '3000-01-01' then null
                else version_ended_at end as version_ended_at_clean
        , 'executed_pause' as flag
        
        from final_table		
        where subscription_state = 'paused'	and version_started_at_ts < current_date()	----- only paused status
)

, future_pause as 
(
    with last_state as 
    (
    select *, row_number() over(partition by subscription_uuid order by version_started_at desc) as row_no
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTION_HISTORY"	
    order by subscription_uuid,subscription_activated_at,version_started_at
    )
    
    select core_sub.subscription_uuid,
            'paused' as subscription_state
            ,case when plan_code ilike 'syn-wk%' and plan_code ilike '%tuc%' then 'syn-wk'
                    else plan_code end as updated_plan_code	
            , to_date(min(subscription_activated_at) over (partition by last_state.subscription_uuid)) as subscription_activated_date	
            , to_date(max(subscription_expires_at) over (partition by last_state.subscription_uuid)) as subscription_expires_date
            , 1000 as final_rank
            ,core_sub.subscription_id as subscription_api_id
            , core_sub.current_period_ends_at as version_started_at_ts
            , Null as version_ended_at_ts
            ,to_date(version_started_at_ts) as version_started_at
            ,NULL as version_ended_at
            ,NULL as version_ended_at_clean
            ,'future_pause' as pause_flag
            
    from "MARKETING_DATABASE"."SEED_CORE_PUBLIC"."SEED_ECOMMERCE_SUBSCRIPTION" as core_sub
        left join last_state on core_sub.subscription_id = last_state.subscription_api_id
    where last_state.row_no = 1 and core_sub.subscription_state = 'PA' and core_sub.current_period_ends_at >= current_date()

)
, all_pause as 
(
    select * from subscription_pause_history
    union all 
    select * from future_pause
    order by 1,8
)

, paused_at as 
(
    select subscription_id as subscription_api_id,
        max(timestamp) as paused_at_ts,
        to_date(paused_at_ts) as paused_at
    from "SEGMENT_EVENTS"."CORE_STAGING"."SUBSCRIPTION_PAUSED"
    group by 1
)


select  SUBSCRIPTION_UUID,
	SUBSCRIPTION_STATE,
	UPDATED_PLAN_CODE,
	SUBSCRIPTION_ACTIVATED_DATE,
	SUBSCRIPTION_EXPIRES_DATE,
	FINAL_RANK,
    paused_at_ts as paused_at_ts_from_site,
    paused_at as paused_at_from_site,
	VERSION_STARTED_AT_TS,
	VERSION_ENDED_AT_TS,
	VERSION_STARTED_AT,
	VERSION_ENDED_AT,
	VERSION_ENDED_AT_CLEAN,
    coalesce(paused_at,version_started_at) as paused_at_clean,
    coalesce(paused_at_ts,version_started_at_ts) as paused_at_clean_ts,
    FLAG 
    from all_pause 
         left join paused_at on all_pause.subscription_api_id = paused_at.subscription_api_id
        ;