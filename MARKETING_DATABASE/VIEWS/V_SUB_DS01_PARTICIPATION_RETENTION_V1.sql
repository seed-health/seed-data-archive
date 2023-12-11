create or replace view MARKETING_DATABASE.PUBLIC.V_SUB_DS01_PARTICIPATION_RETENTION_V1(
	RECHARGE_SUBSCRIPTION_ID,
	RECURLY_SUBSCRIPTION_ID,
	SUBSCRIPTION_ID,
	FIRST_CHARGED_DATE,
	TOTAL_QUANTITY,
	CHARGED_DATE,
	CHARGED_QUANTITY
) as


with all_gain_loss as
(
    select distinct 
        recharge_subscription_id,
        recurly_subscription_id,
        case when RECHARGE_SUBSCRIPTION_ID is null then RECURLY_SUBSCRIPTION_ID
            else RECHARGE_SUBSCRIPTION_ID
            end as subscription_id,
        cohort.created_at as first_charged_date,
        cohort.quantity as total_quantity 
    from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_DS_01" as cohort
    
),
        
recurly_charges_adjusted as
(
    with recurly as
    (
        select to_varchar(s.uuid) as subscription_id,
            t.date as charged_date,
            adjustment_plan_code as sku,
            adjustment_quantity as charged_quantity
        from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
        join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
        left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s on t.subscription_id = s.uuid
        where t.type = 'purchase'
            and t.status = 'success'
            and adjustment_description not ilike '%shipping%'
            and (s.plan_name ilike '%DS-01%' or s.plan_name ilike 'Daily Synbiotic%')
            and adjustment_plan_code not ilike 'pds%'
 
    ),
    recurly_charges as
    (
        select *
        from recurly

        union all
        
        select subscription_id, DATEADD(day, 30, charged_date) as charged_date, sku, charged_quantity
        from recurly
        where sku ilike '%2mo%' or sku ilike '%3mo%' or sku ilike '%6mo%'

        union all
        
        select subscription_id, DATEADD(day, 60, charged_date) as charged_date, sku, charged_quantity
        from recurly
        where sku ilike '%3mo%' or sku ilike '%6mo%'

        union all
        
        select subscription_id, DATEADD(day, 90, charged_date) as charged_date, sku, charged_quantity
        from recurly
        where sku ilike '%6mo%'

        union all
        
        select subscription_id, DATEADD(day, 120, charged_date) as charged_date, sku, charged_quantity
        from recurly
        where sku ilike '%6mo%'

        union all
        
        select subscription_id, DATEADD(day, 150, charged_date) as charged_date, sku, charged_quantity
        from recurly
        where sku ilike '%6mo%'
        
    )
        
    select recharge_subscription_id,recurly_subscription_id,agl.subscription_id,first_charged_date,total_quantity,rcur_ch.charged_date,rcur_ch.charged_quantity
    from all_gain_loss as agl
    left join recurly_charges as rcur_ch on agl.recurly_subscription_id = rcur_ch.subscription_id
),

recharge_charges_adjusted as
(
    with recharge as
    (
        select to_varchar(s.id) as subscription_id, 
            c.processed_at as charged_date, 
            cli.sku as sku,
            cli.quantity as charged_quantity  
        from "MARKETING_DATABASE"."RECHARGE"."CHARGE" as c
        join "MARKETING_DATABASE"."RECHARGE"."CHARGE_LINE_ITEM" as cli on cli.charge_id = c.id
        join "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" s on cli.subscription_id = s.id
    ),
    recharge_charges as
    (
        select *
        from recharge

        union all
        
        select subscription_id, DATEADD(day, 30, charged_date) as charged_date, sku, charged_quantity
        from recharge
        where sku ilike '%2mo%' or sku ilike '%3mo%' or sku ilike '%6mo%'

        union all
        
        select subscription_id, DATEADD(day, 60, charged_date) as charged_date, sku, charged_quantity
        from recharge
        where sku ilike '%3mo%' or sku ilike '%6mo%'

        union all
        
        select subscription_id, DATEADD(day, 90, charged_date) as charged_date, sku, charged_quantity
        from recharge
        where sku ilike '%6mo%'

        union all
        
        select subscription_id, DATEADD(day, 120, charged_date) as charged_date, sku, charged_quantity
        from recharge
        where sku ilike '%6mo%'

        union all
        
        select subscription_id, DATEADD(day, 150, charged_date) as charged_date, sku, charged_quantity
        from recharge
        where sku ilike '%6mo%'
    )

    select recharge_subscription_id,recurly_subscription_id,agl.subscription_id,first_charged_date,total_quantity,rchr_ch.charged_date,rchr_ch.charged_quantity
    from all_gain_loss as agl
    left join recharge_charges as rchr_ch on agl.recharge_subscription_id = rchr_ch.subscription_id
    
)

select * from recurly_charges_adjusted
where charged_date is null or charged_date <= current_date
union all 
select * from recharge_charges_adjusted
where charged_date is null or charged_date <= current_date;