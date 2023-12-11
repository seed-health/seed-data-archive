create or replace view MARKETING_DATABASE.PUBLIC.RETENTION_PLAN(
	SUBSCRIPTION_UUID,
	VERSION_1_PLAN_CODE,
	VERSION_1_START_DATE,
	VERSION_1_END_DATE,
	VERSION_2_PLAN_CODE,
	VERSION_2_START_DATE,
	VERSION_2_END_DATE,
	VERSION_3_PLAN_CODE,
	VERSION_3_START_DATE,
	VERSION_3_END_DATE,
	VERSION_4_PLAN_CODE,
	VERSION_4_START_DATE,
	VERSION_4_END_DATE,
	SUBSCRIPTION_START_DATE,
	SUBSCRIPTION_END_DATE,
	SHIP_ADDRESS_COUNTRY,
	TOTAL_CAPSULES_ORDERED_BY_USER,
	TOTAL_CAPSULES_ORDERED_ON_BASELINE_BY_USER,
	TOTAL_CAPSULES_ORDERED_ON_STP_BY_USER,
	MEDIAN_INCOME,
	MEAN_INCOME
) as
      
      with updated_table as 
      (select *,case when plan_code like 'syn-rf-6mo' then 'syn-rf-6mo'
                  when plan_code like 'syn-rf-3mo' then 'syn-rf-3mo'
                  when plan_code like 'syn-rf-2mo' then 'syn-rf-2mo'
                  else 'syn-rf' end as updated_plan_code 
     from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTION_HISTORY"
      where plan_code not ilike '%pds%'and plan_code not ilike '%ds01-trial%'
      ),
      
      --Updating the switch as the next plan version instead of how it is in the dataset
      tags as
      (
      select *, row_number() over(partition by subscription_uuid order by version_started_at) as rank_sub,
                row_number() over(partition by subscription_uuid, updated_plan_code order by version_started_at) rank_plan,
                (rank_sub - rank_plan) as final_rank 

      from updated_table
      --where subscription_state != 'expired'
      order by subscription_uuid, version_started_at

        ),

        agg_rank_table as (
        select subscription_uuid, updated_plan_code, final_rank, min(version_started_at) as version_start_date, max(version_ended_at) as version_end_date from tags
        group by subscription_uuid, updated_plan_code, final_rank
        ),

      final_table as(
      select subscription_uuid,updated_plan_code as plan_code_final,version_start_date,version_end_date, row_number() over(partition by subscription_uuid order by version_start_date) as row_number from agg_rank_table
      order by subscription_uuid, version_start_date
        ),

      transpose_data as(
        with sub_1 as
        (
          select subscription_uuid, plan_code_final as version_1_plan_code, version_start_date as version_1_start_date, version_end_date as version_1_end_date from final_table
          where row_number = 1
        ),
        sub_2 as
        (
          select subscription_uuid, plan_code_final as version_2_plan_code, version_start_date as version_2_start_date, version_end_date as version_2_end_date from final_table
          where row_number = 2
        ),
        sub_3 as
        (
          select subscription_uuid, plan_code_final as version_3_plan_code, version_start_date as version_3_start_date, version_end_date as version_3_end_date from final_table
          where row_number = 3
        ),
        sub_4 as
        (
          select subscription_uuid, plan_code_final as version_4_plan_code, version_start_date as version_4_start_date, version_end_date as version_4_end_date from final_table
          where row_number = 4
        )
        select sub_1.subscription_uuid, version_1_plan_code, version_1_start_date, version_1_end_date, version_2_plan_code, version_2_start_date, 
               version_2_end_date,version_3_plan_code, version_3_start_date, version_3_end_date, version_4_plan_code, version_4_start_date, version_4_end_date
        from sub_1 left join sub_2 on sub_2.subscription_uuid = sub_1.subscription_uuid left join sub_3 on sub_3.subscription_uuid = sub_1.subscription_uuid left join sub_4 on sub_4.subscription_uuid = sub_1.subscription_uuid
      ),
      
      -- Add subscription information with country
     retention as(
      select transpose_data.*,s.created_at as subscription_start_date, s.canceled_at as subscription_end_date,s.ship_address_country 
      from transpose_data left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" s on s.uuid = transpose_data.subscription_uuid
      ),
      -- Adding quantity
      
     all_orders as (
                select s.uuid as subscription_uuid, to_date(t.date) as transaction_date, adjustment_quantity as quantity_ordered, 
                     case when adjustment_description ilike '%2 month%' then 2
                          when adjustment_description ilike '%3 month%' then 3
                          when adjustment_description ilike '%6 month%' then 6
                          else 1 end as description_order,
                    case when description_order = 3 then quantity_ordered*60*3
                         when description_order = 6 then quantity_ordered*60*6
                          else quantity_ordered*60 end as total_capsules_ordered,
                     row_number() over(partition by subscription_uuid order by transaction_date) as rank_order,
                     lag(transaction_date) over(partition by subscription_uuid order by transaction_date) as previous_order_date,
                     datediff(day,previous_order_date,transaction_date) as days_between_order,
                     round(days_between_order/30) as months_between_orders,
                     case when months_between_orders is null then 1
                          else months_between_orders end as cycle_flag
                from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
                join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
                left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s on t.subscription_id = s.uuid
                where t.type = 'purchase'
                and t.status = 'success'
                and adjustment_description not ilike '%shipping%' 
                and adjustment_description not like '%Replacement%'
                and (s.plan_name ilike '%DS-01%' or s.plan_name ilike 'Daily Synbiotic%')
                order by subscription_uuid, transaction_date 
      ),
      total as(
      select subscription_uuid,sum(total_capsules_ordered) as total_capsules_orderd_by_user      
      from all_orders
      group by subscription_uuid
        ),
      baseline as(
      select subscription_uuid,sum(total_capsules_ordered) as total_capsules_ordered_on_baseline_by_user      
      from all_orders
      where description_order = 1
      group by subscription_uuid
      ),
      stp as (
      select subscription_uuid,sum(total_capsules_ordered) as total_capsules_ordered_on_STP_by_user      
      from all_orders
      where description_order = 3 or description_order = 6
      group by subscription_uuid
      ),
      final_table_stp as(
      select total.*,baseline.total_capsules_ordered_on_baseline_by_user,stp.total_capsules_ordered_on_STP_by_user
      from total left join baseline on total.subscription_uuid = baseline.subscription_uuid left join stp on total.subscription_uuid = stp.subscription_uuid
       ),
       
      zip_code as(
      select s.uuid as subscription_id, left(s.SHIP_ADDRESS_ZIP,5) as zip_code
        from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s 
        --left join "MARKETING_DATABASE"."GOOGLE_SHEETS"."CENSUS_2020" as cen on s.zip_code = cen.zip_code
        where s.SHIP_ADDRESS_COUNTRY = 'US'),
        
      zip_code_merge as (
      
      select zc.*,cen.ESTIMATE_HOUSEHOLDS_MEDIAN_INCOME_DOLLARS_ as median_income, cen.ESTIMATE_HOUSEHOLDS_MEAN_INCOME_DOLLARS_ as mean_income
      from zip_code as zc left join "MARKETING_DATABASE"."GOOGLE_SHEETS"."CENSUS_2020" as cen on zc.zip_code = cen.zip_code)
      
       
       select ret.*,ftl.total_capsules_orderd_by_user,ftl.total_capsules_ordered_on_baseline_by_user,ftl.total_capsules_ordered_on_STP_by_user, zcm.median_income, zcm.mean_income
       from retention as ret left join final_table_stp as ftl on ret.subscription_uuid = ftl.subscription_uuid
       left join zip_code_merge as zcm on zcm.subscription_id = ret.subscription_uuid;