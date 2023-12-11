create or replace view "MARKETING_DATABASE"."PRODUCTION_VIEWS"."V_MAPPING_CONSOLIDATED"
as
    select *
    from
    (
        select distinct trim(to_varchar(MAPPED_RECHARGE_SUBSCRIPTION_ID)) as _RECHARGE_SUBSCRIPTION_ID, trim(to_varchar(MAPPED_RECURLY_SUBSCRIPTION_ID)) as _RECURLY_SUBSCRIPTION_ID   
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."RECHARGE_RECURLY_MAPPING"  

        union all

        select trim(to_varchar(MAPPED_RECHARGE_SUBSCRIPTION_ID)) as _RECHARGE_SUBSCRIPTION_ID, trim(to_varchar(MAPPED_RECURLY_SUBSCRIPTION_ID)) as _RECURLY_SUBSCRIPTION_ID   
        from "MARKETING_DATABASE"."GOOGLE_SHEETS"."RECHARGE_RECURLY_MAPPING_NOV_12_2020"

        union all

        select trim(to_varchar(RECHARGE_SUBSCRIPTION_ID)) as _RECHARGE_SUBSCRIPTION_ID, trim(to_varchar(RECURLY_SUBSCRIPTION_ID)) as _RECURLY_SUBSCRIPTION_ID  
        from "MARKETING_DATABASE"."RECHARGE"."RECHARGE_RECURLY_MAPPING"
      
    ) as consolidated
    where consolidated._RECHARGE_SUBSCRIPTION_ID not in ('canceled', 'cancel')
    and consolidated._RECURLY_SUBSCRIPTION_ID not in ('no need to map since it''s cancelled', 'expired', 'cancelled', 'canceled', 'cancel')
;