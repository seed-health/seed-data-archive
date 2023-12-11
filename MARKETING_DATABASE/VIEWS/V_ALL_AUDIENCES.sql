create or replace view MARKETING_DATABASE.PRODUCTION_VIEWS.V_ALL_AUDIENCES as (
    select * from "MARKETING_DATABASE"."COHORTPOC"."TWO_A"
        union all
    select * from "MARKETING_DATABASE"."COHORTPOC"."TWO_B"
        union all 
    select * from "MARKETING_DATABASE"."COHORTPOC"."TWO_C"
        union all 
    select * from "MARKETING_DATABASE"."COHORTPOC"."SIX_A"
);