create or replace view MARKETING_DATABASE.PRODUCTION_VIEWS.NEWTESTVIEWTHREE as (
    select v.*, a.audience_name, a.audience_id, test_group
    FROM "MARKETING_DATABASE"."PRODUCTION_VIEWS"."NEWTESTVIEW" as v
    left join "MARKETING_DATABASE"."COHORTPOC"."ALL_EXPERIMENT_GROUPS" as a on a.email = v.email
);