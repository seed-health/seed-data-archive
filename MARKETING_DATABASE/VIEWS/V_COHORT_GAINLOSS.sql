create or replace view MARKETING_DATABASE.PRODUCTION_VIEWS.V_COHORT_GAINLOSS(
  COHORT_ID,
  TOTAL_QUANTITY,
  COHORT_ID_LOSSES,
  "'2018-06'",
  "'2018-07'",
  "'2018-08'",
  "'2018-09'",
  "'2018-10'",
  "'2018-11'",
  "'2018-12'",
  "'2019-01'",
  "'2019-02'",
  "'2019-03'",
  "'2019-04'",
  "'2019-05'",
  "'2019-06'",
  "'2019-07'",
  "'2019-08'",
  "'2019-09'",
  "'2019-10'",
  "'2019-11'",
  "'2019-12'",
  "'2020-01'",
  "'2020-02'",
  "'2020-03'",
  "'2020-04'",
  "'2020-05'",
  "'2020-06'",
  "'2020-07'",
  "'2020-08'",
  "'2020-09'",
  "'2020-10'",
  "'2020-11'",
  "'2020-12'",
  "'2021-01'",
  "'2021-02'",
  "'2021-03'",
  "'2021-04'",
  "'2021-05'",
  "'2021-06'",
  "'2021-07'",
  "'2021-08'",
  "'2021-09'",
  "'2021-10'",
  "'2021-11'",
  "'2021-12'",
  "'2022-01'",
  "'2022-02'",
  "'2022-03'"
) as

    with adjusted_gain_loss as
    (

        select *
        from
        (

            -- ADJUST: Edit V_SUBSCRIPTION_MAPPING with Dave Sylvia's manual edits.  If Dave has no adjustment, leave the original result
            select to_varchar(map.RECURLY_SUBSCRIPTION_ID) as recurly_id,
                case when dave.RECHARGE_SUBSCRIPTION_ID is null then to_date(map.first_charged_date) else to_date(dave.REVISED_CREATED_AT) end as created_at,
                to_date(map.cancelled_at) as cancelled_at,
                map.total_quantity
            from
            (
                select distinct
                    recharge_subscription_id,
                    recurly_subscription_id,
                    created_at as first_charged_date,
                    cancelled_at as cancelled_at,
                    quantity as total_quantity 
                from "MARKETING_DATABASE"."PRODUCTION_VIEWS"."V_SUBSCRIPTION_MAPPING"
            ) as map
            left join "MARKETING_DATABASE"."GOOGLE_SHEETS"."MAPPING_TABLE_DAVE_ADJUSTMENTS_FEB_16" dave on dave.RECHARGE_SUBSCRIPTION_ID = map.recharge_subscription_id

            union all

            -- IMPORT: Manual data from Recharge
            select to_varchar(rec.customer_id) as recurly_id, 
                    to_date(rec.REVISED_CREATED_AT) as created_at,
                    to_date(rec.REVISED_CANCELLED_AT) as cancelled_at,
                    rec.quantity
            from "MARKETING_DATABASE"."GOOGLE_SHEETS"."RECHARGE_INPUTS" rec

            union all

            -- IMPORT: Manual data from OceanX
            select to_varchar(ocx.Membership_ID) as recurly_id, 
                    to_date(ocx.Membership_Start_Date) as created_at,
                    to_date(ocx.Membership_End_Date) as cancelled_at,
                    ocx.Membership_Count
            from "MARKETING_DATABASE"."GOOGLE_SHEETS"."OCX_INPUTS" ocx
        )
        where to_varchar(recurly_id) not in 
        (
            select case when to_varchar(recurly_subscription_id) is null then '-1' else to_varchar(recurly_subscription_id) end as id from "MARKETING_DATABASE"."PRODUCTION_VIEWS"."V_ZERO_LTR_SUBSCRIPTIONS_PRE_JAN_2021"
            union all
            select case when to_varchar(recharge_subscription_id) is null then '-1' else to_varchar(recharge_subscription_id) end as id from "MARKETING_DATABASE"."PRODUCTION_VIEWS"."V_ZERO_LTR_SUBSCRIPTIONS_PRE_JAN_2021"
        )

    )
    
    -- Join Cohort Month + Total Initial Subscriptions + Total Losses for each Month in history
    select *
    from
    (
        -- Calculate: Total initial subscriptions for each cohort
        select left(created_at, 7) as cohort_id, sum(total_quantity) as total_quantity
        from adjusted_gain_loss
        group by cohort_id

    ) as initial_gain
    join 
    (
        -- Calculate: Total losses for each cohort, for every month in history.  PIVOT() accounts for each month explicitly and must be updated manually each month
        select *
        from 
        (
            select left(created_at, 7) as cohort_id_losses, total_quantity, left(cancelled_at, 7) as cancelled_at_month
            from adjusted_gain_loss
        ) as agg
        pivot (
            sum(total_quantity) for cancelled_at_month in 
            ('2018-06','2018-07','2018-08','2018-09','2018-10','2018-11','2018-12','2019-01','2019-02','2019-03','2019-04','2019-05','2019-06','2019-07','2019-08','2019-09','2019-10','2019-11','2019-12','2020-01','2020-02','2020-03','2020-04','2020-05','2020-06','2020-07','2020-08','2020-09','2020-10','2020-11','2020-12','2021-01','2021-02','2021-03','2021-04','2021-05','2021-06','2021-07','2021-08','2021-09','2021-10','2021-11','2021-12','2022-01','2022-02','2022-03')
        )
    ) as losses on initial_gain.cohort_id  = losses.cohort_id_losses
    order by losses.cohort_id_losses

;