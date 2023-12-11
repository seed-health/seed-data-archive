CREATE VIEW "MARKETING_DATABASE"."DOH_GOOGLE_SHEETS_TEST".RETENTION2021OCT_24A AS with all_gain_loss as
    (
        select distinct
            /*cohort.is_recharge_native,
            cohort.is_imported,
            cohort.is_recurly_native,
            cohort.RECHARGE_SUBSCRIPTION_ID,
            cohort.RECURLY_SUBSCRIPTION_ID,*/

            recharge.subscription_id as recharge_subscription_id,
            recurly.subscription_id as recurly_subscription_id,

            -- if RECHARGE ID exists, default to RECHARGE FIRST_CHARGED_DATE
            case when recharge.subscription_id is not null then
                recharge.first_charged_date
            else
                recurly.first_charged_date
            end as first_charged_date,

            -- if RECURLY ID exists, default to RECURLY CANCELLED_AT
            case when recurly.subscription_id is not null then
                recurly.canceled_at
            else
                recharge.cancelled_at
            end as cancelled_at,

            -- if RECURLY ID exists, default to RECURLY QUANTITY
            case when recurly.subscription_id is not null then
                recurly.total_quantity
            else
                recharge.total_quantity
            end as total_quantity,  
            recurly.total_quantity as recurly_total_quantity,
            recharge.total_quantity as recharge_total_quantity

        from (select a.email, b.*
                from "MARKETING_DATABASE"."DOH_GOOGLE_SHEETS_TEST"."RET_2021_OCT_24_A" as a
                join "MARKETING_DATABASE"."PUBLIC"."V_MERGEDRECHARGEANDRECURLYSUBSCRIPTIONS_FORCOHORTS_V2.1" as b on a.email = customer_email) as cohort

        left join
        (
            select to_varchar(s.id) as subscription_id, 
                    min(c.processed_at) as first_charged_date, 
                    s.quantity as total_quantity,
                    s.cancelled_at
            from "MARKETING_DATABASE"."RECHARGE"."CHARGE" as c
            join "MARKETING_DATABASE"."RECHARGE"."CHARGE_LINE_ITEM" as cli on c.id = cli.charge_id
            left join "MARKETING_DATABASE"."RECHARGE"."CHARGE_SHIPPING_LINE" csl on csl.charge_id = c.id
            join "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" s on cli.subscription_id = s.id
            where c.status = 'SUCCESS'
            and (ifnull(c.total_price,0.0) - ifnull(c.total_tax,0.0) - ifnull(csl.price,0.0)) > 0
            group by to_varchar(s.id), s.quantity, s.cancelled_at
            order by to_varchar(s.id)

        ) as recharge on lower(to_varchar(recharge.subscription_id)) = lower(to_varchar(cohort.recharge_subscription_id))

        left join 
        (
            select to_varchar(s.uuid) as subscription_id, 
                    min(t.date) as first_charged_date, 
                    s.quantity as total_quantity,
                    s.canceled_at
            from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a
            join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s on s.uuid = a.subscription_id
            join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" i on a.invoice_id = i.id
            join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" t on t.invoice_id = i.id
            where a.invoice_type in ('purchase', 'renewal')
            and i.status in ('closed', 'paid')
            and t.type = 'purchase' and t.status = 'success' and t.test = 'FALSE'
            and a.adjustment_product_code != 'free_shipping'
            and (ifnull(t.amount,0) - ifnull(a.adjustment_tax,0) - ifnull(a.adjustment_discount,0)) > 0
            group by to_varchar(s.uuid), s.quantity, s.canceled_at
            order by to_varchar(s.uuid)          

        ) as recurly on lower(to_varchar(recurly.subscription_id)) = lower(to_varchar(cohort.recurly_subscription_id))
       
    )

    --select count(*) from all_gain_loss -- 14354 versus 187687
    --select count(*)
    --from "MARKETING_DATABASE"."PUBLIC"."V_MERGEDRECHARGEANDRECURLYSUBSCRIPTIONS_FORCOHORTS_V2.1" as cohort -- 195,693
    --where CUSTOMER_EMAIL in (select email from "MARKETING_DATABASE"."DOH_GOOGLE_SHEETS_TEST"."RETENTION_2021_OCTOBER_LOYALTY_A") -- 14,545
    -- select initial_gain.*
    
    
    select *
    from
    (

        select left(created_at, 7) as cohort_id, sum(total_quantity) as total_quantity
        from
        (

              -- The guts
              select *
              from
              (
                      -- Mapping Table Data
                      select to_varchar(map.RECURLY_SUBSCRIPTION_ID) as recurly_id,
                              case when dave.RECHARGE_SUBSCRIPTION_ID is null then to_date(map.first_charged_date) else to_date(dave.REVISED_CREATED_AT) end as created_at,
                              to_date(map.cancelled_at) as cancelled_at,
                              map.total_quantity
                      from all_gain_loss map
                      -- left join "MARKETING_DATABASE"."GOOGLE_SHEETS"."MAPPING_TABLE_DAVE_ADJUSTMENTS" dave on dave.RECHARGE_SUBSCRIPTION_ID = map.recharge_subscription_id
                      -- left join "MARKETING_DATABASE"."GOOGLE_SHEETS"."MAPPING_TABLE_DAVE_ADJUSTMENTS_FEB_16" dave on dave.RECHARGE_SUBSCRIPTION_ID = map.recharge_subscription_id
                      left join "MARKETING_DATABASE"."GOOGLE_SHEETS"."MAPPING_TABLE_DAVE_ADJUSTMENTS_FEB_16" dave on dave.RECHARGE_SUBSCRIPTION_ID = map.recharge_subscription_id
                
                      /*union all

                      -- Recharge Manual Sheet Data (RERUN DATES)
                      select to_varchar(rec.customer_id) as recurly_id, 
                              to_date(rec.REVISED_CREATED_AT) as created_at,
                              to_date(rec.REVISED_CANCELLED_AT) as cancelled_at,
                              rec.quantity
                      from "MARKETING_DATABASE"."GOOGLE_SHEETS"."RECHARGE_INPUTS" rec*/

                      /*union all

                      -- OceanX Manual Sheet Data
                      select to_varchar(ocx.Membership_ID) as recurly_id, 
                              to_date(ocx.Membership_Start_Date) as created_at,
                              to_date(ocx.Membership_End_Date) as cancelled_at,
                              ocx.Membership_Count
                      from "MARKETING_DATABASE"."GOOGLE_SHEETS"."OCX_INPUTS" ocx*/

              )  as a
              
              where to_varchar(recurly_id) not in 
              (
                select case when to_varchar(recurly_subscription_id) is null then '-1' else to_varchar(recurly_subscription_id) end as id from "MARKETING_DATABASE"."PUBLIC"."V_ZERO_LTR_SUBSCRIPTIONS"
                union all
                select case when to_varchar(recharge_subscription_id) is null then '-1' else to_varchar(recharge_subscription_id) end as id from "MARKETING_DATABASE"."PUBLIC"."V_ZERO_LTR_SUBSCRIPTIONS"
              )

        ) as _cohort
        group by cohort_id

    ) as initial_gain    
    join 
    (
      select *
      from 
      (


        select left(created_at, 7) as cohort_id_losses, total_quantity, left(cancelled_at, 7) as cancelled_at_month
        from
        (
              -- The guts
              select *
              from
              (
                      -- Mapping Table Data
                      select to_varchar(map.RECURLY_SUBSCRIPTION_ID) as recurly_id,
                              case when dave.RECHARGE_SUBSCRIPTION_ID is null then to_date(map.first_charged_date) else to_date(dave.REVISED_CREATED_AT) end as created_at,
                              to_date(map.cancelled_at) as cancelled_at,
                              map.total_quantity
                      from all_gain_loss map
                      -- left join "MARKETING_DATABASE"."GOOGLE_SHEETS"."MAPPING_TABLE_DAVE_ADJUSTMENTS" dave on dave.RECHARGE_SUBSCRIPTION_ID = map.recharge_subscription_id
                      --left join "MARKETING_DATABASE"."GOOGLE_SHEETS"."MAPPING_TABLE_DAVE_ADJUSTMENTS_FEB_16" dave on dave.RECHARGE_SUBSCRIPTION_ID = map.recharge_subscription_id
                      left join "MARKETING_DATABASE"."GOOGLE_SHEETS"."MAPPING_TABLE_DAVE_ADJUSTMENTS_FEB_16" dave on dave.RECHARGE_SUBSCRIPTION_ID = map.recharge_subscription_id
                
                      /*union all

                      -- Recharge Manual Sheet Data (RERUN DATES)
                      select to_varchar(rec.customer_id) as recurly_id, 
                              to_date(rec.REVISED_CREATED_AT) as created_at,
                              to_date(rec.REVISED_CANCELLED_AT) as cancelled_at,
                              rec.quantity
                      from "MARKETING_DATABASE"."GOOGLE_SHEETS"."RECHARGE_INPUTS" rec*/

                      /*union all

                      -- OceanX Manual Sheet Data
                      select to_varchar(ocx.Membership_ID) as recurly_id, 
                              to_date(ocx.Membership_Start_Date) as created_at,
                              to_date(ocx.Membership_End_Date) as cancelled_at,
                              ocx.Membership_Count
                      from "MARKETING_DATABASE"."GOOGLE_SHEETS"."OCX_INPUTS" ocx*/

              )  as a
              where to_varchar(recurly_id) not in 
              (
                select case when to_varchar(recurly_subscription_id) is null then '-1' else to_varchar(recurly_subscription_id) end as id from "MARKETING_DATABASE"."PUBLIC"."V_ZERO_LTR_SUBSCRIPTIONS"
                union all
                select case when to_varchar(recharge_subscription_id) is null then '-1' else to_varchar(recharge_subscription_id) end as id from "MARKETING_DATABASE"."PUBLIC"."V_ZERO_LTR_SUBSCRIPTIONS"
              )


        ) as _cohort    


      ) as agg
                      pivot (
                          sum(total_quantity) for cancelled_at_month in 
                          ('2018-06','2018-07','2018-08','2018-09','2018-10','2018-11','2018-12','2019-01','2019-02','2019-03','2019-04','2019-05','2019-06','2019-07','2019-08','2019-09','2019-10','2019-11','2019-12','2020-01','2020-02','2020-03','2020-04','2020-05','2020-06','2020-07','2020-08','2020-09','2020-10','2020-11','2020-12','2021-01','2021-02','2021-03','2021-04','2021-05','2021-06','2021-07','2021-08','2021-09','2021-10','2021-11')
                      )
    ) as losses on initial_gain.cohort_id  = losses.cohort_id_losses
    order by losses.cohort_id_losses;