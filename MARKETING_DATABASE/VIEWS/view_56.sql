create or replace view MARKETING_DATABASE.PUBLIC.V_COHORT_PDS08_APR_26_2022(
	COHORT_ID,
	TOTAL_QUANTITY,
	COHORT_ID_LOSSES,
	"'2022-04'"
) as

with all_gain_loss as
    (

        select distinct
            recurly_subscription_id,
            cohort.created_at as first_charged_date,
            cohort.cancelled_at as cancelled_at,
            cohort.quantity as total_quantity 
        from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_PDS_08" as cohort
        )

    select *
    from
    (

        select left(first_charged_date, 7) as cohort_id, sum(total_quantity) as total_quantity
        from all_gain_loss
        group by cohort_id

    ) as initial_gain
    
    join 
    (
      select *
      from 
      (


        select left(first_charged_date, 7) as cohort_id_losses, total_quantity, left(cancelled_at, 7) as cancelled_at_month
        from all_gain_loss
 


      ) as agg
                      pivot (
                          sum(total_quantity) for cancelled_at_month in 
                          ('2022-04')
                      )
    ) as losses on initial_gain.cohort_id  = losses.cohort_id_losses
    order by losses.cohort_id_losses;