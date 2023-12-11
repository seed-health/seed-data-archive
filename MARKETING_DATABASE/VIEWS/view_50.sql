create or replace view MARKETING_DATABASE.PUBLIC.V_ACCOUNT_MONTHLY_RETENTION_PDS08_MAR_31_2023(
	CUSTOMER_EMAIL,
	START_MONTH_YEAR,
	START_QUANTITY,
	CUSTOMER_EMAIL_RETENTION,
	ACTIVE_AT_EOMY,
	PDS_08_QUANTITY
) as
    
    with pds_08_sub_table as
        (
        select RECURLY_SUBSCRIPTION_ID as subscription_id, customer_email,case when lower(current_status) = 'canceled' then 'cancelled'
                              else lower(current_status)
                              end as current_status,
               to_date(created_at) as created_at ,coalesce(to_date(cancelled_at),current_date()) as cancelled_at, quantity,
               case when current_status = 'active' then 1
                    else 0 end as active_flag from 
        "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_PDS_08"

        where current_status not in ('future','pending') and customer_email is not null and customer_email != ''
        ),


         cus_info_pds_08 as
        ( 
          -- Cleaning for multiple DS_01 subs started on the same day
         with pds_08_sub_agg as 
          (
            select customer_email,created_at,sum(quantity) as quantity
            from pds_08_sub_table
            group by customer_email,created_at
          ),

          pds_08_first_sub as
          (
            select *,row_number() over(partition by customer_email order by created_at) as rw_no
            from pds_08_sub_agg
          )

         select customer_email,left(created_at,7) as start_month_year, quantity
         from pds_08_first_sub
         where rw_no = 1
        ),


           date_explode_pds_08 as 
        (
              select subscription_id,customer_email,quantity,created_at ,created_at as active_dt, cancelled_at  
              from pds_08_sub_table
              union all
              select subscription_id,customer_email, quantity,created_at ,dateadd(month, 1, active_dt), cancelled_at 
              from date_explode_pds_08
              where (month(active_dt) < month(cancelled_at)) or (year(active_dt) < year(cancelled_at))
             ),   




        active_dates_pds_08 as
        (

        select *, left(lag(active_dt,1) over(partition by subscription_id order by active_dt),7) as active_at_eomy
        from date_explode_pds_08 as de
        ),

        retention_data_pds_08 as (
        select customer_email as customer_email_retention,active_at_eomy, sum(quantity) as pds_08_quantity
        from active_dates_pds_08
        where active_at_eomy is not null
        group by customer_email,active_at_eomy
        order by customer_email,active_at_eomy
        ),/* If you want pivot data for excel in the future

        retention_pivot_ds_01 as(
        select * from retention_data_ds_01
        pivot (
                              sum(ds_01_quantity) for active_at_eomy in 
                              ('2018-06','2018-07','2018-08','2018-09','2018-10','2018-11','2018-12','2019-01','2019-02','2019-03','2019-04','2019-05','2019-06','2019-07','2019-08','2019-09','2019-10','2019-11','2019-12','2020-01','2020-02','2020-03','2020-04','2020-05','2020-06','2020-07','2020-08','2020-09','2020-10','2020-11','2020-12','2021-01','2021-02','2021-03','2021-04','2021-05','2021-06','2021-07','2021-08','2021-09','2021-10','2021-11','2021-12','2022-01','2022-02','2022-03','2022-04','2022-05','2022-06','2022-07','2022-08','2022-09','2022-10','2022-11','2022-12','2023-01','2023-02')
                          )
        ),*/

        acc_pds08_ret_cust as 
        (
        select pds_start.customer_email,pds_start.start_month_year,pds_start.quantity as start_quantity, pds_ret.*
        from cus_info_pds_08 as pds_start
        left join retention_data_pds_08 as pds_ret on pds_ret.customer_email_retention = pds_start.customer_email
        order by start_month_year
        )

        select * from acc_pds08_ret_cust;