create or replace view MARKETING_DATABASE.PUBLIC.V_ACCOUNT_DAILY_RETENTION_V_01(
	CUSTOMER_EMAIL,
	DS01_ACTIVE_FLAG,
	DS_01_START_DATE,
	DS01_ACTIVE_DATE,
    DS01_QUANTITY,
    PDS08_ACTIVE_FLAG,
    PDS_08_START_DATE,
    PDS08_ACTIVE_DATE,
    PDS08_QUANTITY,
    ACCOUNT_ACTIVE_FLAG,
    ACCOUNT_START_DATE,
    ACCOUNT_ACTIVE_DATE,
    ACCOUNT_QUANTITY
) as


    with ds_01_sub_table as 
    (select customer_email,case when lower(current_status) = 'canceled' then 'cancelled'
                          else lower(current_status)
                          end as current_status,
           to_date(created_at) as created_at ,coalesce(to_date(cancelled_at),current_date()) as cancelled_at, quantity,
           case when current_status = 'active' then 1
                else 0 end as active_flag
    from MARKETING_DATABASE.PUBLIC.V_SUBSCRIPTION_MAPPING_DS_01
    where current_status not in ('future','pending') and customer_email is not null and customer_email != ''
    ),

    pds_08_sub_table as
    (
    select customer_email,case when lower(current_status) = 'canceled' then 'cancelled'
                          else lower(current_status)
                          end as current_status,
           to_date(created_at) as created_at ,coalesce(to_date(cancelled_at),current_date()) as cancelled_at, quantity,
           case when current_status = 'active' then 1
                else 0 end as active_flag from 
    "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_PDS_08"
    where current_status not in ('future','pending') and customer_email is not null and customer_email != ''
    ),

    date_explode_ds_01 as 
    (
          select customer_email,quantity, created_at as active_dt, cancelled_at  
          from ds_01_sub_table
          union all
          select customer_email, quantity, dateadd(day, 1, active_dt), cancelled_at  
          from date_explode_ds_01
          where active_dt <= cancelled_at
         ),

     date_explode_pds_08 as 
    (
          select customer_email,quantity, created_at as active_dt, cancelled_at  
          from pds_08_sub_table
          union all
          select customer_email, quantity, dateadd(day, 1, active_dt), cancelled_at  
          from date_explode_pds_08
          where active_dt <= cancelled_at
         ),   


    cus_info_ds_01 as
    (
        select customer_email, min(created_at) as first_charge_dt_ds_01, max(active_flag) as active_flag_ds_01
        from ds_01_sub_table
        group by customer_email 
    ),

    cus_info_pds_08 as
    (
        select customer_email, min(created_at) as first_charge_dt_pds_08, max(active_flag) as active_flag_pds_08
        from pds_08_sub_table
        group by customer_email 
    ),

    all_raw_data_pds_08 as(

    select de.customer_email,cus_info_pds_08.first_charge_dt_pds_08 as pds_08_start_date,active_flag_pds_08,quantity as quantity_pds08,active_dt as pds08_active_date,datediff(day,first_charge_dt_pds_08,active_dt) as active_day_pds08
    from date_explode_pds_08 as de left join cus_info_pds_08 on de.customer_email = cus_info_pds_08.customer_email
    ),

    all_raw_data_ds_01 as(

    select de.customer_email,cus_info_ds_01.first_charge_dt_ds_01 as ds_01_start_date,active_flag_ds_01,quantity as quantity_ds01,active_dt as ds01_active_date,datediff(day,first_charge_dt_ds_01,active_dt) as active_day_ds01
    from date_explode_ds_01 as de left join cus_info_ds_01 on de.customer_email = cus_info_ds_01.customer_email
    ),


    ds_01_final_table as(
    select customer_email as ds01_customer_email,active_flag_ds_01 ,ds_01_start_date,ds01_active_date, sum(quantity_ds01) as ds_01_quantity 
    from all_raw_data_ds_01
    group by customer_email,active_flag_ds_01,ds_01_start_date,ds01_active_date
    order by customer_email,active_flag_ds_01,ds_01_start_date,ds01_active_date
    ),

    pds_08_final_table as(
    select customer_email as pds08_customer_email,active_flag_pds_08 ,pds_08_start_date,pds08_active_date, sum(quantity_pds08) as pds_08_quantity 
    from all_raw_data_pds_08
    group by customer_email,active_flag_pds_08,pds_08_start_date,pds08_active_date
    order by customer_email,active_flag_pds_08,pds_08_start_date,pds08_active_date
    )

    select coalesce(ds01_customer_email,pds08_customer_email) as customer_email,
           coalesce(active_flag_ds_01,0) as ds01_active_flag,ds_01_start_date,ds01_active_date,coalesce(ds_01_quantity,0) as ds01_quantity,
           coalesce(active_flag_pds_08,0) as pds08_active_flag,pds_08_start_date,pds08_active_date,coalesce(pds_08_quantity,0) as pds08_quantity,
           case 
                when active_flag_ds_01 = 1 or active_flag_pds_08 = 1 then 1
                 else 0 
                 end as account_active_flag,
           least(coalesce(ds_01_start_date,'2050-01-01'),coalesce(pds_08_start_date,'2051-01-01')) as account_start_date, 
           coalesce(ds01_active_date,pds08_active_date) as account_active_date,
           (ds01_quantity + pds08_quantity) as account_quantity       

    from ds_01_final_table as ds full join pds_08_final_table as pds on ds.ds01_customer_email = pds.pds08_customer_email and ds.ds01_active_date = pds.pds08_active_date
    order by customer_email,account_active_date