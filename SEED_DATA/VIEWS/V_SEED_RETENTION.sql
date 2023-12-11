create or replace view SEED_DATA.DEV.V_SEED_RETENTION(
	ACTIVE_MONTH_YEAR,
	ACTIVE_SUBS_QUANTITY,
	CANCELLED_SUBS_QUANTITY,
	PAUSED_SUBS_QUANTITY,
	BILLED_REFILL_QUNATITY,
	BILLED_REFILL_2MO_QUNATITY,
	BILLED_REFILL_3MO_QUNATITY,
	BILLED_REFILL_6MO_QUNATITY,
	RETAINED_REFILL_2MO_QUNATITY,
	RETAINED_REFILL_3MO_QUNATITY,
	RETAINED_REFILL_6MO_QUNATITY
) as 

    with date_range as 
    (
        select 
            date
            from seed_data.dev.dim_date
            where year in (2022,2023) and month <= 12  and day = 1
    ),
    
    cohort as 
    (
    select *
    from "SEED_DATA"."DEV"."V_SUBSCRIPTION_MASTER" as s
        cross join date_range as dr
    where to_date(activated_at) < date and (to_date(cancelled_at) is null or to_date(cancelled_at) >= date)
        and recurly_subscription_id is not null -- There are 514 recharge subscription id that shows still active and is not mapped to a recurly subscription
    ),
    
    transaction as 
    (
    select o.*, 
        case when sku ilike '%wk%' then 'Welcome Kit'
             when sku ilike '%rf' then 'Refill'
             when sku ilike '%2mo%' then 'Refill - 2 Months'
             when sku ilike '%3mo%' then 'SRP Refill - 3 Months'
             when sku ilike '%6mo%' then 'SRP Refill - 6 Months'
             else null end as sku_category,
        case when to_date(invoice_date) < dr.date then 'Retained' else 'Billed' end as bill_category,
        dr.date 
    from "SEED_DATA"."DEV"."V_ORDER_HISTORY" as o 
        cross join date_range as dr 
    where(
            (sku ilike '%wk%' and to_date(invoice_date)>= dr.date and to_date(invoice_date) < add_months(dr.date,1)) or 
            (sku ilike '%rf' and to_date(invoice_date)>= dr.date and to_date(invoice_date) < add_months(dr.date,1)) or
            (sku ilike '%2mo%' and to_date(invoice_date)>= add_months(dr.date,-1) and to_date(invoice_date) < add_months(dr.date,1)) or 
            (sku ilike '%3mo%' and to_date(invoice_date)>= add_months(dr.date,-2) and to_date(invoice_date) < add_months(dr.date,1)) or 
            (sku ilike '%6mo%' and to_date(invoice_date)>= add_months(dr.date,-5) and to_date(invoice_date) < add_months(dr.date,1))
         )
    ),
    
    cohort_agg as 
    (
    
    select date as active_month_year,
        sum(quantity) as active_subs_quantity,
        sum(case when to_date(cancelled_at) is not null and 
                      to_date(cancelled_at) >= date and 
                      to_date(cancelled_at) < add_months(date,1)
                      then quantity else 0 end) as cancelled_subs_quantity,
        sum(case when to_date(pause_start_date) is not null and
                      (to_date(pause_start_date) < add_months(date,1)) and 
                      (to_date(pause_end_date) is null or to_date(pause_end_date) >= add_months(date,1))
                      then quantity else 0 end) as paused_subs_quantity   
    from cohort
    group by 1
    ),
    
    transaction_agg as 
    (
    select c.date as active_month_year,
        sum(case when bill_category = 'Billed' and sku_category = 'Refill' then c.quantity else 0 end) as billed_refill_qunatity,
        sum(case when bill_category = 'Billed' and sku_category = 'Refill - 2 Months' then c.quantity else 0 end) as billed_refill_2mo_qunatity,
        sum(case when bill_category = 'Billed' and sku_category = 'SRP Refill - 3 Months' then c.quantity else 0 end) as billed_refill_3mo_qunatity,
        sum(case when bill_category = 'Billed' and sku_category = 'SRP Refill - 6 Months' then c.quantity else 0 end) as billed_refill_6mo_qunatity,
        sum(case when bill_category = 'Retained' and sku_category = 'Refill - 2 Months' then c.quantity else 0 end) as retained_refill_2mo_qunatity,
        sum(case when bill_category = 'Retained' and sku_category = 'SRP Refill - 3 Months' then c.quantity else 0 end) as retained_refill_3mo_qunatity,
        sum(case when bill_category = 'Retained' and sku_category = 'SRP Refill - 6 Months' then c.quantity else 0 end) as retained_refill_6mo_qunatity
    from cohort as c
    left join transaction as t on c.recurly_subscription_id = t.subscription_id and c.date = t.date
    group by 1
    ),
    
    
    final_data as 
    (
    select cohort_agg.*,
        transaction_agg.billed_refill_qunatity,
        transaction_agg.billed_refill_2mo_qunatity,
        transaction_agg.billed_refill_3mo_qunatity,
        transaction_agg.billed_refill_6mo_qunatity,
        transaction_agg.retained_refill_2mo_qunatity,
        transaction_agg.retained_refill_3mo_qunatity,
        transaction_agg.retained_refill_6mo_qunatity
    from cohort_agg 
    join transaction_agg on cohort_agg.active_month_year = transaction_agg.active_month_year
    )
    
    select *
    from final_data;