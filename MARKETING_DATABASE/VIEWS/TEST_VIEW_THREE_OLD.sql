create or replace view TEST_VIEW_THREE_OLD(
	RECHARGE_SUBSCRIPTION_ID,
	RECURLY_SUBSCRIPTION_ID,
	FIRST_CHARGED_DATE,
	CANCELLED_AT,
	TOTAL_QUANTITY,
	RECURLY_TOTAL_QUANTITY,
	RECHARGE_TOTAL_QUANTITY,
	CUSTOMER_EMAIL,
	PLAN_NAME
) as
        --select distinct
        select 
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
            recharge.total_quantity as recharge_total_quantity,
            customer_email,
            
            --sku plan name
            case when recurly.subscription_id is not null then
            recurly.sku
            else
                recharge.sku
            end as plan_name 
        from "MARKETING_DATABASE"."PUBLIC"."V_MERGEDRECHARGEANDRECURLYSUBSCRIPTIONS_FORCOHORTS_V2.1" as cohort

        left join
        (
            select to_varchar(s.id) as subscription_id, 
                    min(c.processed_at) as first_charged_date, 
                    s.quantity as total_quantity,
                    s.cancelled_at,
                    s.sku as sku
            from "MARKETING_DATABASE"."RECHARGE"."CHARGE" as c
            join "MARKETING_DATABASE"."RECHARGE"."CHARGE_LINE_ITEM" as cli on c.id = cli.charge_id
            left join "MARKETING_DATABASE"."RECHARGE"."CHARGE_SHIPPING_LINE" csl on csl.charge_id = c.id
            join "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" s on cli.subscription_id = s.id
            where c.status = 'SUCCESS'
            and (ifnull(c.total_price,0.0) - ifnull(c.total_tax,0.0) - ifnull(csl.price,0.0)) > 0
            group by to_varchar(s.id), s.quantity, s.cancelled_at, s.sku
            order by to_varchar(s.id)

        ) as recharge on lower(to_varchar(recharge.subscription_id)) = lower(to_varchar(cohort.recharge_subscription_id))

        left join 
        (
            select to_varchar(s.uuid) as subscription_id, 
                    min(t.date) as first_charged_date, 
                    s.quantity as total_quantity,
                    s.canceled_at,
                    s.plan_code as sku
            from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a
            join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s on s.uuid = a.subscription_id
            join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" i on a.invoice_id = i.id
            join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" t on t.invoice_id = i.id
            where a.invoice_type in ('purchase', 'renewal')
            and i.status in ('closed', 'paid')
            and t.type = 'purchase' and t.status = 'success' and t.test = 'FALSE'
            and a.adjustment_product_code != 'free_shipping'
            and (ifnull(t.amount,0) - ifnull(a.adjustment_tax,0) - ifnull(a.adjustment_discount,0)) > 0
            group by to_varchar(s.uuid), s.quantity, s.canceled_at, s.plan_code
            order by to_varchar(s.uuid)          

        ) as recurly on lower(to_varchar(recurly.subscription_id)) = lower(to_varchar(cohort.recurly_subscription_id))
where recharge.first_charged_date is not null or recurly.first_charged_date is not null