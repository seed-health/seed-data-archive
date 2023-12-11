create or replace view "MARKETING_DATABASE"."PUBLIC"."V_PILOT_DASHBOARD"
as

    select charges.*, zeroifnull(refunds.total_refund) as total_refund
    from 
    (
        select recurly_non_shipping.day as day, 
            recurly_non_shipping.sku as sku,
            zeroifnull(sum(recurly_shipping.shipping_amount)) as shipping_amount,                      
            zeroifnull(sum(recurly_non_shipping.tax_amount)) as tax_amount,
            zeroifnull(sum(recurly_non_shipping.discount_amount)) as discount_amount,
            zeroifnull(sum(recurly_non_shipping.total_charge_quantity)) as total_charge_quantity,
            zeroifnull(sum(recurly_non_shipping.total_subscription_quantity)) as total_subscription_quantity,
            zeroifnull(sum(recurly_non_shipping.price_total)) as price_total,
            zeroifnull(sum(recurly_non_shipping.price_subtotal)) as price_subtotal,
            zeroifnull(sum(recurly_non_shipping.price_total)) - zeroifnull(sum(recurly_non_shipping.tax_amount)) - zeroifnull(sum(recurly_shipping.shipping_amount)) + zeroifnull(sum(recurly_non_shipping.discount_amount)) as gross_revenue


        from
        (

              -- Non-Shipping Adjustments
              select to_date(i.closed_at) as day, 
                     i.id,
                     a.adjustment_description as sku,
                    sum(distinct i.tax_amount) as tax_amount,
                    0 as shipping_amount,
                    sum(distinct i.invoice_discount) as discount_amount,
                    count(distinct i.id) as total_charge_quantity,
                    sum(a.adjustment_quantity) as total_subscription_quantity,
                    sum(distinct i.invoice_total) as price_total,
                    sum(distinct i.invoice_subtotal) as price_subtotal                  
              from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a
              join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as i on i.id = a.invoice_id
              where i.status in ('paid')
              and i.invoice_doc_type in ('charge') 
              and adjustment_description not ilike '%shipping%'
              group by i.id, day, sku

          ) as recurly_non_shipping
          left join 
          (
              -- Shipping Adjustments            
              select to_date(i.closed_at) as day, 
                    i.id,
                    a.adjustment_description as sku,
                    0 as tax_amount,
                    sum(a.adjustment_total) as shipping_amount,
                    0 as discount_amount,
                    0 as total_charge_quantity,
                    0 as total_subscription_quantity,
                    0 as price_total,            
                    0 as price_subtotal                  
              from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a
              join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as i on i.id = a.invoice_id
              where i.status in ('paid')
              and i.invoice_doc_type in ('charge')
              and adjustment_description ilike '%shipping%'
              group by i.id, day, sku

          ) as recurly_shipping 
          on recurly_shipping.id = recurly_non_shipping.id 
            and recurly_shipping.day = recurly_non_shipping.day
        group by recurly_non_shipping.day, recurly_non_shipping.sku
    ) as charges
    left join
    (

        select to_date(i.closed_at) as day,
                a.adjustment_description as sku,
                sum(distinct i.invoice_total) as total_refund
        from"IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a
        join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as i on i.id = a.invoice_id
        where i.invoice_type = 'refund'
        and adjustment_description not ilike '%shipping%'
        group by day, sku

    ) as refunds on charges.day = refunds.day and charges.sku = refunds.sku
    order by charges.day