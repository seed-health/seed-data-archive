create view v_DailyGrowthTracker
as

    select -- subs.*, 
            subs.GAIN,
            subs.LOSS,
            subs.NET,
            subs.RUNNING_TOTAL,
            subs.TRAILING_30_LOSS,
            rev.*,
            avg(subs.running_total) over (order by subs.day asc rows between 29 preceding and current row) as thirty_day_avg_total_active_users,
            (
            trailing_30_loss
            /
            avg(subs.running_total) over (order by subs.day asc rows between 29 preceding and current row) ) * 100 as churn_rate
    from
    (

        with data as (  

            -- Recharge: Daily Gain vs Loss 
            select new.referenced_date as day, 
                new.total_new_quantity as gain, churned.total_churn_quantity as loss, new.total_new_quantity - churned.total_churn_quantity as net
            from
            (
                -- Total New Subscribers
                with date_list as 
                (
                    select distinct to_date(_s.created_at) as referenced_date
                    from "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" as _s
                    order by to_date(_s.created_at) desc
                )
                select dl.referenced_date, count(*) as total_new_customers, sum(quantity) as total_new_quantity
                from date_list dl
                join "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" as s on dl.referenced_date = to_date(dateadd(hour, -4, s.created_at))
                group by dl.referenced_date
                order by dl.referenced_date

            ) as new
            join 
            (
                -- Total Churned Subscribers
                with date_list as 
                (
                    select distinct to_date(_s.created_at) as referenced_date
                    from "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" as _s
                    order by to_date(_s.created_at) desc
                )
                select dl.referenced_date, count(*) as total_churned_customers, sum(quantity) as total_churn_quantity
                from date_list dl
                join "MARKETING_DATABASE"."RECHARGE"."SUBSCRIPTION" as s on dl.referenced_date = to_date(dateadd(hour, -4, s.cancelled_at))
                group by dl.referenced_date
                order by dl.referenced_date

            ) as churned on new.referenced_date = churned.referenced_date
            order by new.referenced_date
        )

        select day, gain, loss, net,
                sum(net) over (order by day asc rows between unbounded preceding and current row) as running_total,
                sum(loss) over (order by day asc rows between 29 preceding and current row) as trailing_30_loss
        from data
    ) as subs  
    join 
    (
      -- Shopify: Daily Revenue Numbers
      select to_date(dateadd(hour, -4, o.created_at)) as day, 
          sum(zeroifnull(o.total_price) - zeroifnull(osl.price) - zeroifnull(o.total_tax) - zeroifnull(o.total_discounts)) as net_revenue,
          sum(o.total_tax) as tax_amount,
          sum(osl.price) as shipping_amount,
          sum(o.total_discounts) as discount_amount,
          count(*) as total_order_quantity,
          avg(o.total_price) as aov,
          sum(o.total_price) as shopify_total_price,
          sum(o.subtotal_price) as shopify_subtotal_price
      from "MARKETING_DATABASE"."SHOPIFY"."ORDER" o
      left join "MARKETING_DATABASE"."SHOPIFY"."ORDER_SHIPPING_LINE" osl on osl.order_id = o.id
      group by to_date(dateadd(hour, -4, o.created_at))
      order by to_date(dateadd(hour, -4, o.created_at))  

    ) as rev
    on rev.day = subs.day
    order by rev.day desc