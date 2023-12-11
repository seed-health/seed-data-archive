create or replace view MARKETING_DATABASE.PUBLIC.V_DAILYGROWTHTRACKER_PDS_01_V_01(
	DAY,
	GAIN,
	LOSS,
	NET,
	TOTAL_ACTIVE,
	TOTAL_ACTIVE_INTERNATIONAL,
	TOTAL_ACTIVE_INTERNATIONAL_3MO_REFILL,
	TOTAL_ACTIVE_GIFTKITS,
	TOTAL_ACTIVE_GIFTCREDITS,
	TOTAL_ACTIVE_PREORDERS,
	TRAILING_30_DAY_GAIN,
	TRAILING_30_DAY_LOSS,
	GAIN_INTERNATIONAL,
	LOSS_INTERNATIONAL,
	NET_INTERNATIONAL,
	GAIN_INTERNATIONAL_3MO_REFILL,
	LOSS_INTERNATIONAL_3MO_REFILL,
	NET_INTERNATIONAL_3MO_REFILL,
	GAIN_GIFTKITS,
	LOSS_GIFTKITS,
	NET_GIFTKITS,
	GAIN_GIFTCREDITS,
	LOSS_GIFTCREDITS,
	NET_GIFTCREDITS,
	GAIN_PREORDERS,
	LOSS_PREORDERS,
	NET_PREORDERS,
	TAX_AMOUNT,
	SHIPPING_AMOUNT,
	TOTAL_DISCOUNT_AMOUNT,
	TOTAL_CHARGE_QUANTITY,
	TOTAL_SUBSCRIPTION_QUANTITY,
	AOV,
	PRICE_TOTAL,
	TOTAL_GIFTKIT_SUBSCRIPTION_QUANTITY,
	TOTAL_GIFTCREDIT_SUBSCRIPTION_QUANTITY,
	REFILL_COGS,
	WELCOME_KIT_COGS,
	GROSS_SALES,
	NET_SALES,
	CREDIT_APPLIED
) as

      -- REVENUE UNION
      with revenue as
      (

        select rev.day,
              sum(total_tax) as tax_amount,
              sum(shipping_amount) as shipping_amount,
              sum(total_discount_amount) as total_discount_amount,
              sum(total_charge_quantity) as total_charge_quantity,
              sum(total_subscription_quantity) as total_subscription_quantity,
              zeroifnull(sum(rev.price_total) - sum(rev.total_tax)) / sum(total_charge_quantity) as aov,
              sum(price_total) as price_total,
              sum(total_giftkit_subscription_quantity) as total_giftkit_subscription_quantity,
              sum(total_giftcredit_subscription_quantity) as total_giftcredit_subscription_quantity,  
              sum(zeroifnull(manual.refill_cogs)) as refill_cogs,
              sum(zeroifnull(manual.welcome_kit_cogs)) as welcome_kit_cogs,
              zeroifnull(sum(rev.price_total)) - zeroifnull(sum(rev.total_tax)) - zeroifnull(sum(rev.shipping_amount)) + zeroifnull(sum(rev.total_discount_amount)) + zeroifnull(sum(rev.credit_applied)) as gross_sales,
              zeroifnull(sum(rev.price_total)) - zeroifnull(sum(rev.total_tax)) as net_sales,
              zeroifnull(sum(credit_applied)) as credit_applied

          from 
          (
                -- ***** START - RECURLY Daily Revenue ***** --  
                select charge_level_revenue.day, 
                        price_total as price_total, 
                        total_discount_amount as total_discount_amount, 
                        total_charge_quantity as total_charge_quantity,
                        credit_applied as credit_applied,
                        total_subscription_quantity as total_subscription_quantity,    
                        tax_amount as total_tax,
                        zeroifnull(shipping_amount) as shipping_amount,
                        zeroifnull(total_giftkit_subscription_quantity) as total_giftkit_subscription_quantity,
                        zeroifnull(total_giftcredit_subscription_quantity) as total_giftcredit_subscription_quantity
                from
                (
                    select nonship_adj.day as day,
                        nonship_adj.tax as tax_amount,
                        shipping_adj.shipping as shipping_amount,
                        credit.credit_amount as credit_applied,
                        nonship_adj.discount_amount as total_discount_amount,
                        nonship_adj.total_charges as total_charge_quantity,
                        nonship_adj.quantity as total_subscription_quantity,
                        nonship_adj.total_price as price_total
                    from
                    (
                        select to_date(t.date) as day,
                                sum(adjustment_discount) as discount_amount,
                                sum(t.tax_amount) as tax,
                                sum(adjustment_total) as adj_total_price,
                                sum(t.amount) as total_price,  
                                sum(adjustment_quantity) as quantity,
                                count(*) as total_charges
                        from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
                        join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
                        left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s on t.subscription_id = s.uuid
                        where t.type = 'purchase'
                            and t.status = 'success'
                            and adjustment_description not ilike '%shipping%'
                            and (s.plan_name ilike '%PDS-08%' or s.plan_name ilike 'Pediatric Daily Synbiotic%')
                        group by day
                        order by day

                    ) as nonship_adj 
                    left join
                    ( 

                        -- Shipping totals will NEVER match up to DGT
                        select to_date(t.date) as day,
                            sum(adjustment_total) as shipping
                        from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t
                        join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a on t.invoice_id = a.invoice_id
                        left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s on t.subscription_id = s.uuid
                        where t.type = 'purchase'
                            and t.status = 'success'
                            and  adjustment_description ilike '%shipping%'
                            and (s.plan_name ilike '%PDS-08%' or s.plan_name ilike 'Pediatric Daily Synbiotic%')
                        group by day
                        order by day  

                    ) as shipping_adj on nonship_adj.day = shipping_adj.day
                    
                    left join
                    (	--- Credit amount applied date
	                    select to_date(t.date) as day ,
	                    sum(c.amount) as credit_amount
	                      from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."CREDIT_PAYMENTS" as c 
                          join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as i on c.applied_to_invoice_number = i.invoice_number 
                          join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" as t on t.invoice_id = i.id
                          left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s on t.subscription_id = s.uuid
	                      where action != 'write_off' 
                                and (s.plan_name ilike '%PDS-08%' or s.plan_name ilike 'Pediatric Daily Synbiotic%')
	                      group by day
                      
                    ) as credit on nonship_adj.day = credit.day
                    order by nonship_adj.day
                ) as charge_level_revenue
                left join 
                (
                    select to_date(i.closed_at) as day, 
                          sum(a.adjustment_quantity) as total_giftkit_subscription_quantity
                    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a
                    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as i on i.id = a.invoice_id
                    left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s on a.subscription_id = s.uuid
                    where i.status in ('paid')
                    and i.invoice_doc_type in ('charge') 
                    and adjustment_description = 'Gift Kit Holiday 2020'
                    and (s.plan_name ilike '%PDS-08%' or s.plan_name ilike 'Pediatric Daily Synbiotic%')
                    group by day 
                ) as charge_level_revenue_giftkit 
                  on charge_level_revenue_giftkit.day = charge_level_revenue.day
                left join 
                (
                    select to_date(i.closed_at) as day, 
                          sum(a.adjustment_quantity) as total_giftcredit_subscription_quantity
                    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" as a
                    join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" as i on i.id = a.invoice_id
                    left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as s on a.subscription_id = s.uuid
                    where i.status in ('paid')
                    and i.invoice_doc_type in ('charge') 
                    and adjustment_description = 'GIFT_CREDIT_PLACEHOLDER'
                    and (s.plan_name ilike '%PDS-08%' or s.plan_name ilike 'Pediatric Daily Synbiotic%')
                    group by day 
                ) as charge_level_revenue_giftcredit
                  on charge_level_revenue_giftcredit.day = charge_level_revenue.day
                -- ***** END - RECURLY Daily Revenue ***** -- 

          ) as rev
          left join "MARKETING_DATABASE"."GOOGLE_SHEETS"."MANUAL_INPUT" manual on manual.date = rev.day
          group by rev.day
          order by rev.day
        
      ), 
      
      gainlossnet as -- GAIN LOSS JOIN
      (

          select gain.dt as day, 
                  zeroifnull(gain.total_subscribers_gained) as total_subscribers_gained, 
                  zeroifnull(loss.total_subscribers_lost) total_subscribers_lost,   
                  zeroifnull(gain.total_subscriptions_gained) as gain,
                  zeroifnull(loss.total_subscriptions_lost) as loss,
                  zeroifnull(gain.total_subscriptions_gained) - zeroifnull(loss.total_subscriptions_lost) as net,

                  -- _international
                  zeroifnull(gain.total_subscriptions_gained_international) as gain_international,
                  zeroifnull(loss.total_subscriptions_lost_international) as loss_international,
                  zeroifnull(gain.total_subscriptions_gained_international) - zeroifnull(loss.total_subscriptions_lost_international) as net_international,

                  -- _international_3mo_refill
                  zeroifnull(gain.total_subscriptions_gained_international_3mo_refill) as gain_international_3mo_refill,
                  zeroifnull(loss.total_subscriptions_lost_international_3mo_refill) as loss_international_3mo_refill,
                  zeroifnull(gain.total_subscriptions_gained_international_3mo_refill) - zeroifnull(loss.total_subscriptions_lost_international_3mo_refill) as net_international_3mo_refill,

                  -- _giftkits
                  zeroifnull(gain.total_subscriptions_gained_giftkits) as gain_giftkits,
                  zeroifnull(loss.total_subscriptions_lost_giftkits) as loss_giftkits,
                  zeroifnull(gain.total_subscriptions_gained_giftkits) - zeroifnull(loss.total_subscriptions_lost_giftkits) as net_giftkits,

                  -- _giftcredits
                  zeroifnull(gain.total_subscriptions_gained_giftcredits) as gain_giftcredits,
                  zeroifnull(loss.total_subscriptions_lost_giftcredits) as loss_giftcredits,
                  zeroifnull(gain.total_subscriptions_gained_giftcredits) - zeroifnull(loss.total_subscriptions_lost_giftcredits) as net_giftcredits,

                  -- _preorders
                  zeroifnull(gain.total_subscriptions_gained_preorders) as gain_preorders,
                  zeroifnull(loss.total_subscriptions_lost_preorders) as loss_preorders,
                  zeroifnull(gain.total_subscriptions_gained_preorders) - zeroifnull(loss.total_subscriptions_lost_preorders) as net_preorders

          from
          (

                with subscription_breakdowns as 
                (

                    select 
                        -- only perform timezone offset subscriptions created within recharge
                        to_date(created_at) as dt,
                        count(*) as total_subscribers_gained, 
                        sum(quantity) as total_subscriptions_gained,
                        is_international,
                        sku,
                        is_preorder
                    from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_PDS_08"
                    group by dt, is_international, sku, is_preorder
                    order by dt
                )            
            

                -- **************************************************
                -- BREAKDOWNS: Create aggregation + join for each breakdown
                -- **************************************************      
                select total.dt, total_subscribers_gained, total_subscriptions_gained, 
                        total_subscribers_gained_international, total_subscriptions_gained_international,
                        total_subscribers_gained_international_3mo_refill, total_subscriptions_gained_international_3mo_refill,
                        total_subscribers_gained_giftkits, total_subscriptions_gained_giftkits,
                        total_subscribers_gained_giftcredits, total_subscriptions_gained_giftcredits,
                        total_subscribers_gained_preorders, total_subscriptions_gained_preorders
                from
                (
                    select dt, sum(total_subscribers_gained) total_subscribers_gained, sum(total_subscriptions_gained) as total_subscriptions_gained
                    from subscription_breakdowns
                    group by dt
                ) as total
                left join
                (
                    select dt, sum(total_subscribers_gained) total_subscribers_gained_international, sum(total_subscriptions_gained) as total_subscriptions_gained_international
                    from subscription_breakdowns
                    where is_international = 1
                    group by dt    
                ) as total_international on total.dt = total_international.dt      
                left join
                (
                    select dt, sum(total_subscribers_gained) total_subscribers_gained_international_3mo_refill, sum(total_subscriptions_gained) as total_subscriptions_gained_international_3mo_refill
                    from subscription_breakdowns
                    where is_international = 1
                    and (sku = 'syn-rf-3mo' or sku ilike '%3mo%')
                    group by dt    
                ) as total_international_3mo_refill on total.dt = total_international_3mo_refill.dt      
                left join
                (
                    select dt, sum(total_subscribers_gained) total_subscribers_gained_giftkits, sum(total_subscriptions_gained) as total_subscriptions_gained_giftkits
                    from subscription_breakdowns              
                    where sku in ('syn-wk-gift-2020','SYN-WK-GIFT')
                    group by dt    
                ) as total_giftkits on total.dt = total_giftkits.dt
                left join
                (
                    select dt, sum(total_subscribers_gained) total_subscribers_gained_giftcredits, sum(total_subscriptions_gained) as total_subscriptions_gained_giftcredits
                    from subscription_breakdowns              
                    where sku ilike 'SYN-WK-GIFT-CREDIT%'
                    group by dt    
                ) as total_giftcredits on total.dt = total_giftcredits.dt
                left join
                (
                    select dt, sum(total_subscribers_gained) total_subscribers_gained_preorders, sum(total_subscriptions_gained) as total_subscriptions_gained_preorders
                    from subscription_breakdowns              
                    where is_preorder = 1
                    group by dt    
                ) as total_preorders on total.dt = total_preorders.dt


          ) as gain
          left join
          (

                with subscription_breakdowns as 
                (

                    select 
                          -- only perform timezone offset on subscriptions cancelled within recharge, not on recurly
                          to_date(cancelled_at) as dt,
                          count(*) as total_subscribers_lost, 
                          sum(quantity) as total_subscriptions_lost,
                          is_international,
                          sku,
                          is_preorder
                    from "MARKETING_DATABASE"."PUBLIC"."V_SUBSCRIPTION_MAPPING_PDS_08" as map
                    where map.cancelled_at is not null
                    group by dt, is_international, sku, is_preorder
                    order by dt
                )               
            
                -- **************************************************
                -- BREAKDOWNS: Create aggregation for each breakdown
                -- **************************************************    
                select total.dt, total_subscribers_lost, total_subscriptions_lost, 
                        total_subscribers_lost_international, total_subscriptions_lost_international,
                        total_subscribers_lost_international_3mo_refill, total_subscriptions_lost_international_3mo_refill,
                        total_subscribers_lost_giftkits, total_subscriptions_lost_giftkits,
                        total_subscribers_lost_giftcredits, total_subscriptions_lost_giftcredits,
                        total_subscribers_lost_preorders, total_subscriptions_lost_preorders
                from
                (
                    select dt, sum(total_subscribers_lost) total_subscribers_lost, sum(total_subscriptions_lost) as total_subscriptions_lost
                    from subscription_breakdowns
                    group by dt
                ) as total
                left join
                (
                    select dt, sum(total_subscribers_lost) total_subscribers_lost_international, sum(total_subscriptions_lost) as total_subscriptions_lost_international
                    from subscription_breakdowns
                    where is_international = 1
                    group by dt    
                ) as total_international on total.dt = total_international.dt          
                left join
                (
                    select dt, sum(total_subscribers_lost) total_subscribers_lost_international_3mo_refill, sum(total_subscriptions_lost) as total_subscriptions_lost_international_3mo_refill
                    from subscription_breakdowns
                    where is_international = 1
                    and (sku = 'syn-rf-3mo' or sku ilike '%3mo%')
                    group by dt    
                ) as total_international_3mo_refill on total.dt = total_international_3mo_refill.dt      
                left join
                (
                    select dt, sum(total_subscribers_lost) total_subscribers_lost_giftkits, sum(total_subscriptions_lost) as total_subscriptions_lost_giftkits
                    from subscription_breakdowns              
                    where sku in ('syn-wk-gift-2020','SYN-WK-GIFT')
                    group by dt    
                ) as total_giftkits on total.dt = total_giftkits.dt
                left join
                (
                    select dt, sum(total_subscribers_lost) total_subscribers_lost_giftcredits, sum(total_subscriptions_lost) as total_subscriptions_lost_giftcredits
                    from subscription_breakdowns              
                    where sku ilike 'SYN-WK-GIFT-CREDIT%'
                    group by dt    
                ) as total_giftcredits on total.dt = total_giftcredits.dt
                left join
                (
                    select dt, sum(total_subscribers_lost) total_subscribers_lost_preorders, sum(total_subscriptions_lost) as total_subscriptions_lost_preorders
                    from subscription_breakdowns              
                    where is_preorder = 1
                    group by dt    
                ) as total_preorders on total.dt = total_preorders.dt


          ) as loss 
          on gain.dt = loss.dt
          order by gain.dt  

      )
      
      select gainlossnet_agg.DAY,
          gainlossnet_agg.GAIN,
          gainlossnet_agg.LOSS,
          gainlossnet_agg.NET,
          gainlossnet_agg.TOTAL_ACTIVE,
          gainlossnet_agg.TOTAL_ACTIVE_INTERNATIONAL,
          gainlossnet_agg.TOTAL_ACTIVE_INTERNATIONAL_3MO_REFILL,
          gainlossnet_agg.TOTAL_ACTIVE_GIFTKITS,
          gainlossnet_agg.TOTAL_ACTIVE_GIFTCREDITS,
          gainlossnet_agg.TOTAL_ACTIVE_PREORDERS,
          gainlossnet_agg.TRAILING_30_DAY_GAIN,
          gainlossnet_agg.TRAILING_30_DAY_LOSS,
          gainlossnet_agg.GAIN_INTERNATIONAL,
          gainlossnet_agg.LOSS_INTERNATIONAL,
          gainlossnet_agg.NET_INTERNATIONAL,
          gainlossnet_agg.GAIN_INTERNATIONAL_3MO_REFILL,
          gainlossnet_agg.LOSS_INTERNATIONAL_3MO_REFILL,
          gainlossnet_agg.NET_INTERNATIONAL_3MO_REFILL,
          gainlossnet_agg.GAIN_GIFTKITS,
          gainlossnet_agg.LOSS_GIFTKITS,
          gainlossnet_agg.NET_GIFTKITS,
          gainlossnet_agg.GAIN_GIFTCREDITS,
          gainlossnet_agg.LOSS_GIFTCREDITS,
          gainlossnet_agg.NET_GIFTCREDITS,
          gainlossnet_agg.GAIN_PREORDERS,
          gainlossnet_agg.LOSS_PREORDERS,
          gainlossnet_agg.NET_PREORDERS,

          revenue.TAX_AMOUNT,
          revenue.SHIPPING_AMOUNT,
          revenue.TOTAL_DISCOUNT_AMOUNT,
          revenue.TOTAL_CHARGE_QUANTITY,
          revenue.TOTAL_SUBSCRIPTION_QUANTITY,
          revenue.AOV,
          revenue.PRICE_TOTAL,
          revenue.TOTAL_GIFTKIT_SUBSCRIPTION_QUANTITY,
          revenue.TOTAL_GIFTCREDIT_SUBSCRIPTION_QUANTITY,
          revenue.REFILL_COGS,
          revenue.WELCOME_KIT_COGS,
          revenue.GROSS_SALES,
          revenue.NET_SALES,
          revenue.CREDIT_APPLIED
                  

      from
      (
          select day, gain, loss, net,
                  sum(net) over (order by day asc rows between unbounded preceding and current row) as total_active,
                  sum(net_international) over (order by day asc rows between unbounded preceding and current row) as total_active_international,
                  sum(net_international_3mo_refill) over (order by day asc rows between unbounded preceding and current row) as total_active_international_3mo_refill,
                  sum(net_giftkits) over (order by day asc rows between unbounded preceding and current row) as total_active_giftkits,
                  sum(net_giftcredits) over (order by day asc rows between unbounded preceding and current row) as total_active_giftcredits,  
                  sum(net_preorders) over (order by day asc rows between unbounded preceding and current row) as total_active_preorders,  

                  sum(gain) over (order by day asc rows between 29 preceding and current row) as trailing_30_day_gain,        
                  sum(loss) over (order by day asc rows between 29 preceding and current row) as trailing_30_day_loss,

                  gain_international,
                  loss_international,
                  net_international,
                  gain_international_3mo_refill,
                  loss_international_3mo_refill,
                  net_international_3mo_refill,
                  gain_giftkits,
                  loss_giftkits,
                  net_giftkits,
                  gain_giftcredits,
                  loss_giftcredits,
                  net_giftcredits,
                  gain_preorders,
                  loss_preorders,
                  net_preorders

          from gainlossnet
      ) as gainlossnet_agg
      left join "MARKETING_DATABASE"."GOOGLE_SHEETS"."MANUAL_INPUT" manual on manual.date = gainlossnet_agg.day
      left join revenue on revenue.day = gainlossnet_agg.day

      order by gainlossnet_agg.day asc;