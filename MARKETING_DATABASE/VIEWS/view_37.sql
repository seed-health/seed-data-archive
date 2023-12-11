create view v_customer_recurly as 
select case 
                when rsh.version_started_at is null then rs.created_at
                
                else rsh.version_started_at
            end as Date, 
            rs.Email as Email,
            rs.Account_Code as AccountCode,
            'Recurly' as DataSource, 
            case
                when rsh.subscription_state is null then rs.state
                else rsh.subscription_state
            end as Action,
            'Recurly.Subscriptions' as SourceTableName,
            UUID as SourceColumnName,
            to_varchar(rs.UUID) as SourceTableID,
            to_varchar(rsh.subscription_uuid) as pk_subscription_id,
            to_varchar(rs.ship_address_firstname) as first_name,
            to_varchar(rs.ship_address_lastname) as last_name,
            to_varchar(rs.created_at) as start_time,
            to_varchar(rs.activated_at) as activated_at,
            to_varchar(rs.canceled_at) as end_time,
            to_varchar(rs.total_billing_cycles) as length, --Check
            to_varchar(rsh.plan_code) as product_id,
            to_varchar(rsh.plan_name) as product_name,
            to_varchar(rs.quantity) as quantity,
            to_varchar(rs.total_recurring_amount) as total_subscription_value,
            to_varchar(rs.ship_address_phone) as phone,
            to_varchar(rs.ship_address_street1) as address,
            to_varchar(rs.ship_address_city) as city, 
            to_varchar(rs.ship_address_state) as state,
            to_varchar(rs.ship_address_country) as country,
            to_varchar(rs.ship_address_zip) as zipcode

            /*Still Need to check length and total_subscription_value. Still missing columns Total_Shipped_Quantity, Total_subscription_count, discounts_applied*/
    from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" as rs
    left join "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTION_HISTORY" rsh on rs.ACCOUNT_CODE = rsh.ACCOUNT_CODE