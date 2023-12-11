create view "MARKETING_DATABASE"."PUBLIC".V_AllAudience
    as
				WITH credit_final AS (
    SELECT
        i.account_code,
       CASE
            WHEN adjustment_plan_code ILIKE '%3%' then 'DS-01-SRP3'
            WHEN adjustment_plan_code ILIKE '%6%' then 'DS-01-SRP6'
            WHEN adjustment_plan_code ILIKE '%pds%' THEN 'PDS-08'
            ELSE 'DS-01'
        END AS product,
        sum(cp.amount) AS credit_amount
    FROM
        "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."CREDIT_PAYMENTS" AS CP
        LEFT JOIN "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY" AS i ON i.invoice_number = cp.applied_to_invoice_number
        LEFT JOIN "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" AS a ON a.invoice_id = i.id
    WHERE
        action != 'write_off'
    GROUP BY
        i.account_code,
        product
    ORDER BY
        credit_amount DESC
),
nonship_adj AS (
    SELECT
        t.account_code,
        CASE
            WHEN adjustment_plan_code ILIKE '%3%' then 'DS-01-SRP3'
            WHEN adjustment_plan_code ILIKE '%6%' then 'DS-01-SRP6'
            WHEN adjustment_plan_code ILIKE '%pds%' THEN 'PDS-08'
            ELSE 'DS-01'
        END AS product,
        sum(adjustment_discount) AS total_discount_amount,
        sum(adjustment_tax) AS total_tax,
        sum(t.amount) AS total_price,
        sum(adjustment_quantity) AS quantity
    FROM
        "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" AS t
        JOIN "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" AS a ON t.invoice_id = a.invoice_id
    WHERE
        t.type = 'purchase'
        AND t.status = 'success'
        AND adjustment_description NOT ILIKE '%shipping%'
        AND adjustment_description NOT ILIKE '%(Replacement)%'
        AND adjustment_description NOT ILIKE '%preorder%'
    GROUP BY
        t.account_code,
        product
    ORDER BY
        total_price,
        account_code,
        product DESC
),
shipping_adj AS (
    SELECT
        t.account_code,
       CASE
            WHEN adjustment_plan_code ILIKE '%3%' then 'DS-01-SRP3'
            WHEN adjustment_plan_code ILIKE '%6%' then 'DS-01-SRP6'
            WHEN adjustment_plan_code ILIKE '%pds%' THEN 'PDS-08'
            ELSE 'DS-01'
        END AS product,
        sum(adjustment_amount) AS total_shipping,
        sum(adjustment_tax) AS total_shipping_tax
    FROM
        "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" AS t
        JOIN "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" AS a ON t.invoice_id = a.invoice_id
    WHERE
        t.type = 'purchase'
        AND t.status = 'success'
        AND adjustment_description ILIKE '%shipping%'
    GROUP BY
        t.account_code,
        product
    ORDER BY
        account_code,
        product DESC
),
refunds AS (
    SELECT
        t.account_code,
         CASE
            WHEN adjustment_plan_code ILIKE '%3%' then 'DS-01-SRP3'
            WHEN adjustment_plan_code ILIKE '%6%' then 'DS-01-SRP6'
            WHEN adjustment_plan_code ILIKE '%pds%' THEN 'PDS-08'
            ELSE 'DS-01'
        END AS product,
        sum(t.amount) AS refund_amount
    FROM
        "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."TRANSACTIONS" AS t
        JOIN "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."ADJUSTMENTS" AS a ON t.invoice_id = a.invoice_id
    WHERE
        t.type = 'refund'
        AND t.status = 'success'
        AND adjustment_description NOT ILIKE '%shipping%'
    GROUP BY
        t.account_code,
        product
),
max_account AS (
    SELECT
        account_code,
        CASE
            WHEN plan_code ILIKE '%3%' THEN 'DS-01-SRP3'
            WHEN plan_code ILIKE '%6%' THEN 'DS-01-SRP6'
            WHEN plan_code ILIKE '%pds%' THEN 'PDS-08'
            ELSE 'DS-01'
        END AS product,
        min(activated_at) AS first_active_date,
        max(canceled_at) AS canceled_date
    FROM
        "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS"
    GROUP BY
        account_code,
        product
),
account_info AS (
    SELECT
        DISTINCT s.account_code,
        s.EMAIL AS email,
        s.ship_address_phone as phone_ship,
        s.STATE AS status,
        s.SHIP_ADDRESS_NICKNAME AS name,
        s.SHIP_ADDRESS_FIRSTNAME AS fname,
        s.SHIP_ADDRESS_LASTNAME AS lname,
        s.SHIP_ADDRESS_STREET1 AS address,
        SHIP_ADDRESS_CITY AS city,
        SHIP_ADDRESS_STATE AS STATE,
        SHIP_ADDRESS_ZIP AS postal,
        SHIP_ADDRESS_COUNTRY AS country,
        row_number() over(
            partition BY s.account_code
            ORDER BY
                country
        ) AS row_number
    FROM
        "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."SUBSCRIPTIONS" AS s
        JOIN max_account AS ma ON s.account_code = ma.account_code
        AND s.activated_at = ma.first_active_date
),
account_final AS (
    SELECT
        a.account_code,
        product,
        status,
        first_active_date,
        LEFT(first_active_date, 10) as sdc_date_start,
        LEFT(canceled_date, 10) as sdc_cancel_date,
        (datediff(day, sdc_date_start, sdc_cancel_date) + 1) as sdc_number_of_days,
        (datediff(day, sdc_cancel_date, CURRENT_DATE()) + 1) as sdc_number_of_days_since_cancel,
        email,
    phone_ship,
        name,
        fname,
        lname,
        address,
        city,
        postal,
        STATE,
        country
    FROM
        account_info AS a
        LEFT JOIN max_account AS ma ON a.account_code = ma.account_code
    WHERE
        row_number = 1
        AND country LIKE 'US'
        AND email NOT LIKE '%seed.com%'
        AND email <> ''
)
SELECT
    af.*,
    coalesce(total_price, 0) AS total_price,
    coalesce(quantity, 0) AS total_quantity,
    coalesce(total_tax, 0) AS total_tax,
    coalesce(total_discount_amount, 0) AS total_discount,
    coalesce(s.total_shipping, 0) AS total_shipping,
    coalesce(s.total_shipping_tax, 0) AS total_shipping_tax,
    coalesce(cf.credit_amount, 0) AS total_credit,
    coalesce(r.refund_amount, 0) AS total_refunds,
    (total_price - total_refunds - total_credit) AS sdc_final_price,
    round((div0(sdc_final_price, total_quantity)), 1) as sdc_price_per,
    round((div0(sdc_final_price, sdc_number_of_days)), 1) as sdc_spend_per_day,
    round((div0(sdc_number_of_days, 30)), 1) as sdc_billing_periods
FROM
    account_final AS af
    LEFT JOIN nonship_adj AS ns ON ns.account_code = af.account_code
    AND ns.product = af.product
    LEFT JOIN shipping_adj AS s ON af.account_code = s.account_code
    AND af.product = s.product
    LEFT JOIN credit_final AS cf ON af.account_code = cf.account_code
    AND af.product = cf.product
    LEFT JOIN refunds AS r ON af.account_code = r.account_code
    AND af.product = r.product