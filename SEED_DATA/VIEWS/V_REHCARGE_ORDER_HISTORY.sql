create or replace view SEED_DATA.DEV.V_REHCARGE_ORDER_HISTORY(
	INVOICE_ID,
	TRANSACTION_ID,
	CUSTOMER_ID,
	CUSTOMER_EMAIL,
	SUBSCRIPTION_ID,
	ORDER_DATE,
	SKU,
    DISCOUNT_CODE,
	QUANTITY,
	BASE_PRICE,
	TOTAL_AMOUNT_PAID,
	TAX,
	DISCOUNT,
	SHIPPING_COST,
	REFUND_AMOUNT
) as 

with recharge_rev as
(
    with recharge_charge_line_item as
    (
        --- We need to normalize the amount of transaction based on the line items on the transaction   
        
        select charge_id,index, subscription_id,quantity,sku, 
            case when sku ilike '%3MO' then quantity*3 else quantity end as adjusted_quantity,
            sum(adjusted_quantity) over(partition by charge_id) as adjusted_quantity_total,
            (adjusted_quantity/adjusted_quantity_total) as norm_factor
        from "MARKETING_DATABASE"."RECHARGE"."CHARGE_LINE_ITEM"
        where sku not ilike '12345'
        -- Filtering out what looks like a test SKU
                
    ),

    recharge_discount_code as 
    (
        select charge_id, code as discount_code
        from "MARKETING_DATABASE"."RECHARGE"."CHARGE_DISCOUNT_CODE" as cdc
          left join "MARKETING_DATABASE"."RECHARGE"."DISCOUNT" as d on cdc.discount_id = d.id 
    )
    
    select o.id as invoice_id ,o.processed_at, c.id as transaction_id,c.customer_id as customer_id, c.email as customer_email,cli.subscription_id, 
        to_date(dateadd(hour, -4, c.processed_at)) as order_date,cli.sku as sku, cli.quantity as quantity, 
        (c.total_line_items_price*cli.norm_factor) as base_price,(c.total_price*cli.norm_factor) as total_amount_paid, 
        (c.total_tax*cli.norm_factor) as tax, (c.total_discounts*cli.norm_factor) as discount,
        (csl.price*cli.norm_factor)  as shipping_cost, (c.total_refunds*cli.norm_factor) as refund_amount,
        dc.discount_code
    from "MARKETING_DATABASE"."RECHARGE"."ORDER" as o
        join "MARKETING_DATABASE"."RECHARGE"."CHARGE" as c on c.id = o.charge_id
        join recharge_charge_line_item as cli on cli.charge_id = c.id
        left join "MARKETING_DATABASE"."RECHARGE"."CHARGE_SHIPPING_LINE" as csl on csl.charge_id = c.id
        left join recharge_discount_code as dc on dc.charge_id = c.id
    where c.processed_at is not null 
    
)
select 
  invoice_id 
, transaction_id
, customer_id
, customer_email
, subscription_id
, order_date
, sku
, discount_code
, ifnull(sum(quantity),0) as quantity
, ifnull(sum(base_price),0) as base_price
, ifnull(sum(total_amount_paid),0) as total_amount_paid
, ifnull(sum(tax),0) as tax
, ifnull(sum(discount),0) as discount
, ifnull(sum(shipping_cost),0) as shipping_cost
, ifnull(sum(refund_amount),0) as refund_amount

from recharge_rev

group by 1,2,3,4,5,6,7,8


--select * from "MARKETING_DATABASE"."RECHARGE"."ORDER" limit 10
;