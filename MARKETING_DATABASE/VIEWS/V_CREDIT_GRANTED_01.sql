create or replace view MARKETING_DATABASE.PRODUCTION_VIEWS.V_CREDIT_GRANTED_01(
	INVOICE_ID,
    INVOICE_NUMBER,
    CREDIT_GRANTED_DATE,
    CREDIT_AMOUNT
) as

select ID as invoice_ID,Invoice_number,to_date(billed_date) AS credit_granted_date,abs(invoice_total) AS credit_amount
from "IO06230_RECURLY_SEED_SHARE"."CLASSIC"."INVOICES_SUMMARY"
    where invoice_type = 'credit'