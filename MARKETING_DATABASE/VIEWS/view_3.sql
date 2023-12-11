create view kustomer_customer_view as 
(select 
b.Email as pk_email,
sum(a.CONVERSATION_COUNTS_ALL) as ticket_volume,
--list_agg(d.ticket_tag, ',') as ticket_tag,
listagg( distinct e.name, ',') as ticket_channel,
max(c.CREATED_AT) as last_ticket_date,
case when 
  count_if(c.status  = 'open') > 0 then 'unsolved' else 'solved' end as ticket_status,
datediff(minute, MIN(c.FIRST_MESSAGE_IN_SENT_AT),MAX(c.LAST_MESSAGE_OUT_SENT_AT)) as avg_handle_time,
avg(c.Satisfaction) as satisfication
from "MARKETING_DATABASE"."KUSTOMER"."CUSTOMER" as a
join "MARKETING_DATABASE"."KUSTOMER"."CUSTOMER_EMAIL" as b on a.ID = b.CUSTOMER_ID
join "MARKETING_DATABASE"."KUSTOMER"."CONVERSATION" as c on a.ID = c.CUSTOMER_ID
join"MARKETING_DATABASE"."KUSTOMER"."CUSTOMER_LAST_CONVERSATION_TAG" as d on b.CUSTOMER_ID = d.CUSTOMER_ID
join "MARKETING_DATABASE"."KUSTOMER"."CONVERSATION_CHANNEL" as e on C.ID = e.CONVERSATION_ID
group by b.email, e.name)