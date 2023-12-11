create view v_customer_kustomer as

select
	ce.email as pk_email,
	count(*) as ticket_volume,
	cc.name as ticket_channel,
	max(first_message_in_sent_at) as last_ticket_date,
	max(last_message_direction) as last_ticket_direction,
	avg(con.satisfaction) as avg_satisfaction,
	avg(datediff(hour,con.first_message_in_sent_at, con.last_message_at)) as avg_handle_time,
	listagg(distinct t.name, ' | ') as tags,
	case when
	  count_if(con.status  = 'open') > 0 then 'open' else 'closed' end as ticket_status
from "MARKETING_DATABASE"."KUSTOMER"."CUSTOMER" as cust
join "MARKETING_DATABASE"."KUSTOMER"."CUSTOMER_EMAIL" as ce on ce.customer_id = cust.id
join "MARKETING_DATABASE"."KUSTOMER"."CONVERSATION"as con on con.customer_id = cust.id
join "MARKETING_DATABASE"."KUSTOMER"."CONVERSATION_CHANNEL" as cc on cc.conversation_id = con.id
left join "MARKETING_DATABASE"."KUSTOMER"."CONVERSATION_TAG" as ct on ct.conversation_id = con.id
left join "MARKETING_DATABASE"."KUSTOMER"."TAG" as t on t.id = ct.tag_id
group by cc.name, ce.email,cust.name
order by pk_email asc