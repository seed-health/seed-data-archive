create or replace view SEED_DATA.DEV.V_ITERABLE_CAMPAIGN_PERFORMANCE(
	CAMPAIGN_ID,
	CAMPAIGN_NAME,
	DELIVERY_METHOD,
	CAMPAIGN_TYPE,
	START_DATE,
	END_DATE,
	START_TIMESTAMP,
	END_TIMESTAMP,
	STATUS,
	TYPE,
	SUB_TYPE,
	PRODUCT,
	DISCOUNT,
	SEGMENT,
	FLOW_TYPE,
	FLOW_SUBTYPE,
	EMAIL_SUBJECT,
	SENT,
	BOUNCED,
	OPENED,
	UNIQUE_OPENED,
	CLICKED,
	UNIQUE_CLICKED,
	UNSUBSCRIBED,
	DELIVERED,
	DELIVERD_RATE,
	BOUNCED_RATE,
	OPENED_RATE,
	CLICK_RATE,
	CLICK_TO_OPEN_RATE,
	UNSUBSCRIBED_RATE,
	CONVERSION_ACTIVE,
	CONVERSION_RATE_ACTIVE,
	CONVERSION_STP,
	CONVERSION_RATE_STP
) as 
-------------------------------------------ITERABLE-----------------------------------------------------
with campaigns as (
select 
cast(id as string) as CAMPAIGN_ID
,name as CAMPAIGN_NAME
,message_medium as DELIVERY_METHOD
,type as CAMPAIGN_TYPE
,to_date(start_at) as START_DATE
,to_date(ended_at) as END_DATE
,cast(start_at as TIMESTAMP_TZ) as START_TIMESTAMP
,cast(ended_at as TIMESTAMP_TZ) as END_TIMESTAMP
,campaign_state as STATUS


from "ITERABLE_EVENT_DATA"."ORG_3034"."CAMPAIGNS" 
  where workflow_id is null
  
  union all 
  
  select 
 cast(c.id as string) as CAMPAIGN_ID
,wf.name as CAMPAIGN_NAME
,message_medium as DELIVERY_METHOD
,'FLOW' as CAMPAIGN_TYPE
,to_date(wf.created_at) as START_DATE
,to_date(ended_at) as END_DATE
,cast(start_at as TIMESTAMP_TZ) as START_TIMESTAMP
,cast(ended_at as TIMESTAMP_TZ) as END_TIMESTAMP
,campaign_state as STATUS

from "ITERABLE_EVENT_DATA"."ORG_3034"."CAMPAIGNS" c
left join "ITERABLE_EVENT_DATA"."ORG_3034"."WORKFLOWS" wf
  on c.workflow_id = wf.id
  where workflow_id is not null
)
------------adding labels----------------
,audience as (
select *   
from
PROD_DB.GROWTH.V_ITERABLE_CAMPAIGN_LABELS 
  
  )
--------------Sent-------------
,sent as (
select 
  campaign_id,
  EMAIL_SUBJECT as EMAIL_SUBJECT,
  count(distinct email) as sent
  from
   "ITERABLE_EVENT_DATA"."ORG_3034"."EMAIL_BLAST_SENDS_VIEW" 
    group by 1,2

  union all
  select 
  campaign_id,
  EMAIL_SUBJECT as EMAIL_SUBJECT,
  count(distinct email) as sent
  from
  "ITERABLE_EVENT_DATA"."ORG_3034"."EMAIL_TRIGGERED_SENDS_VIEW" 
      group by 1,2
  )
--------------bounces-----------
, bounces as (
select 
  campaign_id,
  count(distinct email) as bounced
  from
"ITERABLE_EVENT_DATA"."ORG_3034"."EMAIL_BOUNCES_VIEW"
  group by 1
  )  
--------------Opened----------- 
  
 , opened as (
  select 
  campaign_id,
  count(email) as opened,
  count(distinct email) as unique_opened
  from
  "ITERABLE_EVENT_DATA"."ORG_3034"."EMAIL_OPENS_VIEW"
   group by 1 
  )
 
--------------Clicks -----------
,clicks as (
select 
campaign_id,
count(email) as clicked ,
count(distinct email) as unique_clicked 
  from "ITERABLE_EVENT_DATA"."ORG_3034"."EMAIL_CLICKS_VIEW" 
  group by 1
)

---------------UNSUBSCRIBED------------
, UNSUBSCRIBED as (
  select 
  campaign_id,
  count(email) as UNSUBSCRIBED
  from
"ITERABLE_EVENT_DATA"."ORG_3034"."EMAIL_UNSUBSCRIBES_VIEW" 
  group by 1
  
 )
 
 ----------------The next couple subqueries are to determine conversion--------------
 
, delivered as 
(
select upper(email) as email,
       email_subject, 
       campaign_id, 
       timestamp as delivered_date
        from
         "SEGMENT_EVENTS"."ITERABLE_PRODUCTION"."EMAIL_DELIVERED" 
             
)

, Sub_Start as (
select UPPER(EMAIL) as Email, 
       Timestamp as activated_date, 
       Event
       from 
       "SEGMENT_EVENTS"."CORE_STAGING"."RECURLY_START_SUBSCRIPTION" 
     )

, diff as (
select 
campaign_id,
d.email,
Delivered_Date,
activated_date,
datediff(hour,delivered_date,activated_date) as date_diff

from delivered d
left join sub_start s
on d.email = s.email

where date_diff between 0 and 72 
          )

,conversion_active as (
  select 
campaign_id,
count(email) as conversion_Active
  from diff
  group by 1
  )
  
  , Stp_Start as (
select UPPER(EMAIL) as Email, 
       Timestamp as activated_date, 
       Event
       from 
        SEGMENT_EVENTS.CORE_STAGING.STP_ENROLLMENT
     )

, STP_diff as (
select 
campaign_id,
d.email,
Delivered_Date,
activated_date,
datediff(hour,delivered_date,activated_date) as date_diff

from delivered d
left join stp_start s
on d.email = s.email

where date_diff between 0 and 72 
          )
 
 ,conversion_stp as (
  select 
campaign_id,
count(email) as conversion_STP
  from STP_diff
  group by 1
  )
  

,final as 
(
select 
 cast(c.CAMPAIGN_ID as string) as campaign_id
,c.CAMPAIGN_NAME
,c.DELIVERY_METHOD
,c.CAMPAIGN_TYPE
,c.START_DATE
,c.END_DATE
,c.START_TIMESTAMP
,c.END_TIMESTAMP
,c.STATUS
,al.type
,al.sub_type
,al.product
,al.discount
,al.segment
,al.flow_type
,al.flow_subtype
,s.EMAIL_SUBJECT
,s.SENT
,b.BOUNCED
,o.OPENED
,o.UNIQUE_OPENED
,cl.CLICKED
,cl.UNIQUE_CLICKED  
,ch.UNSUBSCRIBED
,s.SENT-b.BOUNCED as DELIVERED  
,delivered/s.SENT as DELIVERD_RATE
,b.BOUNCED/s.SENT as BOUNCED_RATE
,o.UNIQUE_OPENED/DELIVERED as OPENED_RATE
,cl.UNIQUE_CLICKED/DELIVERED as CLICK_RATE  
,cl.UNIQUE_CLICKED/o.UNIQUE_OPENED as CLICK_TO_OPEN_RATE
,ch.UNSUBSCRIBED/DELIVERED as UNSUBSCRIBED_RATE
,CONVERSION_ACTIVE as CONVERSION_ACTIVE
,CONVERSION_ACTIVE/DELIVERED as CONVERSION_RATE_ACTIVE
,CONVERSION_STP as CONVERSION_STP
,CONVERSION_STP/DELIVERED as CONVERSION_RATE_STP
  
from
campaigns c
-----Adding Audience Data-------------
left join audience al 
on c.CAMPAIGN_ID = al.CAMPAIGN_ID  
  
-----Adding Sent-------------  
left join sent s 
on c.CAMPAIGN_ID = s.CAMPAIGN_ID    

-----Adding Sent-------------  
left join bounces b
on c.CAMPAIGN_ID = b.CAMPAIGN_ID    

-----Adding  opened -------------  
left join opened  o
on c.CAMPAIGN_ID = o.CAMPAIGN_ID    
  -----Adding  clicks -------------  
left join clicks  cl
on c.CAMPAIGN_ID = cl.CAMPAIGN_ID    
      -----Adding  unsub -------------  
left join UNSUBSCRIBED  ch
on c.CAMPAIGN_ID = ch.CAMPAIGN_ID    
  
        -----Adding Conversion -------------  
left join conversion_active  con
on c.CAMPAIGN_ID = con.CAMPAIGN_ID  

        -----Adding Conversion -------------  
left join conversion_stp  cons
on c.CAMPAIGN_ID = cons.CAMPAIGN_ID   

)

SELECT * FROM FINAL;