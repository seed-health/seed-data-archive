create or replace view SEED_DATA.DEV.V_KLAVIYO_CAMPAIGN_PERFORMANCE  as 

with campaigns as 
( 
  select 
   ID as CAMPAIGN_ID 
  ,Name as CAMPAIGN_NAME
  ,'Email' as DELIVERY_METHOD
  , CAMPAIGN_TYPE as CAMPAIGN_TYPE
  , to_date(CREATED) as START_DATE
  , null as END_DATE
  , CREATED as START_TIMESTAMP
  , current_timestamp as END_TIMESTAMP
  , STATUS
  , SUBJECT as EMAIL_SUBJECT
  
  from "MARKETING_DATABASE"."KLAVIYO"."CAMPAIGN" 
 ) 

--------------Sent-------------
,sent as (
select 
campaign_id
,count(distinct person_id) as sent
from
"MARKETING_DATABASE"."KLAVIYO"."EVENT" 
group by 1 
  )
  
-------------Delivered----------
,delivered as (
    select 
campaign_id
,count(distinct person_id) as delivered
from
"MARKETING_DATABASE"."KLAVIYO"."EVENT" 
where TYPE = 'Received Email'
group by 1 
  ) 
  
--------------bounces-----------
,bounced as (
    select 
campaign_id
,count(distinct person_id) as bounced
from
"MARKETING_DATABASE"."KLAVIYO"."EVENT" 
where TYPE = 'Bounced Email' 
group by 1 
  )  
 --------------Opened-----------  
  ,opened as (
    select 
campaign_id
,count(person_id) as opened    
,count(distinct person_id) as unique_opened
from
"MARKETING_DATABASE"."KLAVIYO"."EVENT" 
where TYPE = 'Opened Email' 
group by 1 
  ) 
  --------------Clicks -----------
    ,clicked as (
    select 
campaign_id
,count(person_id) as Clicked
,count(distinct person_id) as unique_Clicked
from
"MARKETING_DATABASE"."KLAVIYO"."EVENT" 
where TYPE = 'Clicked Email' 
group by 1 
  ) 

---------------UNSUBSCRIBED------------
, UNSUBSCRIBED as (
    select 
campaign_id
,count(distinct person_id) as unsubscribed
from
"MARKETING_DATABASE"."KLAVIYO"."EVENT" 
where TYPE = 'Unsubscribed' 
group by 1 
  ) 
  
   ----------------The next couple subqueries are to determine conversion--------------
  
, open_click as 
(
    select 
distinct upper(email) as email
, campaign_id
, timestamp as timestamp  
from
"MARKETING_DATABASE"."KLAVIYO"."EVENT" e
left join "MARKETING_DATABASE"."KLAVIYO"."PERSON" p
 on e.person_id = p.id   
where TYPE = 'Opened Email' 
  
  union all 
  
      select 
distinct upper(email) as email
, campaign_id
, timestamp as timestamp 
from
"MARKETING_DATABASE"."KLAVIYO"."EVENT" e
left join "MARKETING_DATABASE"."KLAVIYO"."PERSON" p
 on e.person_id = p.id   
where TYPE = 'Clicked Email' 

  
)

, combined as (
  select 
  distinct upper(email) as email
, campaign_id
, max(timestamp) as event_date 
  from open_click
  group by 1,2
           )
  
, Sub_Start as (
select UPPER(EMAIL) as Email, 
       Timestamp as activated_date, 
       Event
       from 
       "SEGMENT_EVENTS"."CORE_STAGING"."RECURLY_START_SUBSCRIPTION" 
     )

, diff_active as (
select 
campaign_id,
d.email,
event_Date,
activated_date,
datediff(day,event_date,activated_date) as date_diff

from combined d
left join sub_start s
on d.email = s.email

where date_diff between 0 and 5
          )

,conversion_active as (
  select 
campaign_id,
count(distinct email) as conversion_active
  from diff_active
  group by 1
  
 )
 
 , STP_Start as (
select UPPER(EMAIL) as Email, 
       Timestamp as activated_date, 
       Event
       from 
       SEGMENT_EVENTS.CORE_STAGING.STP_ENROLLMENT
     )

, diff_STP as (
select 
campaign_id,
d.email,
event_Date,
activated_date,
datediff(day,event_date,activated_date) as date_diff

from combined d
left join stp_start s
on d.email = s.email

where date_diff between 0 and 5
          )

,conversion_STP as (
  select 
campaign_id,
count(distinct email) as conversion_stp
  from diff_stp
  group by 1
  
 )
  
 ,final as 
(
select 
 c.CAMPAIGN_ID
,c.CAMPAIGN_NAME
,c.DELIVERY_METHOD
,c.CAMPAIGN_TYPE
,c.START_DATE
,c.END_DATE
,c.START_TIMESTAMP
,c.END_TIMESTAMP
,c.STATUS
,c.EMAIL_SUBJECT
,s.SENT
,b.BOUNCED
,o.OPENED
,o.UNIQUE_OPENED
,cl.CLICKED
,cl.UNIQUE_CLICKED  
,ch.UNSUBSCRIBED
,d.Delivered as DELIVERED  
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
  
-----Adding Sent-------------  
left join sent s 
on c.CAMPAIGN_ID = s.CAMPAIGN_ID    
-----Adding delivered-------------  
left join delivered d 
on c.CAMPAIGN_ID = d.CAMPAIGN_ID    
  
-----Adding bounce-------------  
left join bounced b
on c.CAMPAIGN_ID = b.CAMPAIGN_ID    

-----Adding  opened -------------  
left join opened  o
on c.CAMPAIGN_ID = o.CAMPAIGN_ID    
  -----Adding  clicks -------------  
left join clicked  cl
on c.CAMPAIGN_ID = cl.CAMPAIGN_ID    
      -----Adding  unsub -------------  
left join UNSUBSCRIBED  ch
on c.CAMPAIGN_ID = ch.CAMPAIGN_ID    
  
        -----Adding Conversion for campaigns to get new customers-------------  
left join conversion_active  cona
on c.CAMPAIGN_ID = cona.CAMPAIGN_ID  
         -----Adding Conversion for campaigns to get customers to upgrade-------------  
left join conversion_STP  cons
on c.CAMPAIGN_ID = cons.CAMPAIGN_ID   

)

select * from final;