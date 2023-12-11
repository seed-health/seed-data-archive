create or replace view SEED_DATA.DEV.V_MARKETING_CHANNEL_SPEND_WEB_CONVERSIONS as 

with order_events as (
select 
  to_date(oc.event_time) as date
, oc.UTM_SOURCE
, oc.UTM_MEDIUM
, oc.UTM_SOURCE_MEDIUM
, oc.utm_campaign
, oc.referrer
, smm.UTM_SOURCE_MAP
, smm.UTM_MEDIUM_MAP
, smm.UTM_CAMPAIGN_MAP
, count(distinct oc.user_id) as order_complete_uucount

from
SEED_DATA.DEV.V_EVENTS_ORDER_COMPLETE as oc
---- inputs from growth team on utm mapping (https://docs.google.com/spreadsheets/d/1XZ0RChicS-NSVOU9yXodRX77fKLtpNR3nSA-Zfv0TDA/edit#gid=0)
left join MARKETING_DATABASE.GOOGLE_SHEETS.SOURCE_MEDIUM_MAPPING smm
on oc.UTM_SOURCE_MEDIUM = smm.UTM_SOURCE_MEDIUM

where to_date(oc.event_time) between '2023-01-01' and current_date()-1
group by 1,2,3,4,5,6,7,8,9
)

, order_events_conv as (
select 
  oe.date
, oe.UTM_SOURCE
, oe.UTM_MEDIUM
, oe.UTM_SOURCE_MEDIUM
, oe.utm_campaign
, oe.referrer
, oe.UTM_SOURCE_MAP
, oe.UTM_MEDIUM_MAP
, oe.UTM_CAMPAIGN_MAP
, sum(oe.order_complete_uucount) as order_complete_uucount
, ifnull(max(pvt."'BING'"),0) as _bing
, ifnull(max(pvt."'FACEBOOK'"),0) as _facebook
, ifnull(max(pvt."'GEIST'"),0) as _geist
, ifnull(max(pvt."'IHEART'"),0) as _iheart
, ifnull(max(pvt."'INFLUENCER'"),0) as _influencer
, ifnull(max(pvt."'INFLUENCERAGENCY'"),0) as _influenceragency
, ifnull(max(pvt."'MISC'"),0) as _misc
, ifnull(max(pvt."'OUTBRAIN'"),0) as _outbrain
, ifnull(max(pvt."'PCA'"),0) as _pca
, ifnull(max(pvt."'PINTEREST'"),0) as _pinterest
, ifnull(max(pvt."'QUORA'"),0) as _quora
, ifnull(max(pvt."'REDDIT'"),0) as _reddit
, ifnull(max(pvt."'SNAPCHAT'"),0) as _snapchat
, ifnull(max(pvt."'SPOTIFY'"),0) as _spotify
, ifnull(max(pvt."'TAPJOY'"),0) as _tapjoy
, ifnull(max(pvt."'TIKTOK'"),0) as _tiktok
, ifnull(max(pvt."'TRADEDESK'"),0) as _tradedesk
, ifnull(max(pvt."'GOOGLE ADS'"),0) as _googleads
, ifnull(max(mrk.SPEND),0) as TOTAL_SPEND

from order_events as oe
----- join in to pivot spend build / bu
left join SEED_DATA.DEV.V_MARKETING_CHANNEL_SPEND_PIVOT pvt
on oe.date = pvt.date
----- join in to total spend
left join (select date, sum(spend) as spend from SEED_DATA.DEV.V_MARKETING_CHANNEL_SPEND_ALL where concat(SOURCE,'-',TYPE) <> 'PINTEREST-MANUAL' group by 1) mrk
on oe.date = mrk.date

group by 1,2,3,4,5,6,7,8,9
order by 1 desc
)

, partition_build as (
select 
ROW_NUMBER() OVER (PARTITION BY date ORDER BY date desc) AS row_number, *
from order_events_conv)

select  
  date
, UTM_SOURCE
, UTM_MEDIUM
, UTM_SOURCE_MEDIUM
, utm_campaign
, referrer
, UTM_SOURCE_MAP
, UTM_MEDIUM_MAP
, UTM_CAMPAIGN_MAP
, order_complete_uucount
, case when row_number = 1 then ifnull((_bing),0) else 0 end as _bing
, case when row_number = 1 then ifnull((_facebook),0) else 0 end as _facebook
, case when row_number = 1 then ifnull((_geist),0) else 0 end as _geist
, case when row_number = 1 then ifnull((_iheart),0) else 0 end as _iheart
, case when row_number = 1 then ifnull((_influencer),0) else 0 end as _influencer
, case when row_number = 1 then ifnull((_influenceragency),0) else 0 end as _influenceragency
, case when row_number = 1 then ifnull((_misc),0) else 0 end as _misc
, case when row_number = 1 then ifnull((_outbrain),0) else 0 end as _outbrain
, case when row_number = 1 then ifnull((_pca),0) else 0 end as _pca
, case when row_number = 1 then ifnull((_pinterest),0) else 0 end as _pinterest
, case when row_number = 1 then ifnull((_quora),0) else 0 end as _quora
, case when row_number = 1 then ifnull((_reddit),0) else 0 end as _reddit
, case when row_number = 1 then ifnull((_snapchat),0) else 0 end as _snapchat
, case when row_number = 1 then ifnull((_spotify),0) else 0 end as _spotify
, case when row_number = 1 then ifnull((_tapjoy),0) else 0 end as _tapjoy
, case when row_number = 1 then ifnull((_tiktok),0) else 0 end as _tiktok
, case when row_number = 1 then ifnull((_tradedesk),0) else 0 end as _tradedesk
, case when row_number = 1 then ifnull((_googleads),0) else 0 end as _googleads
, case when row_number = 1 then ifnull((TOTAL_SPEND),0) else 0 end as _total_spend
from partition_build


--SELECT * FROM SEED_DATA.DEV.V_MARKETING_CHANNEL_SPEND_WEB_CONVERSIONS WHERE DATE IN ('2023-06-18') 
--select sum(spend) as spend from SEED_DATA.DEV.V_MARKETING_CHANNEL_SPEND_ALL where concat(SOURCE,'-',TYPE) <> 'PINTEREST-MANUAL' and date between '2023-06-01' and '2023-06-18'

--select * from SEED_DATA.DEV.V_MARKETING_CHANNEL_SPEND_ALL
