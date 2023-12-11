create or replace view SEED_DATA.DEV.V_MARKETING_CHANNEL_SPEND_AUTOMATED(
	DATE,
	TYPE,
	CATEGORY,
	CHANNEL,
	PRODUCT,
	SOURCE,
	SPEND,
	CONVERSIONS
) as 

with auto_marketing_spend as (
------ FACEBOOK (SEED)
select
date,
TYPE,
CHANNEL,
SOURCE,
campaign_id,
campaign_name,
sum(clicks) as clicks,
sum(impressions) as impressions,
sum(SPEND) as SPEND,
sum(conversions) as conversions
from
(select 
date,
'AUTOMATED' AS TYPE,
'FACEBOOK' AS CHANNEL,
'FACEBOOK' AS SOURCE,
campaign_id,
campaign_name,
sum(clicks) as clicks,
sum(impressions) as impressions,
sum(SPEND) as SPEND,
0 as conversions
from MARKETING_DATABASE.FACEBOOK.FACEBOOK_PERFORMANCE 
where account_id = '838188179702329' --- seed account
group by 1,2,3,4,5,6
UNION ALL
select 
conv.DATE,
'AUTOMATED' AS TYPE,
'FACEBOOK' AS CHANNEL,
'FACEBOOK' AS SOURCE,
conv.CAMPAIGN_ID,
camp.campaign_name,
0 as clicks,
0 as impressions,
0 as SPEND,
SUM(VALUE) AS conversions
from MARKETING_DATABASE.FACEBOOK.FACEBOOK_PERFORMANCE_ACTIONS as conv
left join (SELECT DISTINCT CAMPAIGN_ID,campaign_name  FROM MARKETING_DATABASE.FACEBOOK.FACEBOOK_PERFORMANCE where account_id = '838188179702329') as camp
on conv.CAMPAIGN_ID = camp.CAMPAIGN_ID
WHERE conv.CAMPAIGN_ID IN (SELECT DISTINCT CAMPAIGN_ID FROM MARKETING_DATABASE.FACEBOOK.FACEBOOK_PERFORMANCE where account_id = '838188179702329')
AND ACTION_TYPE = 'purchase'
AND (camp.campaign_name like 'DS01%' or  camp.campaign_name like 'PDS08%')
group by 1,2,3,4,5,6
) group by 1,2,3,4,5,6

UNION ALL 
------ FACEBOOK (GEISTM)
select
date,
TYPE,
CHANNEL,
SOURCE,
campaign_id,
campaign_name,
sum(clicks) as clicks,
sum(impressions) as impressions,
sum(SPEND) as SPEND,
sum(conversions) as conversions
from
(select 
date,
'AUTOMATED' AS TYPE,
'FACEBOOK' AS CHANNEL,
'GEIST' AS SOURCE,
campaign_id,
campaign_name,
sum(clicks) as clicks,
sum(impressions) as impressions,
sum(SPEND) as SPEND,
0 as conversions
from MARKETING_DATABASE.FACEBOOK.FACEBOOK_PERFORMANCE 
where account_id = '1077704326482692' --- geist account
group by 1,2,3,4,5,6
UNION ALL
select 
conv.DATE,
'AUTOMATED' AS TYPE,
'FACEBOOK' AS CHANNEL,
'GEIST' AS SOURCE,
conv.CAMPAIGN_ID,
camp.campaign_name,
0 as clicks,
0 as impressions,
0 as SPEND,
SUM(VALUE) AS conversions
from MARKETING_DATABASE.FACEBOOK.FACEBOOK_PERFORMANCE_ACTIONS as conv
left join (SELECT DISTINCT CAMPAIGN_ID,campaign_name  FROM MARKETING_DATABASE.FACEBOOK.FACEBOOK_PERFORMANCE where account_id = '1077704326482692') as camp
on conv.CAMPAIGN_ID = camp.CAMPAIGN_ID
WHERE conv.CAMPAIGN_ID IN (SELECT DISTINCT CAMPAIGN_ID FROM MARKETING_DATABASE.FACEBOOK.FACEBOOK_PERFORMANCE where account_id = '1077704326482692')
AND ACTION_TYPE = 'purchase'
--AND (camp.campaign_name like 'DS01%' or  camp.campaign_name like 'PDS08%')
group by 1,2,3,4,5,6
) group by 1,2,3,4,5,6

UNION ALL
------ GOOGLE ADS (GEISTM)
select 
gadw.date,
'AUTOMATED' AS TYPE,
'GOOGLE ADS' AS CHANNEL,
'GEIST' AS SOURCE,
gadw.id as campaign_id,
ch.name as campaign_name,
sum(gadw.clicks) as clicks,
sum(gadw.impressions) as impressions,
sum(gadw.COST_MICROS)/1000000 as SPEND,
sum(gadw.conversions) as conversions
from MARKETING_DATABASE.ADWORDS_GEIST.ADWORDS_CAMPAIGN_PERFORMANCE_REPORTS as gadw
left join ( select distinct id, name from MARKETING_DATABASE.ADWORDS_GEIST.CAMPAIGN_HISTORY) as ch
on gadw.id = ch.id
group by 1,2,3,4,5,6

UNION ALL
------ GOOGLE ADS (SEED)
select 
gadw.date,
'AUTOMATED' AS TYPE,
'GOOGLE ADS' AS CHANNEL,
'GOOGLE ADS' AS SOURCE,
gadw.id as campaign_id,
ch.name as campaign_name,
sum(gadw.clicks) as clicks,
sum(gadw.impressions) as impressions,
sum(gadw.COST_MICROS)/1000000 as SPEND,
sum(gadw.conversions) as conversions
from MARKETING_DATABASE.ADWORDS_CUSTOM_NEW_API.ADWORDS_CAMPAIGN_PERFORMANCE_REPORTS as gadw
left join ( select distinct id, name from MARKETING_DATABASE.ADWORDS_CUSTOM_NEW_API.CAMPAIGN_HISTORY where _FIVETRAN_ACTIVE = 'TRUE') as ch
on gadw.id = ch.id
group by 1,2,3,4,5,6

UNION ALL
------ MICROSOFT ADS / BING
select 
to_date(bing.date) as date,
'AUTOMATED' AS TYPE,
'BING' AS CHANNEL,
'BING' AS SOURCE,
bing.campaign_id,
bing.campaign_name,
sum(bing.clicks) as clicks,
sum(bing.impressions) as impressions,
sum(bing.SPEND) as SPEND,
sum(bing.conversions) as conversions
from MARKETING_DATABASE.BINGADS.CAMPAIGN_PERFORMANCE_DAILY_REPORT as bing
--where to_date(bing.date) = '2023-08-05'
group by 1,2,3,4,5,6

UNION ALL
------ TIKTOK
select 
to_date(tt.STAT_TIME_DAY) as date,
'AUTOMATED' AS TYPE,
'TIKTOK' AS CHANNEL,
'TIKTOK' AS SOURCE,
tt.campaign_id,
ch.campaign_name,
ifnull(sum(tt.clicks),0) as clicks,
ifnull(sum(tt.impressions),0) as impressions,
ifnull(sum(tt.spend),0) AS spend,
ifnull(sum(tt.conversion),0) as conversions
from
(select 
ROW_NUMBER() OVER (PARTITION BY campaign_id, STAT_TIME_DAY ORDER BY campaign_id) AS row_number, *
from MARKETING_DATABASE.TIKTOK_ADS.CAMPAIGN_REPORT_DAILY) as tt
left join ( select distinct campaign_id, campaign_name from MARKETING_DATABASE.TIKTOK_ADS.CAMPAIGN_HISTORY ) as ch
on tt.campaign_id = ch.campaign_id
where row_number = 1
group by 1,2,3,4,5,6

UNION ALL
------ SNAPCHAT
select
to_date(sc.date) AS date,
'AUTOMATED' AS TYPE,
'SNAPCHAT' AS CHANNEL,
'SNAPCHAT' AS SOURCE,
sc.campaign_id,
ch.name as campaign_name,
sum(sc.swipes) as clicks,
sum(sc.impressions) as impressions,
SUM(sc.SPEND)/1000000 AS SPEND,
sum(sc.CONVERSION_PURCHASES) as conversions
from MARKETING_DATABASE.SNAPCHAT_ADS.CAMPAIGN_DAILY_REPORT as sc
left join ( select distinct id, name from MARKETING_DATABASE.SNAPCHAT_ADS.CAMPAIGN_HISTORY ) as ch
on sc.campaign_id = ch.id
group by 1,2,3,4,5,6

UNION ALL
------ OUTBRAIN
select 
to_date(day) AS date,
'AUTOMATED' AS TYPE,
'OUTBRAIN' AS CHANNEL,
'OUTBRAIN' AS SOURCE,
ob.CAMPAIGN_ID,
ch.name as campaign_name,
sum(ob.clicks) as clicks,
sum(ob.impressions) as impressions,
sum(ob.SPEND) as SPEND,
sum(ob.conversions) as conversions
from MARKETING_DATABASE.OUTBRAIN.CAMPAIGN_REPORT as ob
left join ( select distinct id, name from MARKETING_DATABASE.OUTBRAIN.CAMPAIGN_history ) as ch
on ob.CAMPAIGN_ID = ch.id
group by 1,2,3,4,5,6

UNION ALL
------ REDDIT
SELECT
to_date(date) AS activity_date,
type,
channel,
source,
campaign_id,  
campaign_name,
sum(clicks) as clicks,
sum(impressions) as impressions,
sum(SPEND) as SPEND,
sum(conversions_vta)+ sum(conversions_cta) as conversions
  
FROM
(
select 
to_date(cp.date) AS date,
'AUTOMATED' AS TYPE,
'REDDIT' AS CHANNEL,
'REDDIT' AS SOURCE,
cp.CAMPAIGN_ID,
CONCAT('DS01-',camp.name) as campaign_name,
sum(cp.clicks) as clicks,
sum(cp.impressions) as impressions,
sum(cp.SPEND)/1000000 as SPEND,
0 as conversions_vta,
0 as   conversions_cta
  
--- SELECT MIN(DATE) FROM MARKETING_DATABASE.REDDIT_ADS.CAMPAIGN_PERFORMANCE
from MARKETING_DATABASE.REDDIT_ADS.CAMPAIGN_PERFORMANCE as cp
left join ( select distinct id, name from MARKETING_DATABASE.REDDIT_ADS.CAMPAIGN ) as camp
on cp.CAMPAIGN_ID = camp.id
group by 1,2,3,4,5,6
--order by 1

UNION ALL

select 
to_date(cp.date) AS date,
'AUTOMATED' AS TYPE,
'REDDIT' AS CHANNEL,
'REDDIT' AS SOURCE,
cp.CAMPAIGN_ID,
CONCAT('DS01-',camp.name) as campaign_name,
0 as clicks,
0 as impressions,
0 as SPEND,
sum(cp.VIEW_THROUGH_CONVERSION_ATTRIBUTION_WINDOW_DAY) as conversions_vta,
sum(cp.CLICK_THROUGH_CONVERSION_ATTRIBUTION_WINDOW_month) as conversions_cta
  
from MARKETING_DATABASE.REDDIT_ADS.CAMPAIGN_PERFORMANCE_CONVERSIONS as cp
left join ( select distinct id, name from MARKETING_DATABASE.REDDIT_ADS.CAMPAIGN ) as camp
on cp.CAMPAIGN_ID = camp.id
where event_name = 'purchase'
group by 1,2,3,4,5,6
) 
group by 1,2,3,4,5,6

UNION ALL
------ PINTEREST
select 
to_date(cr.date) AS date,
'AUTOMATED' AS TYPE,
'PINTEREST' AS CHANNEL,
'PINTEREST' AS SOURCE,
cr.CAMPAIGN_ID,
cr.CAMPAIGN_NAME as campaign_name,
sum(cr.CLICKTHROUGH_1_GROSS) as clicks,
sum(cr.IMPRESSION_1_GROSS) as impressions,
sum(cr.SPEND_IN_MICRO_DOLLAR)/1000000 as SPEND,
--sum(cr.TOTAL_CONVERSIONS) as conversions
sum(cr.TOTAL_WEB_CHECKOUT) as conversions
from MARKETING_DATABASE.PINTEREST_ADS_REPORTING.CAMPAIGN_REPORT as cr
group by 1,2,3,4,5,6

UNION ALL
------ TRADEDESK HOLD
select 
to_date(date) AS date,
'AUTOMATED' AS TYPE,
'TRADEDESK' AS CHANNEL,
'TRADEDESK' AS SOURCE,
null as CAMPAIGN_ID,
'DS01' as campaign_name,
0 as clicks,
0 as impressions,
0 as SPEND,
0 as conversions
from SEED_DATA.DEV.DIM_DATE
group by 1,2,3,4,5,6
)

SELECT
d.date,
ms.type,
'PERFORMANCE' as category,
ms.channel,
--null as campaign_id,
--ms.campaign_name,
case when campaign_name like '%DS01%' then 'DS-01'
     when campaign_name like '%PDS08%' then 'PDS-08'
     else Null end as product,
ms.source,
--ifnull(sum(ms.clicks),0) as clicks,
--ifnull(sum(ms.impressions),0) as impressions,
ifnull(sum(ms.spend),0) as spend,
ifnull(sum(ms.conversions),0) as conversions
FROM SEED_DATA.DEV.DIM_DATE as d
left join auto_marketing_spend as ms
on d.date = ms.date
where d.date between '2018-01-01' and to_date(current_date())
group by 1,2,3,4,5,6
order by 1 desc 
;