CREATE VIEW google_analytics_web_metrics as 
select date,
avg(avg_session_duration) as Session_Duration,
avg(pageviews_per_session) as Pageviews_per_Session,
avg(avg_page_load_time) as Page_Load_Time,
avg(bounce_rate) as Bounce_Rate
from "MARKETING_DATABASE"."GOOGLE_ANALYTICS_WEB_METRICS"."GOOGLE_ANALYTIS_WEB_METRICS"
group by date
order by date desc