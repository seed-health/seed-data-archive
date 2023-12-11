create or replace view MARKETING_DATABASE.PUBLIC.V_KLAVIYOCRM_CAMPAIGNS as
//select to_date(SEND_TIME) as SENDTIME, FROM_NAME, FROM_EMAIL,NAME,SUBJECT,ID from MARKETING_DATABASE.KLAVIYO.CAMPAIGN

select to_date(SEND_TIME) as SENDTIME, FROM_NAME, FROM_EMAIL,NAME,SUBJECT,ID from MARKETING_DATABASE.KLAVIYO.CAMPAIGN
WHERE status_label = 'Sent' AND


SENDTIME >= '2020-01-01'
ORDER BY SENDTIME ASC


//select * from MARKETING_DATABASE.PUBLIC.V_KLAVIYOCRM_CAMPAIGNS