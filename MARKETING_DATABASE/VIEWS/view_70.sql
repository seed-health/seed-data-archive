create view "MARKETING_DATABASE"."PUBLIC".V_KLAVIYOCRM
AS
select to_date(DATETIME) as SEND_DATE,
COUNT(CASE WHEN TYPE = 'Recurly Start Subscription' THEN 1 END) AS E_NEWSUB,
COUNT(CASE WHEN TYPE = 'Subscription Reactivated' THEN 1 END) AS E_REACTIVATE,
COUNT(CASE WHEN TYPE = 'STP Enrollment' THEN 1 END) AS E_STP_ENROLL,
COUNT(CASE WHEN TYPE = 'Recurly Subscription Cancellation' THEN 1 END) AS E_CANCEL
FROM MARKETING_DATABASE.KLAVIYO.EVENT
WHERE PROPERTY_ATTRIBUTION IS NOT NULL
AND SEND_DATE > '2022-05-31'
GROUP BY SEND_DATE
order by SEND_DATE ASC