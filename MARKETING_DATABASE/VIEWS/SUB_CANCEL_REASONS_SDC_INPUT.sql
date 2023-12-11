create or replace view MARKETING_DATABASE.PUBLIC.SUB_CANCEL_REASONS_SDC_INPUT(
	YEAR_MONTH_CANCELLATION,
    PRODUCT,
    CUSTOER_AGE_MONTH_CATEGORY,
    REASON_CATEGORY,
    SUBSCRIPTION_CANCEL_COUNT
) as

with all_raw_data as (
  select *,case when reason in ('Health situation has changed (pregnancy, illness, medication)',
                              'I\’m following a doctor\'s recommendation',
                              'I’m traveling or moving') then 'Change in Personal Situation'
              when reason in ('I decided to try a different probiotic',
                              'I no longer want a subscription to Seed') then 'Competition'
              when reason in ('Didn’t notice a difference',
                              'I\'m experiencing discomfort',
                              'I’m not noticing a difference',
                               'I’m not noticing any improvement') then 'Efficacy/Discomfort'
              when reason in ('I am unable to continue, for financial reasons',
                              'My financial situation has changed since I started',
                              'Too expensive') then 'Financial'
              when reason in ('I\'m no longer taking probiotics') then 'No longer taking probiotics'
              when reason in ('I still have product on hand, and am not ready for a refill') then 'Not ready for refill'
              when reason in ('Other','Other - leave a comment on the next page') then 'Other'
              when reason in ('I had technical issues, billing issues, or trouble logging in', 
                              'I\'ve run into a delivery, billing, account, or technical issue',
                              'Inconsistent product delivery',
                              'Wasn’t aware this was a subscription') then 'UX'
              else 'UNKNOWN' end as reason_category
        ,datediff('day',activated_at,canceled_at) as customer_age_days, 
        floor(customer_age_days/30) as customer_age_month,
        case when customer_age_month = 0 then '0'
             when customer_age_month = 1 then '1'
             when customer_age_month = 2 then '2'
             when customer_age_month < 6 then '3-5'
             when customer_age_month < 12 then '6-11'
             when customer_age_month < 18 then '12-17'
             when customer_age_month < 24 then '18-23'
             else '24+' end as customer_age_month_category,left(CANCELED_FORM_SUBMITTED_DATE,7) as year_month_cancellation
from "MARKETING_DATABASE"."PUBLIC"."SUB_CANCEL_REASONS_ANALYSIS")

select year_month_cancellation,product,customer_age_month_category,reason_category,count(SUBSCRIPTION_UUID) as subscription_cancel_count
from all_raw_data
group by year_month_cancellation,product,customer_age_month_category,reason_category
order by year_month_cancellation,product,customer_age_month_category,reason_category;