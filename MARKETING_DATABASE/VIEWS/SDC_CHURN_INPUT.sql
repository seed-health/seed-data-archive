create or replace view MARKETING_DATABASE.PUBLIC.SDC_CHURN_INPUT(
    DAY,
	"'Health situation has changed (pregnancy, illness, medication)'",
    "'I’m following a doctor''s recommendation'",
    "'Inconsistent product delivery'",
    "'Other'",
    "'I''ve run into a delivery, billing, account, or technical issue'",
    "'I no longer want a subscription to Seed'",
    "'Didn’t notice a difference'",
    "'I decided to try a different probiotic'",
    "'Too expensive'",
    "'I am unable to continue, for financial reasons'",
    "'I’m not noticing a difference'",
    "'I''m no longer taking probiotics'",
    "'I’m traveling or moving'",
    "'I had technical issues, billing issues, or trouble logging in'",
    "'Other - leave a comment on the next page'",
    "'I''m experiencing discomfort'",
    "'Wasn’t aware this was a subscription'",
    "'I’m not noticing any improvement'",
    "'My financial situation has changed since I started'",
    "'I still have product on hand, and am not ready for a refill'",
    PRODUCT,
	STP_FLAG
) as

with DS_baseline as 
(
  select left(canceled_form_submitted_date,10) as day, reason,count(distinct subscription_uuid) as counts
  from MARKETING_DATABASE.PUBLIC.SUB_CANCEL_REASONS_ANALYSIS
  where product = 'DS-01' and stp_flag = 'Baseline'
  group by day, reason
  order by day, reason
),

DS_stp as 
(
  select left(canceled_form_submitted_date,10) as day, reason,count(distinct subscription_uuid) as counts
  from MARKETING_DATABASE.PUBLIC.SUB_CANCEL_REASONS_ANALYSIS
  where product = 'DS-01' and stp_flag = 'STP'
  group by day, reason
  order by day, reason
),

PDS as
(
  select left(canceled_form_submitted_date,10) as day, reason,count(distinct subscription_uuid) as counts
  from MARKETING_DATABASE.PUBLIC.SUB_CANCEL_REASONS_ANALYSIS
  where product = 'PDS-08'
  group by day, reason
  order by day, reason
),

final_table as (
select *, 'DS-01' as product,0 as STP_flag
from DS_baseline
    PIVOT (sum(counts) for reason in ('Health situation has changed (pregnancy, illness, medication)',
'I’m following a doctor\'s recommendation',
'Inconsistent product delivery',
'Other',
'I\'ve run into a delivery, billing, account, or technical issue',
'I no longer want a subscription to Seed',
'Didn’t notice a difference',
'I decided to try a different probiotic',
'Too expensive',
'I am unable to continue, for financial reasons',
'I’m not noticing a difference',
'I\'m no longer taking probiotics',
'I’m traveling or moving',
'I had technical issues, billing issues, or trouble logging in',
'Other - leave a comment on the next page',
'I\'m experiencing discomfort',
'Wasn’t aware this was a subscription',
'I’m not noticing any improvement',
'My financial situation has changed since I started',
'I still have product on hand, and am not ready for a refill' ))
                                      
UNION ALL

select *, 'DS-01' as product, 1 as STP_flag
from DS_stp
    PIVOT (sum(counts) for reason in ('Health situation has changed (pregnancy, illness, medication)',
'I’m following a doctor\'s recommendation',
'Inconsistent product delivery',
'Other',
'I\'ve run into a delivery, billing, account, or technical issue',
'I no longer want a subscription to Seed',
'Didn’t notice a difference',
'I decided to try a different probiotic',
'Too expensive',
'I am unable to continue, for financial reasons',
'I’m not noticing a difference',
'I\'m no longer taking probiotics',
'I’m traveling or moving',
'I had technical issues, billing issues, or trouble logging in',
'Other - leave a comment on the next page',
'I\'m experiencing discomfort',
'Wasn’t aware this was a subscription',
'I’m not noticing any improvement',
'My financial situation has changed since I started',
'I still have product on hand, and am not ready for a refill' ))

UNION ALL 

select *, 'PDS-08' as product, 0 as STP_flag
from PDS
    PIVOT (sum(counts) for reason in ('Health situation has changed (pregnancy, illness, medication)',
'I’m following a doctor\'s recommendation',
'Inconsistent product delivery',
'Other',
'I\'ve run into a delivery, billing, account, or technical issue',
'I no longer want a subscription to Seed',
'Didn’t notice a difference',
'I decided to try a different probiotic',
'Too expensive',
'I am unable to continue, for financial reasons',
'I’m not noticing a difference',
'I\'m no longer taking probiotics',
'I’m traveling or moving',
'I had technical issues, billing issues, or trouble logging in',
'Other - leave a comment on the next page',
'I\'m experiencing discomfort',
'Wasn’t aware this was a subscription',
'I’m not noticing any improvement',
'My financial situation has changed since I started',
'I still have product on hand, and am not ready for a refill'))

order by day,product,STP_flag)

select *
 /*DAY,
	"'Health situation has changed (pregnancy, illness, medication)'",
    "'I’m following a doctor''s recommendation'",
    "'Inconsistent product delivery'",
    "'Other'",
    "'I''ve run into a delivery, billing, account, or technical issue'",
    "'I no longer want a subscription to Seed'",
    "'Didn’t notice a difference'",
    "'I decided to try a different probiotic'",
    "'Too expensive'",
    "'I am unable to continue, for financial reasons'",
    "'I’m not noticing a difference'",
    "'I''m no longer taking probiotics'",
    "'I’m traveling or moving'",
    "'I had technical issues, billing issues, or trouble logging in'",
    "'Other - leave a comment on the next page'",
    "'I''m experiencing discomfort'",
    "'Wasn’t aware this was a subscription'",
    "'I’m not noticing any improvement'",
    "'My financial situation has changed since I started'",
    "'I still have product on hand, and am not ready for a refill'",
	PRODUCT,
	STP_FLAG*/
from final_table;