create or replace view SEED_DATA.DEV.V_MARKETING_CHANNEL_SPEND_ALL(
	PREV_MTD_FLAG,
	CURR_MTD_FLAG,
	DATE,
	TYPE,
	CATEGORY,
	CHANNEL,
	PRODUCT,
	SOURCE,
	SPEND,
	CONVERSIONS
) as 

with combined_spend as (
SELECT * FROM SEED_DATA.DEV.V_MARKETING_CHANNEL_SPEND_AUTOMATED
UNION ALL
SELECT * FROM SEED_DATA.DEV.V_MARKETING_CHANNEL_SPEND_MANUAL
)

select 
case when date between DATEADD(MONTH, -1, date_trunc('month',current_date())) and DATEADD(MONTH, -1, current_date()-1) then 1 else 0 end as Prev_MTD_Flag,
case when date between DATEADD(MONTH, 0, date_trunc('month',current_date())) and DATEADD(MONTH, 0, current_date()-1) then 1 else 0 end as Curr_MTD_Flag,
*
from combined_spend
where concat(SOURCE,'-',TYPE) <> 'PINTEREST-MANUAL'
and date <= current_date()-1
;