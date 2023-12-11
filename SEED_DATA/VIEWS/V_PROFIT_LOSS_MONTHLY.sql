create or replace view SEED_DATA.DEV.V_PROFIT_LOSS_MONTHLY as 

with pl_monthly as (
select * from MARKETING_DATABASE.GOOGLE_SHEETS.PROFIT_LOSS_MONTHLY )

, pl_account_mapping as (
select 
account_no,
account_no_name,
account_name,
account_group,
account_no_name_org
from 
MARKETING_DATABASE.GOOGLE_SHEETS.PL_ACCOUNT_MAPPING)

select 
pl.month_year,
account_no,
account_name,
account_no_name,
pmap.account_group,
pl.account_no_name_org,
ifnull(sum(value),0) as value

from 
pl_monthly as pl
left join pl_account_mapping pmap
on pl.account_no_name_org = pmap.account_no_name_org

--where --pl.month_year = '2023-05-01' and
--pmap.account_group = 'Product COGS'

group by 1,2,3,4,5,6
order by 1 desc;