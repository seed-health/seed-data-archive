create or replace view MARKETING_DATABASE.PUBLIC.NEWTESTVIEWTWO as 

select *
/*got rid of the distinct to account for proliferation of records*/
from "MARKETING_DATABASE"."PUBLIC"."TEST_VIEW_THREE" as tv
left join (

select 
*
/*debugged the joins here*/
from "MARKETING_DATABASE"."PUBLIC"."FILTERVIEW_TWO" ) as fv on tv.customer_email = fv.email and fv.subscription_id = tv.RECURLY_SUBSCRIPTION_ID;