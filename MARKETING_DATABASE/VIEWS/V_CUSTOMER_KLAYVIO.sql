create view v_customer_klayvio as

select a.email,
case when a.custom_consent like '%email%' then true else false end as Email_Opt_in, --accepts_marketing
case when a.custom_consent like '%mobile%' then true else false end as SMS_Opt_in,--accepts_marketing
M_open_date, M_click_date, M_received_date, e.open_rate, f.click_rate
from "MARKETING_DATABASE"."KLAVIYO"."PERSON" as a
join
  (select p.email, e.type, to_date(max(e.datetime)) as M_open_date
  from "MARKETING_DATABASE"."KLAVIYO"."EVENT" as e
  join "MARKETING_DATABASE"."KLAVIYO"."PERSON" as p on p.id = e.person_id
  where e.type = 'Opened Email' /*and p.email = 'christascreative@gmail.com'*/
  group by p.email, e.type) b on b.email = a.email
 
  join
    (select p2.email, to_date(max(e2.datetime)) as M_click_date
     from "MARKETING_DATABASE"."KLAVIYO"."EVENT" as e2
     join "MARKETING_DATABASE"."KLAVIYO"."PERSON" as p2 on p2.id = e2.person_id
     where e2.type = 'Clicked Email' /*and p2.email = 'christascreative@gmail.com'*/
     group by p2.email) c on c.email = b.email
     
          join
            (select p3.email, to_date(max(e3.datetime)) as M_received_date
             from "MARKETING_DATABASE"."KLAVIYO"."EVENT" as e3
             join "MARKETING_DATABASE"."KLAVIYO"."PERSON" as p3 on p3.id = e3.person_id
             where e3.type = 'Received Email' /*and p3.email = 'christascreative@gmail.com'*/
             group by p3.email) d on d.email = c.email
                           
                            join
                                (select (opened.emails_opened/received.emails_received)*100 as open_rate, opened.email as email
                                  from (

                                     select email, count (distinct campaign_id) as emails_opened
                                      from (

                                          select distinct campaign_id, type, p.email
                                          from "MARKETING_DATABASE"."KLAVIYO"."EVENT" as e
                                          join "MARKETING_DATABASE"."KLAVIYO"."CAMPAIGN" as c
                                          on e.campaign_id = c.id
                                          join "MARKETING_DATABASE"."KLAVIYO"."PERSON" as p
                                          on p.id = e.person_id
                                          where /*p.email = 'christascreative@gmail.com' and*/ type = 'Opened Email'

                                          union

                                          select distinct flow_id, type, p.email
                                          from "MARKETING_DATABASE"."KLAVIYO"."EVENT" as e
                                          join "MARKETING_DATABASE"."KLAVIYO"."FLOW" as f
                                          on e.flow_id = f.id
                                          join "MARKETING_DATABASE"."KLAVIYO"."PERSON" as p
                                          on p.id = e.person_id
                                          where /*p.email = 'christascreative@gmail.com' and*/ type = 'Opened Email')
                                          group by email ) as opened

                                      join (

                                      select email, count (distinct campaign_id) as emails_received
                                      from (
                                      select distinct campaign_id, email
                                      from "MARKETING_DATABASE"."KLAVIYO"."EVENT" as e
                                      join "MARKETING_DATABASE"."KLAVIYO"."CAMPAIGN" as c
                                      on e.campaign_id = c.id
                                      join "MARKETING_DATABASE"."KLAVIYO"."PERSON" as p
                                      on p.id = e.person_id
                                      /*where p.email = 'christascreative@gmail.com'*/

                                        union

                                     select distinct flow_id, email
                                      from "MARKETING_DATABASE"."KLAVIYO"."EVENT" as e
                                      join "MARKETING_DATABASE"."KLAVIYO"."FLOW" as f
                                      on e.flow_id = f.id
                                      join "MARKETING_DATABASE"."KLAVIYO"."PERSON" as p
                                      on p.id = e.person_id
                                      /*where p.email = 'christascreative@gmail.com'*/ )
                                      group by email ) as received on received.email = opened.email
                                      ) as e on d.email = e.email
                                     
                                      join
                                     
                                        ( select (clicked.emails_clicked/received.emails_received)*100 as click_rate, clicked.email as email
                                            from (

                                           select email, count (distinct campaign_id) as emails_clicked
                                            from (

                                                select distinct campaign_id, type, p.email
                                                from "MARKETING_DATABASE"."KLAVIYO"."EVENT" as e
                                                join "MARKETING_DATABASE"."KLAVIYO"."CAMPAIGN" as c
                                                on e.campaign_id = c.id
                                                join "MARKETING_DATABASE"."KLAVIYO"."PERSON" as p
                                                on p.id = e.person_id
                                                where /*p.email = 'christascreative@gmail.com' and*/ type = 'Clicked Email'

                                                union

                                                select distinct flow_id, type, p.email
                                                from "MARKETING_DATABASE"."KLAVIYO"."EVENT" as e
                                                join "MARKETING_DATABASE"."KLAVIYO"."FLOW" as f
                                                on e.flow_id = f.id
                                                join "MARKETING_DATABASE"."KLAVIYO"."PERSON" as p
                                                on p.id = e.person_id
                                                where /*p.email = 'christascreative@gmail.com' and*/ type = 'Clicked Email')
                                                group by email ) as clicked

                                            join (

                                            select email, count (distinct campaign_id) as emails_received
                                            from (
                                            select distinct campaign_id, email
                                            from "MARKETING_DATABASE"."KLAVIYO"."EVENT" as e
                                            join "MARKETING_DATABASE"."KLAVIYO"."CAMPAIGN" as c
                                            on e.campaign_id = c.id
                                            join "MARKETING_DATABASE"."KLAVIYO"."PERSON" as p
                                            on p.id = e.person_id
                                            /*where p.email = 'christascreative@gmail.com'*/

                                              union

                                           select distinct flow_id, email
                                            from "MARKETING_DATABASE"."KLAVIYO"."EVENT" as e
                                            join "MARKETING_DATABASE"."KLAVIYO"."FLOW" as f
                                            on e.flow_id = f.id
                                            join "MARKETING_DATABASE"."KLAVIYO"."PERSON" as p
                                            on p.id = e.person_id
                                            /*where p.email = 'christascreative@gmail.com'*/ )
                                            group by email ) as received on received.email = clicked.email) as f on f.email = e.email