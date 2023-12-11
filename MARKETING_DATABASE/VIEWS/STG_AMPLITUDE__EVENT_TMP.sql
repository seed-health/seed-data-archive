create or replace  view MARKETING_DATABASE.amplitude__source_amplitude.stg_amplitude__event_tmp
  
   as (
    select * 
from MARKETING_DATABASE.amplitude.event
  );