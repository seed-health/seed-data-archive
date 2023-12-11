create or replace view SEED_DATA.DEV.V_KUSTOMER_CONVERSATIONS as 

--with kustomer_conversation as (
select 
     c.ID as Conversation_ID,
     c.Name as Conversation_Name,
     c.STATUS as Conversation_Status,
     c.Snooze_Status as Snooze_Status,
     c.ended as Conversation_Ended,
     c.SATISFACTION_LEVEL_CHANNEL as Satisfaction_Level_Channel,
     c.SATISFACTION_LEVEL_STATUS as Satisfaction_Level_Status,
     c.DIRECTION as Direction,
     c.PRIORITY as Priority,
     c.LAST_MESSAGE_DIRECTION as Last_Message_Direction,
     c.FIRST_MESSAGE_IN_ID as First_Message_In_ID,
     c.SATISFACTION_LEVEL_FIRST_ANSWER as Satisfaction_Awnser, 
     c.ORG_ID as Org_ID,
     c.Created_By as Created_By,
     KU_E.Name as Created_By_Name,
     c.MODIFIED_BY as Modified_By,
     KU.name as Modified_By_Name,
  -------Add in Modified Persons Name
     c.EXTERNAL_ID,
     c.CUSTOMER_ID as Customer_Id,
  
     
     c.SNOOZE_COUNT as Snooze_Count,  
     c.REOPEN_COUNT as Reopen_Count,
     c.message_count as Message_Count,
     c.note_count as Note_Count,
     c.Satisfaction as Satisfaction,
     c.OUTBOUND_MESSAGE_COUNT as Outbound_Message_Count,
     c.message_count-c.OUTBOUND_MESSAGE_COUNT as Inbound_Message_Count,
     c.SATISFACTION_LEVEL_RATING as Satisfaction_Level_Rating,
     c.SATISFACTION_LEVEL_Score as Satisfaction_Level_Score,
     c.SLA_BREACHED as SLA_Breached,   
     c.SLA_BREACH_METRIC as SLA_Breach_Metric,
     c. CUSTOM_PRODUCT_NAME_TREE as Product,
     c.FIRST_RESPONSE_TIME as First_Response_Time,
     c.FIRST_RESPONSE_RESPONSE_TIME as First_Response_Response_Date,

  -----Joined Message Table ---------------
     m.direction_type, 
     m.channel,
  
  -----Breaking out customer contact reasons ---------------
     CUSTOM_CATEGORY_TREE as Category_Hierarchy,
     CUSTOM_ACTION_TAKEN_TREE as Category_Action_Hierarchy,
     split_part(CUSTOM_CATEGORY_TREE, '.',1) as Category_Level_1 , 
     split_part(CUSTOM_CATEGORY_TREE, '.',2) as Category_Level_2 ,
     split_part(CUSTOM_CATEGORY_TREE, '.',3) as Category_Level_3 ,
     split_part(CUSTOM_CATEGORY_TREE, '.',4) as Category_Level_4 ,
     concat(split_part(CUSTOM_CATEGORY_TREE, '.',1) ,'>',split_part(CUSTOM_CATEGORY_TREE, '.',2)) as Cat_Hierarchy_1_2,
     concat(split_part(CUSTOM_CATEGORY_TREE, '.',1) ,'>',split_part(CUSTOM_CATEGORY_TREE, '.',2),'>',split_part(CUSTOM_CATEGORY_TREE, '.',3)) as Cat_Hierarchy_1_3,
     concat(split_part(CUSTOM_CATEGORY_TREE, '.',1) ,'>',split_part(CUSTOM_CATEGORY_TREE, '.',2),'>',split_part(CUSTOM_CATEGORY_TREE, '.',3),'>',split_part(CUSTOM_CATEGORY_TREE, '.',4)) as Cat_Hierarchy_1_4,
     split_part(CUSTOM_ACTION_TAKEN_TREE, '.',1) as Category_Action_Taken_L1 , 
     split_part(CUSTOM_ACTION_TAKEN_TREE, '.',2) as Category_Action_Taken_L2 ,
     split_part(CUSTOM_ACTION_TAKEN_TREE, '.',3) as Category_Action_Taken_L3 ,
     split_part(CUSTOM_ACTION_TAKEN_TREE, '.',4) as Category_Action_Taken_L4 ,
  
  -----Times and Dates ---------------
     to_date(CONVERT_TIMEZONE('America/New_York',c.Created_at)) as Conversation_Created_at_Date,
     CONVERT_TIMEZONE('America/New_York',c.Created_at) as Conversation_Created_at_Timestamp,        
     to_date(CONVERT_TIMEZONE('America/New_York',c.LAST_ACTIVITY_AT)) as Conversation_Last_Activity_Date,
     CONVERT_TIMEZONE('America/New_York',c.LAST_ACTIVITY_AT) as Conversation_Last_Activity_Timestamp,
     to_date(CONVERT_TIMEZONE('America/New_York',m.SENT_AT)) as Message_Sent_Date,
     CONVERT_TIMEZONE('America/New_York',m.SENT_AT) as Message_Sent_Timestamp,     
     to_date(CONVERT_TIMEZONE('America/New_York',c.SNOOZE_STATUS_AT)) as Snooze_Status_at_Date,
     CONVERT_TIMEZONE('America/New_York',c.SNOOZE_STATUS_AT) as Snooze_Status_AT_Timestamp,        
     to_date(CONVERT_TIMEZONE('America/New_York',c.SNOOZE_TIME)) as Snooze_Time_Date,
     CONVERT_TIMEZONE('America/New_York',c.SNOOZE_TIME) as Snooze_Time_Timestamp,          
     to_date(CONVERT_TIMEZONE('America/New_York',c.SATISFACTION_LEVEL_UPDATED_AT)) as Satisfaction_Level_Sent_Date,
     CONVERT_TIMEZONE('America/New_York',c.SATISFACTION_LEVEL_UPDATED_AT) as Satisfaction_Level_Sent_Timestamp,         
     to_date(CONVERT_TIMEZONE('America/New_York',c.FIRST_MESSAGE_IN_CREATED_AT)) as First_Message_Created_At_Date,
     CONVERT_TIMEZONE('America/New_York',c.FIRST_MESSAGE_IN_CREATED_AT) as First_Message_Created_At_Timestamp,  
     to_date(CONVERT_TIMEZONE('America/New_York',c.FIRST_MESSAGE_IN_SENT_AT)) as First_Message_Sent_In_Date,
     CONVERT_TIMEZONE('America/New_York',c.FIRST_MESSAGE_IN_SENT_AT) as First_Message_Sent_In_Timestamp,              
     to_date(CONVERT_TIMEZONE('America/New_York',c.FIRST_RESPONSE_SENT_AT)) as First_Response_Sent_At_Date,
     CONVERT_TIMEZONE('America/New_York',c.FIRST_RESPONSE_SENT_AT) as First_Response_Sent_At_Timestamp,     
     to_date(CONVERT_TIMEZONE('America/New_York',c.LAST_MESSAGE_OUT_SENT_AT)) as Last_Message_Out_Sent_At_Date,
     CONVERT_TIMEZONE('America/New_York',c.LAST_MESSAGE_OUT_SENT_AT) as Last_Message_Out_Sent_At_Timestamp,
     to_date(CONVERT_TIMEZONE('America/New_York',c.LAST_MESSAGE_IN_SENT_AT)) as Last_Message_In_Sent_At_Date,
     CONVERT_TIMEZONE('America/New_York',c.LAST_MESSAGE_IN_SENT_AT) as Last_Message_In_Sent_At_Timestamp,
     to_date(CONVERT_TIMEZONE('America/New_York',c.SLA_BREACH_AT)) as SLA_Breach_At_Date,
     CONVERT_TIMEZONE('America/New_York',c.SLA_BREACH_AT) as SLA_Breach_At_Timestamp,
     to_date(CONVERT_TIMEZONE('America/New_York',c.SLA_METRICS_TOTAL_CUSTOMER_WAIT_TIME_BREACH_AT)) as SLA_Metric_Total_Cust_Wait_Breach_At_Date,
     CONVERT_TIMEZONE('America/New_York',c.SLA_METRICS_TOTAL_CUSTOMER_WAIT_TIME_BREACH_AT) as SLA_Metric_Total_Cust_Wait_Breach_At_Timestamp          
 
from MARKETING_DATABASE.KUSTOMER.CONVERSATION as c
left join MARKETING_DATABASE.KUSTOMER.MESSAGE as m
on c.id = m.conversation_id
left join MARKETING_DATABASE.KUSTOMER.USER as KU
on c.modified_by = KU.id  
left join MARKETING_DATABASE.KUSTOMER.USER as KU_E
on c.created_by = KU_E.id