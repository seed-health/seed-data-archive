create or replace view SEED_DATA.DEV.V_EVENTS_ORDER_COMPLETE(
	UTM_SOURCE_MEDIUM,
	CHANNEL_GROUPING,
	CHANNEL_PLATFORM,
	EVENT_DATE,
	EVENT_MONTHDATE,
	ACTIVE_SKU,
	CREATED_AT,
	DATE_JOINED,
	EMAIL_USER_PROP,
	HAS_ACTIVE_DS01_SUBSCRIPTION,
	HAS_ACTIVE_PDS08_SUBSCRIPTION,
	HAS_PAUSED_SUBSCRIPTION,
	HIGHEST_ACTIVE_DS01_PERCENT_COUPON,
	INITIAL_REFERRER,
	INITIAL_UTM_CAMPAIGN,
	INITIAL_UTM_CONTENT,
	INITIAL_UTM_MEDIUM,
	INITIAL_UTM_SOURCE,
	INITIAL_UTM_TERM,
	MOST_RECENT_PURCHASE_DATE,
	MOST_RECENT_PURCHASE_SKU,
	FULL_NAME,
	PAUSED_SKUS,
	REFERRER,
	SUBSCRIPTION_COUNT,
	TOTAL_ORDERS_COUNT,
	USERNAME,
	UTM_CAMPAIGN,
	UTM_CONTENT,
	UTM_MEDIUM,
	UTM_SOURCE,
	UTM_TERM,
	ZIP,
	PRICE,
	PRODUCT_ID,
	QUANTITY,
	REVENUE,
	REVENUE_TYPE,
	AFFILIATION,
	COUPON,
	CURRENCY,
	DISCOUNT,
	EMAIL,
	EXTERNAL_ID,
	FIRST_NAME,
	LAST_NAME,
	ORDER_ID,
	PAYMENT_METHOD,
	POSTAL_CODE,
	TOTAL_REVENUE,
	SHIPPING,
	STATE,
	SUBTOTAL,
	TAX,
	TOTAL,
	TOTAL_DS01_QUANTITY,
	TOTAL_ORDER_QUANTITY,
	TOTAL_PDS08_QUANTITY,
	DEVICE_ID,
	ID,
	SERVER_UPLOAD_TIME,
	CLIENT_EVENT_TIME,
	AMPLITUDE_ID,
	EVENT_TYPE_ID,
	USER_ID,
	PROJECT_NAME,
	AMPLITUDE_EVENT_TYPE,
	LIBRARY,
	START_VERSION,
	LOCATION_LNG,
	LOCATION_LAT,
	CITY,
	UUID,
	IS_ATTRIBUTION_EVENT,
	IDFA,
	PAYING,
	IP_ADDRESS,
	LANGUAGE,
	APP,
	COUNTRY,
	REGION,
	SESSION_ID,
	VERSION_NAME,
	SAMPLE_RATE,
	AD_ID,
	OS_VERSION,
	EVENT_TYPE,
	DMA,
	SCHEMA,
	DEVICE_BRAND,
	DEVICE_TYPE,
	DEVICE_MANUFACTURER,
	DEVICE_MODEL,
	DEVICE_CARRIER,
	DEVICE_FAMILY,
	OS_NAME,
	PLATFORM,
	CLIENT_UPLOAD_TIME,
	SERVER_RECEIVED_TIME,
	PROCESSED_TIME,
	EVENT_TIME,
	USER_CREATION_TIME,
	DATA,
	GROUPS,
	_INSERT_ID,
	GROUP_PROPERTIES,
	EVENT_PROPERTIES,
	USER_PROPERTIES,
	_FIVETRAN_SYNCED
) as 

with amplitude_events as (
select 
--- USER_PROPERTIES json
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:activeSkus[0], '"', '')) AS active_sku,
  --LTRIM(REGEXP_REPLACE(USER_PROPERTIES:city, '"', '')) AS city,
  --LTRIM(REGEXP_REPLACE(USER_PROPERTIES:country, '"', '')) AS country,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:createdAt::timestamp, '"', '')) AS created_at,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:dateJoined::timestamp, '"', '')) AS date_joined,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:email, '"', '')) AS email_user_prop,
  --LTRIM(REGEXP_REPLACE(USER_PROPERTIES:firstName, '"', '')) AS first_name,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:hasActiveDS01Subscription::boolean, '"', '')) AS has_active_ds01_subscription,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:hasActivePDS08Subscription::boolean, '"', '')) AS has_active_pds08_subscription,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:hasPausedSubscription::boolean, '"', '')) AS has_paused_subscription,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:highestActiveDS01PercentCoupon, '"', '')) AS highest_active_ds01_percent_coupon,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:initial_referrer, '"', '')) AS initial_referrer,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:initial_utm_campaign, '"', '')) AS initial_utm_campaign,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:initial_utm_content, '"', '')) AS initial_utm_content,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:initial_utm_medium, '"', '')) AS initial_utm_medium,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:initial_utm_source, '"', '')) AS initial_utm_source,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:initial_utm_term, '"', '')) AS initial_utm_term,
  --LTRIM(REGEXP_REPLACE(USER_PROPERTIES:lastName, '"', '')) AS last_name,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:mostRecentPurchaseDate::timestamp, '"', '')) AS most_recent_purchase_date,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:mostRecentPurchaseSku, '"', '')) AS most_recent_purchase_sku,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:name, '"', '')) AS full_name,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:pausedSkus, '"', '')) AS paused_skus,
  --LTRIM(REGEXP_REPLACE(USER_PROPERTIES:payment_method, '"', '')) AS payment_method,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:referrer, '"', '')) AS referrer,
  --LTRIM(REGEXP_REPLACE(USER_PROPERTIES:region, '"', '')) AS region,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:subscriptionCount, '"', '')) AS subscription_count,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:totalOrdersCount, '"', '')) AS total_orders_count,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:username, '"', '')) AS username,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:utm_campaign, '"', '')) AS utm_campaign,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:utm_content, '"', '')) AS utm_content,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:utm_medium, '"', '')) AS utm_medium,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:utm_source, '"', '')) AS utm_source,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:utm_term, '"', '')) AS utm_term,
  LTRIM(REGEXP_REPLACE(USER_PROPERTIES:zip, '"', '')) AS zip,
--- EVENT_PROPERTIES json
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:"$price", '"', '')) AS price,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:"$productId", '"', '')) AS product_id,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:"$quantity", '"', '')) AS quantity,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:"$revenue", '"', '')) AS revenue,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:"$revenueType", '"', '')) AS revenue_type,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:affiliation, '"', '')) AS affiliation,
  --LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:city, '"', '')) AS city,
  --LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:country, '"', '')) AS country,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:coupon, '"', '')) AS coupon,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:currency, '"', '')) AS currency,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:discount, '"', '')) AS discount,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:email, '"', '')) AS email,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:externalId, '"', '')) AS external_id,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:firstName, '"', '')) AS first_name,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:lastName, '"', '')) AS last_name,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:order_id, '"', '')) AS order_id,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:paymentMethod, '"', '')) AS payment_method,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:postalCode, '"', '')) AS postal_code,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:revenue, '"', '')) AS total_revenue,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:shipping, '"', '')) AS shipping,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:state, '"', '')) AS state,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:subtotal, '"', '')) AS subtotal,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:tax, '"', '')) AS tax,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:total, '"', '')) AS total,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:totalDS01Quantity, '"', '')) AS total_ds01_quantity,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:totalOrderQuantity, '"', '')) AS total_order_quantity,
  LTRIM(REGEXP_REPLACE(EVENT_PROPERTIES:totalPDS08Quantity, '"', '')) AS total_pds08_quantity,
--- remaining
 * from MARKETING_DATABASE.AMPLITUDE.EVENT 
)
--- AMPLITUDE data
select
CONCAT(utm_medium,';',utm_source) as utm_source_medium,
case
    ----- Direct
    when lower(utm_medium) is null then 'Direct'
    ----- Public Radio
    when lower(utm_medium) ilike '%publicradio%' then 'Public Radio'
    ----- Email
    when lower(utm_source) ilike '%klaviyo%' then 'Email'
    when lower(utm_source) ilike '%iterable%' then 'Email'
    when lower(utm_medium) = 'email' then 'Email'
    ----- Search
    when lower(utm_medium) ilike '%cpc%' then 'Search'
    ----- Organic Social
    when lower(utm_source_medium) ilike '%seedsocial%' then 'Organic Social'
    ----- Performance
    when lower(utm_medium) ilike '%facebook%' or lower(utm_source) ilike '%facebook%'  then 'Performance'
    when lower(utm_medium) ilike '%reddit%' then 'Performance'
    when lower(utm_medium) ilike '%pinterest%' or lower(utm_source) ilike '%pinterest%' then 'Performance'
    when lower(utm_medium) ilike '%snapchat%' then 'Performance'
    when lower(utm_medium) ilike '%tradedesk%' then 'Performance'
    when lower(utm_medium) ilike '%outbrain%' or lower(utm_source) ilike '%outbrain%' then 'Performance'
    when lower(utm_medium) ilike '%tiktok%' then 'Performance'
    when lower(utm_source) ilike '%tapjoy%' then 'Performance'
    when lower(utm_source) ilike '%liveintent%' or lower(utm_medium)ilike '%liveintent%' then 'Performance'
    when lower(utm_medium) = 'social' then 'Performance'
    when lower(utm_source) = 'geistm' then 'Performance'
    ------ Influencer
    when lower(utm_medium) ilike '%instagram%' or lower(utm_medium) ilike '%youtube%' then 'Influencer'
    when lower(utm_medium) = 'app' then 'Influencer'
    when lower(utm_medium) ilike '%influencer%' then 'Influencer'
    when lower(utm_campaign) ilike '%flavcity%' then 'Influencer'
    when lower(utm_medium) = 'social-post' then 'Influencer'
    when lower(referrer) ilike '%instagram%' then 'Influencer'
    ------ Affiliate
    when lower(utm_medium) = 'affiliate' then 'Affiliate'
    ------ Partnerships 
    when lower(utm_medium) = 'partner' then 'Partnerships'
    when lower(utm_medium) = 'article' then 'Partnerships'
    ------ Podcast
    when lower(utm_medium) ilike '%podcast%' then 'Podcast'
    when lower(utm_medium) ilike '%podcst%' then 'Podcast'
    ------ Audio
    when lower(utm_medium) = 'audio' then 'Audio'
    ---- Practitioner
    when lower(utm_medium) = 'practitioner' then 'Practitioner'
    ---- Direct Mail / Insert
    when lower(utm_source_medium) ilike '%insert;mail%' then 'Direct Mail / Insert'
    ----- Banner / Page
    when lower(utm_medium) in ('banner','page') then 'Banner/Page'
    ----- Blog Post
    when lower(utm_medium) ilike '%blog%' then 'Blog Post'
    ----- Newsletter
    when lower(utm_medium) = 'newsletter' then 'Newsletter'
    ----- Referral
    when lower(utm_medium) = 'referral' then 'Referral'
    ----- Other 
    else 'Other' end as channel_grouping,
----- Performance
case
    when lower(utm_medium) ilike '%facebook%' or lower(utm_source) ilike '%facebook%'  then 'Facebook'
    when lower(utm_medium) ilike '%reddit%' then 'Reddit'
    when lower(utm_medium) ilike '%pinterest%' or lower(utm_source) ilike '%pinterest%' then 'Pinterest'
    when lower(utm_medium) ilike '%snapchat%' then 'Snapchat'
    when lower(utm_medium) ilike '%tradedesk%' then 'TradeDesk'
    when lower(utm_medium) ilike '%outbrain%' or lower(utm_source) ilike '%outbrain%' then 'Outbrain'
    when lower(utm_medium) ilike '%tiktok%' then 'TikTok'
    when lower(utm_source) ilike '%tapjoy%' then 'TapJoy'
    when lower(utm_source) ilike '%liveintent%' or lower(utm_medium)ilike '%liveintent%' then 'Liveintent'
    when lower(utm_source) ilike '%geistm%' then 'GeistM'
    when lower(utm_source) ilike '%bing%' then 'Bing'
    when lower(utm_source) ilike '%google%' then 'Google Ads'
    when lower(utm_source) ilike '%youtube%' then 'Youtube'
    when lower(utm_source) ilike '%klaviyo%' then 'Klaviyo'
    when lower(utm_source) ilike '%iterable%' then 'Iterable'
    else 'Other' end as channel_platform,
to_date(event_time) as event_date,
date_trunc('month',to_date(event_time)) as event_monthdate,
*
from amplitude_events
--from MARKETING_DATABASE.AMPLITUDE.EVENT 
where 
event_type = 'Order Completed';