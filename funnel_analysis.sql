SELECT * FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` LIMIT 1000;

WITH session_base AS (
  SELECT
    user_pseudo_id,
    (SELECT value.int_value 
     FROM UNNEST(event_params)
     WHERE key = 'ga_session_id') AS session_id,
    CONCAT(user_pseudo_id, CAST((SELECT value.int_value 
                                 FROM UNNEST(event_params)
                                 WHERE key = 'ga_session_id') AS STRING)) AS user_session_id,
    
    REGEXP_EXTRACT(
      (SELECT value.string_value
       FROM UNNEST(event_params)
       WHERE key = 'page_location'),
      r'^https?://[^/]+(/[^?]*)'
    ) AS landing_page_location,
    
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name AS campaign,
    
    device.category AS device_category,
    device.language AS device_language,
    device.operating_system AS operating_system,
    
    event_timestamp

  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE event_name = 'session_start'
),

funnel_events AS (
  SELECT
    CONCAT(
      user_pseudo_id,
      CAST((SELECT value.int_value
            FROM UNNEST(event_params)
            WHERE key = 'ga_session_id') AS STRING)
    ) AS user_session_id,
    
    event_name,
    
    TIMESTAMP_MICROS(event_timestamp) AS event_time

  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE event_name IN (
    'session_start',
    'view_item',
    'add_to_cart',
    'begin_checkout',
    'add_shipping_info',
    'add_payment_info',
    'purchase'
  )
)

SELECT
  sb.user_session_id,
  sb.user_pseudo_id,
  sb.session_id,
  sb.landing_page_location,
  sb.source,
  sb.medium,
  sb.campaign,
  sb.device_category,
  sb.device_language,
  sb.operating_system,
  
  fe.event_name,
  TIMESTAMP_MICROS(sb.event_timestamp) AS session_start_time,
  fe.event_time

FROM session_base sb

LEFT JOIN funnel_events fe
  ON sb.user_session_id = fe.user_session_id
