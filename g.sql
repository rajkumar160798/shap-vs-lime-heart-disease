-- Mapping table to connect visit_hx individuals to subscriber_id
WITH identifier_to_subscriber AS (
  SELECT DISTINCT 
    indiv_anlytcs_id AS individual_analytics_identifier, 
    indiv_anlytcs_sbscrbr_id AS subscriber_id
  FROM `anbc-hcb-prod.ah_reports_hcb_prod.call_hx`
  WHERE indiv_anlytcs_id IS NOT NULL AND indiv_anlytcs_sbscrbr_id IS NOT NULL
),

-- Join visits and calls for the same subscriber within 48 hours
calls_after_visit AS (
  SELECT
    c.unique_id,
    c.event_time,
    v.visit_start_date_time,
    TIMESTAMP_DIFF(c.event_time, PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', v.visit_start_date_time), HOUR) AS hours_diff,
    c.indiv_anlytcs_sbscrbr_id AS subscriber_id,
    v.session_id,
    v.user_platform,
    v.visit_date,
    m.lob_cd,
    m.test_mbr_ind,
    m.psuid
  FROM `anbc-hcb-prod.ah_reports_hcb_prod.call_hx` c
  JOIN identifier_to_subscriber map 
    ON c.indiv_anlytcs_sbscrbr_id = map.subscriber_id
  JOIN `anbc-hcb-prod.ah_reports_hcb_prod.visit_hx` v 
    ON map.individual_analytics_identifier = v.individual_analytics_identifier
  LEFT JOIN `anbc-hcb-prod.insights_share_hcb_prod.v_enriched_membership` m 
    ON map.subscriber_id = m.subscr_id
  WHERE
    v.login_ind = 1
    AND c.event_time >= PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', v.visit_start_date_time)
    AND TIMESTAMP_DIFF(c.event_time, PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', v.visit_start_date_time), HOUR) <= 48
),

-- Add A1A and ISM controls
enriched_calls AS (
  SELECT
    cav.*,
    a1a.control AS a1a_control,
    ism.control AS ism_control,
    TRUE AS call_after_visit_ind,
    CASE
      WHEN a1a.control = 1 OR ism.control = 1 THEN TRUE
      ELSE FALSE
    END AS ism_a1a_active
  FROM calls_after_visit cav
  LEFT JOIN `anbc-hcb-prod.insights_share_hcb_prod.a1a_controls` a1a 
    ON CAST(a1a.psuid AS STRING) = CAST(cav.psuid AS STRING)
  LEFT JOIN `anbc-hcb-prod.insights_share_hcb_prod.ism_controls` ism 
    ON CAST(ism.psuid AS STRING) = CAST(cav.psuid AS STRING)
)

-- Final reporting aggregation
SELECT
  FORMAT_DATE('%Y-%m', DATE(visit_date)) AS month,
  lob_cd AS segment,
  user_platform,
  COUNT(DISTINCT session_id) AS num_visits,
  COUNT(DISTINCT unique_id) AS num_calls_within_48h,
  COUNT(DISTINCT unique_id) * 1.0 / COUNT(DISTINCT session_id) AS call_after_rate,
  MAX(ism_a1a_active) AS any_ism_a1a_active
FROM enriched_calls
WHERE
  UPPER(TRIM(lob_cd)) IN ('COMMERCIAL', 'MEDICARE', 'IFP')
  AND TRIM(test_mbr_ind) = 'N'
GROUP BY
  month,
  segment,
  user_platform
ORDER BY
  month,
  segment,
  user_platform;
