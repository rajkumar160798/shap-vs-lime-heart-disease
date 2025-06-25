-- Step 1: Build map between visit_hx individual_analytics_identifier and call_hx subscriber ID
WITH identifier_to_subscriber AS (
  SELECT DISTINCT 
    indiv_anlytcs_id AS individual_analytics_identifier,
    indiv_anlytcs_sbscrbr_id
  FROM `anbc-hcb-prod.ah_reports_hcb_prod.call_hx`
  WHERE indiv_anlytcs_id IS NOT NULL AND indiv_anlytcs_sbscrbr_id IS NOT NULL
),

-- Step 2: Join visits and calls on subscriber ID, check 48-hour window
calls_after_visit AS (
  SELECT
    c.*,
    v.visit_start_date_time,
    TIMESTAMP_DIFF(c.event_time, PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', v.visit_start_date_time), HOUR) AS hours_diff,
    v.session_id,
    v.user_platform,
    v.visit_date,
    m.lob_cd,
    m.test_mbr_ind,
    m.psuid,
    TRUE AS call_after_visit_ind
  FROM `anbc-hcb-prod.ah_reports_hcb_prod.call_hx` c
  JOIN identifier_to_subscriber map 
    ON c.indiv_anlytcs_sbscrbr_id = map.indiv_anlytcs_sbscrbr_id
  JOIN `anbc-hcb-prod.ah_reports_hcb_prod.visit_hx` v 
    ON map.individual_analytics_identifier = v.individual_analytics_identifier
  LEFT JOIN `anbc-hcb-prod.insights_share_hcb_prod.v_enriched_membership` m 
    ON m.indiv_anlytcs_sbscrbr_id = map.indiv_anlytcs_sbscrbr_id
  WHERE
    v.login_ind = 1
    AND c.event_time >= PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', v.visit_start_date_time)
    AND TIMESTAMP_DIFF(c.event_time, PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', v.visit_start_date_time), HOUR) <= 48
),

-- Step 3: Add A1A and ISM control flags
enriched_call_hx AS (
  SELECT
    cav.*,
    a1a.control AS a1a_control,
    ism.control AS ism_control,
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

-- Step 4: Final output
SELECT
  unique_id,
  indiv_anlytcs_id,
  indiv_anlytcs_sbscrbr_id,
  event_time,
  visit_start_date_time,
  hours_diff,
  session_id,
  user_platform,
  visit_date,
  lob_cd,
  test_mbr_ind,
  psuid,
  call_after_visit_ind,
  ism_a1a_active
FROM enriched_call_hx;

-- This query retrieves calls made within 48 hours after a visit, enriched with A1A and ISM control flags.
-- It joins visit and call data based on individual analytics identifiers and subscriber IDs,
-- ensuring that the calls are within the specified time window.
-- The final output includes relevant fields along with the control flags indicating A1A and ISM activity.
-- The query is designed to be efficient and leverages appropriate joins and filters to minimize data

i created  the call-after-visit process from the HX tables. So if you see, I joined this login, and I got results like this thing, 22 records, 23 records.

mark:  What you're doing with login, we have a login field in the visit HX table, so we can just use that. We don't need that extra join. We'll need the WHERE clause WHERE to get it to add it. I don't think we need to use Adobe ngX anywhere, given that we have visit HX. Hopefully, yeah. That was part of the goal of creating the visit HX table, is that we won't need to use the Adobe table. So we should be able to get away with just using the visit HX table and the call HX table. If there is something missing, then it might be better to just change it and to fix the visit HX. Since we really haven't published that, there's no one depending on that data yet. With the exception of the daily call-after-visit, which is not yet live, but it's really close to production. They're going to start running production runs on that sometime this week. Sometime this week, they said, but it might not be until next week. So you'll see there's a login indicator. So just check where that equals one. Okay. Okay. And also, like, so like I use also like VNH membership for joining on like individual analytics identifier. And also like calculated this call-after-visit as like... No, you can't join on individual analytics identifier. You got to join on... Oh, wait. Yeah, so you join to enrich membership on individual... So I think you should make the individual analytics identifier to the subscriber ID table. Make that as a separate table, I think. Okay. You might be able to get away with it without doing it here, but I'd have to look at it. That's my initial gut reaction. But the call-to-visit should not be on the individual analytics identifier, but it should be on the subscriber. Okay. Right. And you still have to do that whole thing within 48 hours, right? Okay. Where's the check for 48 hours? No, Mark, I didn't add that check for 48 hours. Okay. Okay. Good. Yeah. And we'll go from there. I will say that this does... The way Richard developed the call-after-visit is different than what I wanted it to be. Okay. But that's not that big of a deal. You know, I wanted it to be take all the events, calls, and visits, put them in order in a table, and then note what the next one is and how far away it is. Oh. Right? So you could say, yeah, this member visited here and then visited again and then visited again and then called. Right? Okay. And if they call again, even if it's like two hours later, that counts as a call-after-call, not a call-after-visit. Okay. But let's not worry about that. But here I think it would be a little bit easier. The way I wanted to do it, the way he did it, it's easier where he just says, but anyway, cool. I like the progress you've made. Let's get rid of that log-in check and just use the log-in field of the Visit HX table and join on. So wait, why are you using the Enriched Membership for at all, just to see that they're a valid member? Yes, ma'am. Okay. But we'll also need to use it to get the subscriber for the Visit table, because the Visit table only has the member ID, not the subscriber ID. Okay. So should he be using the subscriber ID in the SELECT statement to get the call-after-visit table? How do we want the shape of the data? Like by what? I don't think we need a single massive table. Right? That's a good question. How do we create like a massive call-after-visit table where we have a call table and we add as a feature of it, this is a call-after-visit? Because that's really kind of all we need to do, right? We don't need to create the Visit count, which has come from the Visit table itself. It's the call table. We're really just saying for the call, add an indicator if it's call-after-visit. Okay. And if that's the case, we could do that for, that could be a new permanent table, right? That's interesting. So what you could be creating is just a copy of the call table with an additional field that says it's within 48 hours of a visit for any member within the same household as this call was. Okay. Do you understand that? Yes, Mark, I understand that. Okay. Yeah, try that. Try making that. Okay. Because that would ease greatly our ability to run reporting on this. Okay. Sure, Mark, I love that. Yeah. Oh, I'm sorry. You know what you're not doing in here? You're not checking A1A. Okay. We need to check A1A status also. It was A1A on ISM. That would be awesome. We got a permanent table that just kind of had, so it's the same, it's all the same things that's in the call table, plus two additional columns, one for ISM_ala_active.  And another one for this call is within 48 hours of a visit(call_after_visit_ind). Okay. 


mark wanted final table to have additional columns like ism_a1a_active and call_after_visit_ind


Create a permanent call-level table that enriches every call record with visit relationship and status flags, to support reporting and downstream analytics — without relying on Adobe/NGX or unnecessary joins.

The new table will include the following features:

| **Feature**                             | **Explanation**                                                                                                                   |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| **1. One row per call**                 | The base of the final table should be the existing `call_hx` table.                                                               |
| **2. `call_after_visit_ind` column**    | A boolean flag indicating whether this call occurred **within 48 hours** after a visit by **the same subscriber**.                |
| **3. `ism_a1a_active` column**          | A derived flag showing whether **either A1A or ISM control = 1** for that member (via `psuid` match).                             |
| **4. Remove Adobe CTE / NGX join**      | No need to join with Adobe or use `login` CTE — just rely on `visit_hx.login_ind = 1`.                                            |
| **5. Join on subscriber ID**            | All joins between `call_hx` and `visit_hx` should be through **`indiv_anlytcs_sbscrbr_id`**, not just the analytics identifier.   |
| **6. Add 48-hour check**                | Calculate the time difference between `visit_start_date_time` and `call_hx.event_time` — only keep rows with a diff ≤ 48 hours.   |
| **7. Use `v_enriched_membership`**      | To get the `psuid`, `lob_cd`, and member status — via `indiv_anlytcs_sbscrbr_id`.                                                 |
| **8. Output same columns as `call_hx`** | Include all original fields from `call_hx`, with **additional fields** appended (`call_after_visit_ind`, `ism_a1a_active`, etc.). |

---------
| Column Name                | Source                              | Description                                                   |
| -------------------------- | ----------------------------------- | ------------------------------------------------------------- |
| `unique_id`                | `call_hx`                           | Unique call identifier                                        |
| `indiv_anlytcs_id`         | `call_hx`                           | Original analytics ID                                         |
| `indiv_anlytcs_sbscrbr_id` | `call_hx` / `v_enriched_membership` | Subscriber ID used for joining with `visit_hx` and membership |
| `event_time`               | `call_hx`                           | Time of the call                                              |
| `visit_start_date_time`    | `visit_hx`                          | Timestamp of the related visit (if within 48h)                |
| `hours_diff`               | calculated                          | Time difference between visit and call                        |
| `lob_cd`                   | `v_enriched_membership`             | Line of business segment                                      |
| `psuid`                    | `v_enriched_membership`             | Plan sponsor ID                                               |
| `a1a_control`              | `a1a_controls`                      | Flag from A1A lookup                                          |
| `ism_control`              | `ism_controls`                      | Flag from ISM lookup                                          |
| `call_after_visit_ind`     | calculated                          | `TRUE` if the call happened within 48 hours after a visit     |
| `ism_a1a_active`           | calculated                          | `TRUE` if either A1A or ISM control = 1 for the member        |