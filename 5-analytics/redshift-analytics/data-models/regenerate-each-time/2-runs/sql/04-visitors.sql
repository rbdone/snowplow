-- Copyright (c) 2013-2015 Snowplow Analytics Ltd. All rights reserved.
--
-- This program is licensed to you under the Apache License Version 2.0,
-- and you may not use this file except in compliance with the Apache License Version 2.0.
-- You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the Apache License Version 2.0 is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
--
-- Authors: Yali Sassoon, Christophe Bogaert
-- Copyright: Copyright (c) 2013-2015 Snowplow Analytics Ltd
-- License: Apache License Version 2.0

-- Sessions basic table contains a line per individual session
-- The standard model identifies sessions using only first party cookies and session domain indexes

DROP TABLE IF EXISTS snowplow_intermediary.visitors_basic;
CREATE TABLE snowplow_intermediary.visitors_basic
  DISTKEY (blended_user_id) -- Optimized to join on other session_intermediary.visitors_X tables
  SORTKEY (blended_user_id, first_touch_tstamp) -- Optimized to join on other session_intermediary.visitors_X tables
  AS (
    SELECT
      blended_user_id,
      MIN(collector_tstamp) AS first_touch_tstamp,
      MAX(collector_tstamp) AS last_touch_tstamp,
      COUNT(*) AS event_count,
      MAX(domain_sessionidx) AS session_count,
      SUM(CASE WHEN event = 'page_view' THEN 1 ELSE 0 END) AS page_view_count,
      COUNT(DISTINCT(FLOOR(EXTRACT (EPOCH FROM collector_tstamp)/30)))/2::FLOAT AS time_engaged_with_minutes
    FROM snowplow_intermediary.events_enriched_final
    GROUP BY 1
  );

DROP TABLE IF EXISTS snowplow_intermediary.visitors_landing_page;
CREATE TABLE snowplow_intermediary.visitors_landing_page
  DISTKEY (blended_user_id) -- Optimized to join on other session_intermediary.visitors_X tables
  SORTKEY (blended_user_id) -- Optimized to join on other session_intermediary.visitors_X tables
  AS (
    SELECT
      blended_user_id,
      page_urlhost,
      page_urlpath
    FROM (
      SELECT
        blended_user_id,
        FIRST_VALUE(page_urlhost) OVER (PARTITION BY domain_userid ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlhost,
        FIRST_VALUE(page_urlpath) OVER (PARTITION BY domain_userid ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlpath
      FROM snowplow_intermediary.events_enriched_final
    ) AS a
    GROUP BY 1,2,3
  );


DROP TABLE IF EXISTS snowplow_intermediary.visitors_source;
CREATE TABLE snowplow_intermediary.visitors_source
  DISTKEY (blended_user_id) -- Optimized to join on other session_intermediary.visitors_X tables
  SORTKEY (blended_user_id) -- Optimized to join on other session_intermediary.visitors_X tables
  AS (
    SELECT
      *
    FROM (
      SELECT
        blended_user_id,
        mkt_source,
        mkt_medium,
        mkt_term,
        mkt_content,
        mkt_campaign,
        refr_source,
        refr_medium,
        refr_term,
        refr_urlhost,
        refr_urlpath,
        dvce_tstamp, -- Will not be included in the consolidated sessions_new
        RANK() OVER (PARTITION BY blended_user_id
          ORDER BY dvce_tstamp, mkt_source, mkt_medium, mkt_term, mkt_content, mkt_campaign, refr_source, refr_medium, refr_term, refr_urlhost, refr_urlpath) AS rank
      FROM snowplow_intermediary.events_enriched_final
      WHERE refr_medium != 'internal' -- Not an internal referer
        AND (
          NOT(refr_medium IS NULL OR refr_medium = '') OR
          NOT (
            (mkt_campaign IS NULL AND mkt_content IS NULL AND mkt_medium IS NULL AND mkt_source IS NULL AND mkt_term IS NULL) OR
            (mkt_campaign = '' AND mkt_content = '' AND mkt_medium = '' AND mkt_source = '' AND mkt_term = '')
          )
        )
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    ) AS t
    WHERE rank = 1 -- Pull only the first referer for each visit
  );

DROP TABLE IF EXISTS snowplow_pivots.visitors;
CREATE TABLE snowplow_pivots.visitors
  DISTKEY (blended_user_id) -- Optimized to join on other session_intermediary.visitors_X tables
  SORTKEY (blended_user_id, first_touch_tstamp) -- Optimized to join on other session_intermediary.visitors_X tables
  AS (
    SELECT 
      b.blended_user_id,
      b.first_touch_tstamp,
      b.last_touch_tstamp,
      b.event_count,
      b.session_count,
      b.page_view_count,
      b.time_engaged_with_minutes,
      l.page_urlhost AS landing_page_host,
      l.page_urlpath AS landing_page_path,
      s.mkt_source,
      s.mkt_medium,
      s.mkt_term,
      s.mkt_content,
      s.mkt_campaign,
      s.refr_source,
      s.refr_medium,
      s.refr_term,
      s.refr_urlhost,
      s.refr_urlpath
    FROM snowplow_intermediary.visitors_basic b
    LEFT JOIN snowplow_intermediary.visitors_landing_page l ON b.blended_user_id = l.blended_user_id
    LEFT JOIN snowplow_intermediary.visitors_source s       ON b.blended_user_id = s.blended_user_id
  );
