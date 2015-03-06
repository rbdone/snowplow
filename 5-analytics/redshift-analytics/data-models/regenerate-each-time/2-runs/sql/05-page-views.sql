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

DROP TABLE IF EXISTS snowplow_intermediary.page_views_new;
CREATE TABLE snowplow_intermediary.page_views_new 
  DISTKEY (domain_userid) -- Optimized to join on other session_intermediary.page_views_X tables
  SORTKEY (domain_userid, domain_sessionidx) -- Optimized to join on other session_intermediary.page_views_X tables
  AS (
    SELECT
      blended_user_id,
      inferred_user_id,
      domain_userid,
      domain_sessionidx,
      page_urlhost,
      page_urlpath,
      MIN(collector_tstamp) AS first_touch_tstamp,
      MAX(collector_tstamp) AS last_touch_tstamp,
      COUNT(*) AS event_count,
      SUM(CASE WHEN event = 'page_view' THEN 1 ELSE 0 END) AS page_view_count,
      SUM(CASE WHEN event = 'page_ping' THEN 1 ELSE 0 END) AS page_ping_count,
      COUNT(DISTINCT(FLOOR(EXTRACT (EPOCH FROM collector_tstamp)/30)))/2::FLOAT AS time_engaged_with_minutes
    FROM snowplow_intermediary.events_enriched_final
    GROUP BY 1,2,3,4,5,6
  );

DROP TABLE IF EXISTS snowplow_intermediary.page_views_to_load;
CREATE TABLE snowplow_intermediary.page_views_to_load
  DISTKEY (domain_userid)
  SORTKEY (domain_userid, domain_sessionidx)
  AS (
    SELECT
      p.*,
      t.min_tstamp,
      t.max_tstamp
    FROM snowplow_intermediary.page_views_new p 
    LEFT JOIN (
      SELECT
        MIN(last_touch_tstamp) AS min_tstamp,
        MAX(last_touch_tstamp) AS max_tstamp
      FROM snowplow_intermediary.page_views_new
    ) t ON 1
  );

DROP TABLE IF EXISTS snowplow_pivots.page_views
CREATE TABLE snowplow_pivots.page_views
  DISTKEY (domain_userid)
  SORTKEY (domain_userid, domain_sessionidx)
  AS (
SELECT
      blended_user_id,
      inferred_user_id,
      domain_userid,
      domain_sessionidx,
      page_urlhost,
      page_urlpath,
      first_touch_tstamp,
      last_touch_tstamp,
      event_count,
      page_view_count,
      page_ping_count,
      time_engaged_with_minutes,
      min_tstamp AS processing_run_min_collector_tstamp,
      max_tstamp AS processing_run_max_collector_tstamp
    FROM snowplow_intermediary.page_views_to_load