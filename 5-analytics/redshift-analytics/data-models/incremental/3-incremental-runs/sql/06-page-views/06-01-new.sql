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

-- The page_views_new table contains one line per page view (in this batch).

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
    FROM snowplow_landing.events
    GROUP BY 1,2,3,4,5,6
  );
