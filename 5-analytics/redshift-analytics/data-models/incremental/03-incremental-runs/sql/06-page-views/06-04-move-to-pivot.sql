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

-- Events belonging to the same page view can arrive at different times and could end up in different batches.
-- Rows in the page_views_new table therefore have to be merged with those in the pivot table.

-- Move the consolidated page views to the pivot table (but first delete its contents).

BEGIN;
  DELETE FROM snowplow_pivots.page_views;

  INSERT INTO snowplow_pivots.page_views (
    SELECT
      blended_user_id,
      inferred_user_id,
      domain_userid,
      domain_sessionidx,
      page_urlhost,
      page_urlpath,
      MIN(first_touch_tstamp) AS first_touch_tstamp,
      MAX(last_touch_tstamp) AS last_touch_tstamp,
      MIN(dvce_tstamp) AS dvce_min_tstamp, -- Used to replace SQL window functions
      MAX(dvce_tstamp) AS dvce_max_tstamp, -- Used to replace SQL window functions
      MAX(max_etl_tstamp) AS max_etl_tstamp, -- Used for debugging
      SUM(event_count) AS event_count,
      SUM(page_view_count) AS page_view_count,
      SUM(page_ping_count) AS page_ping_count,
      SUM(time_engaged_with_minutes) AS time_engaged_with_minutes
    FROM snowplow_intermediary.page_views_new
    GROUP BY 1,2,3,4,5,6
  );
COMMIT;