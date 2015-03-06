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

-- The page_views_to_load table has one line per page view (in the current batch). It combines page views from page_views_new
-- with those in page_views_in_progress. The latter contains page views that have previously been recorded but might not
-- have completed, i.e. the last event was recorded within 60 minutes of the previous data data run occurring.

-- Last, merge rows that occur in both tables.

INSERT INTO snowplow_intermediary.page_views_to_load
(
  SELECT
    n.blended_user_id,
    n.inferred_user_id,
    n.domain_userid,
    n.domain_sessionidx,
    n.page_urlhost,
    n.page_urlpath,
    o.first_touch_tstamp AS first_touch_tstamp,
    n.last_touch_tstamp AS last_touch_tstamp,
    n.event_count + o.event_count AS event_count,
    n.page_view_count + o.page_view_count AS page_view_count,
    n.page_ping_count + o.page_ping_count AS page_ping_count,
    n.time_engaged_with_minutes + o.time_engaged_with_minutes AS time_engaged_with_minutes
  FROM snowplow_intermediary.page_views_new n
  JOIN snowplow_intermediary.page_views_in_progress o -- An INNER JOIN this time, use only rows that occur in both tables
  ON n.domain_userid = o.domain_userid
    AND n.domain_sessionidx = o.domain_sessionidx
    AND n.page_urlhost = o.page_urlhost
    AND n.page_urlpath = o.page_urlpath
);
