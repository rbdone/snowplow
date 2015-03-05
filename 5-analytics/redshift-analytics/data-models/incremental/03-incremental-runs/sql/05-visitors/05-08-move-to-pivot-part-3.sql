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

-- There is no in_progress table for visitors, because a visitor can always come back.
-- The visitors_new table will therefore have to be merged into the visitors pivot table.

-- First, combinate entires for visitors that occur in both tables.

INSERT INTO snowplow_pivots.visitors (
  SELECT 
    n.blended_user_id,
    o.first_touch_tstamp,
    n.last_touch_tstamp,
    o.event_count + n.event_count AS event_count,
    o.session_count + n.session_count AS session_count,
    o.page_view_count + n.page_view_count AS page_view_count,
    o.time_engaged_with_minutes + n.time_engaged_with_minutes AS time_engaged_with_minutes,
    o.landing_page_host,
    o.landing_page_path,
    o.mkt_source,
    o.mkt_medium,
    o.mkt_term,
    o.mkt_content,
    o.mkt_campaign,
    o.refr_source,
    o.refr_medium,
    o.refr_term,
    o.refr_urlhost,
    o.refr_urlpath
  FROM snowplow_intermediary.visitors_new n
  JOIN snowplow_intermediary.visitors_old o ON n.blended_user_id = o.blended_user_id -- INNER JOIN so that we get visitors that have entries in both tables
);
