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

-- Events belonging to the same visitor can arrive at different times and could end up in different batches.
-- Rows in the visitors_new table therefore have to be merged with those in the pivot table.

-- Move the consolidated visitors to the pivot table.

INSERT INTO snowplow_pivots.visitors (
  SELECT
    b.blended_user_id,
    b.first_touch_tstamp,
    b.last_touch_tstamp,
    b.dvce_min_tstamp,
    b.dvce_max_tstamp,
    b.max_etl_tstamp,
    b.event_count,
    b.session_count,
    b.page_view_count,
    b.time_engaged_with_minutes,
    f.landing_page_host,
    f.landing_page_path,
    f.mkt_source,
    f.mkt_medium,
    f.mkt_term,
    f.mkt_content,
    f.mkt_campaign,
    f.refr_source,
    f.refr_medium,
    f.refr_term,
    f.refr_urlhost,
    f.refr_urlpath
  FROM      snowplow_intermediary.sessions_to_load_basic AS b
  LEFT JOIN snowplow_intermediary.sessions_to_load_first AS f ON blended_user_id = f.blended_user_id
);
