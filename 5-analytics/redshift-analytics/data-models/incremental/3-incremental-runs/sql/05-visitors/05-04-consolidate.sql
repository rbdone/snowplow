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

-- The visitors_source table contains one line per individual website visitor (in this batch).
-- The standard model identifies visitors using only a first party cookie.

-- Finally, join the different visitors tables together into a visitors_new table.

DROP TABLE IF EXISTS snowplow_intermediary.visitors_new;
CREATE TABLE snowplow_intermediary.visitors_new
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
