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

-- The visitors_basic table contains one line per individual website visitor (in this batch).
-- The standard model identifies visitors using only a first party cookie.

-- First, create a basic table with simple information per visitor that can be derived from a single table scan.

DROP TABLE IF EXISTS snowplow_intermediary.visitors_basic;
CREATE TABLE snowplow_intermediary.visitors_basic
  DISTKEY (domain_userid) -- Optimized to join on other session_intermediary.visitors_X tables
  SORTKEY (domain_userid) -- Optimized to join on other session_intermediary.visitors_X tables
  AS (
    SELECT
		  domain_userid,
		  MIN(collector_tstamp) AS first_touch_tstamp,
		  MAX(collector_tstamp) AS last_touch_tstamp,
		  COUNT(*) AS event_count,
      MAX(domain_sessionidx) AS session_count,
		  SUM(CASE WHEN event = 'page_view' THEN 1 ELSE 0 END) AS page_view_count,
		  COUNT(DISTINCT(FLOOR(EXTRACT (EPOCH FROM collector_tstamp)/30)))/2::FLOAT AS time_engaged_with_minutes
    FROM snowplow_landing.events
    WHERE domain_userid IS NOT NULL -- Do not aggregate NULL
      AND etl_tstamp IN (SELECT etl_tstamp FROM snowplow_intermediary.distinct_etl_tstamps) -- Prevent processing data added after this batch started
      AND collector_tstamp > '2000-01-01' -- Make sure collector_tstamp has a reasonable value, can otherwise cause SQL errors
    GROUP BY 1
  );
