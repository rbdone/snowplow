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

-- The visitors_landing_page table contains one line per individual website visitor (in this batch).
-- The standard model identifies visitors using only a first party cookie.

-- Next, create a table with landing page per visitor

DROP TABLE IF EXISTS snowplow_intermediary.visitors_landing_page;
CREATE TABLE snowplow_intermediary.visitors_landing_page
  DISTKEY (domain_userid) -- Optimized to join on other session_intermediary.visitors_X tables
  SORTKEY (domain_userid) -- Optimized to join on other session_intermediary.visitors_X tables
  AS (
    SELECT
      domain_userid,
      page_urlhost,
      page_urlpath
    FROM (
      SELECT
        domain_userid,
        FIRST_VALUE(page_urlhost) OVER (PARTITION BY domain_userid ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlhost,
        FIRST_VALUE(page_urlpath) OVER (PARTITION BY domain_userid ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlpath
      FROM snowplow_landing.events
      WHERE domain_userid IS NOT NULL -- Do not aggregate NULL
        AND etl_tstamp IN (SELECT etl_tstamp FROM snowplow_intermediary.distinct_etl_tstamps) -- Prevent processing data added after this batch started
        AND collector_tstamp > '2000-01-01' -- Make sure collector_tstamp has a reasonable value, can otherwise cause SQL errors
    ) AS a
    GROUP BY 1,2,3
  );
