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

-- Next, create a table with source information per visitor

DROP TABLE IF EXISTS snowplow_intermediary.visitors_source;
CREATE TABLE snowplow_intermediary.visitors_source
  DISTKEY (domain_userid) -- Optimized to join on other session_intermediary.visitors_X tables
  SORTKEY (domain_userid) -- Optimized to join on other session_intermediary.visitors_X tables
  AS (
    SELECT
      *
    FROM (
      SELECT
        domain_userid,
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
        RANK() OVER (PARTITION BY domain_userid
          ORDER BY dvce_tstamp, mkt_source, mkt_medium, mkt_term, mkt_content, mkt_campaign, refr_source, refr_medium, refr_term, refr_urlhost, refr_urlpath) AS rank
      FROM
        snowplow_landing.events
      WHERE domain_userid IS NOT NULL -- Do not aggregate NULL
        AND etl_tstamp IN (SELECT etl_tstamp FROM snowplow_intermediary.distinct_etl_tstamps) -- Prevent processing data added after this batch started
        AND collector_tstamp > '2000-01-01' -- Make sure collector_tstamp has a reasonable value, can otherwise cause SQL errors
        AND refr_medium != 'internal' -- Not an internal referer
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
