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

-- The sessions_geo table has one line per session (in this batch) and assigns a geography to each session.
-- The standard model identifies sessions using only first party cookies and session domain indexes.

DROP TABLE IF EXISTS snowplow_intermediary.sessions_geo;
CREATE TABLE snowplow_intermediary.sessions_geo 
  DISTKEY (domain_userid) -- Optimized to join on other session_intermediary.session_X tables
  SORTKEY (domain_userid, domain_sessionidx) -- Optimized to join on other session_intermediary.session_X tables
  AS (
    SELECT -- 3. Join with reference_data.country_codes
      v.domain_userid,
      v.domain_sessionidx,
      g.name AS geo_country,
      v.geo_country AS geo_country_code_2_characters,
      g.three_letter_iso_code AS geo_country_code_3_characters,
      v.geo_region,
      v.geo_city,
      v.geo_zipcode,
      v.geo_latitude,
      v.geo_longitude
    FROM (
      SELECT -- 2. Dedupe records (just in case there are two events with the same dvce_tstamp for a particular session)
        domain_userid,
        domain_sessionidx,
        geo_country, 
        geo_region,
        geo_city,
        geo_zipcode,
        geo_latitude,
        geo_longitude
      FROM (
        SELECT -- 1. Take first value for geography from each session
          domain_userid,
          domain_sessionidx,
          FIRST_VALUE(geo_country) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_country,
          FIRST_VALUE(geo_region) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_region,
          FIRST_VALUE(geo_city) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_city,
          FIRST_VALUE(geo_zipcode) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_zipcode,
          FIRST_VALUE(geo_latitude) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_latitude,
          FIRST_VALUE(geo_longitude) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_longitude
        FROM snowplow_landing.events
        WHERE etl_tstamp IN (SELECT etl_tstamp FROM snowplow_intermediary.distinct_etl_tstamps) -- Prevent processing data added after this batch started
          AND collector_tstamp > '2000-01-01' -- Make sure collector_tstamp has a reasonable value, can otherwise cause SQL errors
      ) AS a
      GROUP BY 1,2,3,4,5,6,7,8
    ) AS v
    LEFT JOIN reference_data.country_codes AS g
    ON v.geo_country = g.two_letter_iso_code
  );
