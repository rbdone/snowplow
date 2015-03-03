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

-- The sessions_source table contains one line per session (in this batch) and assigns browser details to each session.
-- The standard model identifies sessions using only first party cookies and session domain indexes.

DROP TABLE IF EXISTS snowplow_intermediary.sessions_technology;
CREATE TABLE snowplow_intermediary.sessions_technology 
  DISTKEY (domain_userid) -- Optimized to join on other session_intermediary.session_X tables
  SORTKEY (domain_userid, domain_sessionidx) -- Optimized to join on other session_intermediary.session_X tables
  AS (
    SELECT
      domain_userid,
      domain_sessionidx,
      br_name,
      br_family,
      br_version,
      br_type,
      br_renderengine,
      br_lang,
      br_features_director,
      br_features_flash,
      br_features_gears,
      br_features_java,
      br_features_pdf,
      br_features_quicktime,
      br_features_realplayer,
      br_features_silverlight,
      br_features_windowsmedia,
      br_cookies,
      os_name,
      os_family,
      os_manufacturer,
      os_timezone,
      dvce_type,
      dvce_ismobile,
      dvce_screenwidth,
      dvce_screenheight
    FROM (
      SELECT
        domain_userid,
        domain_sessionidx,
        br_name,
        br_family,
        br_version,
        br_type,
        br_renderengine,
        br_lang,
        br_features_director,
        br_features_flash,
        br_features_gears,
        br_features_java,
        br_features_pdf,
        br_features_quicktime,
        br_features_realplayer,
        br_features_silverlight,
        br_features_windowsmedia,
        br_cookies,
        os_name,
        os_family,
        os_manufacturer,
        os_timezone,
        dvce_type,
        dvce_ismobile,
        dvce_screenwidth,
        dvce_screenheight,
        RANK() OVER (PARTITION BY domain_userid, domain_sessionidx 
          ORDER BY dvce_tstamp, br_name, br_family, br_version, br_type, br_renderengine, br_lang, br_features_director, br_features_flash, 
          br_features_gears, br_features_java, br_features_pdf, br_features_quicktime, br_features_realplayer, br_features_silverlight,
          br_features_windowsmedia, br_cookies, os_name, os_family, os_manufacturer, os_timezone, dvce_type, dvce_ismobile, dvce_screenwidth,
          dvce_screenheight) AS rank
      FROM snowplow_landing.events
      WHERE etl_tstamp IN (SELECT etl_tstamp FROM snowplow_intermediary.distinct_etl_tstamps) -- Prevent processing data added after this batch started
        AND collector_tstamp > '2000-01-01' -- Make sure collector_tstamp has a reasonable value, can otherwise cause SQL errors
      ) AS a
    WHERE rank = 1
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
  );
