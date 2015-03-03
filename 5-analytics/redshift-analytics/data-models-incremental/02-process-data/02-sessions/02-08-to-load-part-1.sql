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

-- The sessions_to_load table has one line per session (in the current batch). It combines sessions from sessions_new
-- with those in sessions_in_progress. The latter contains sessions that have previously been recorded but might not
-- have completed, i.e. the last event was recorded within 60 minutes of the previous data data run occurring.

-- The standard model identifies sessions using only first party cookies and session domain indexes.

-- First, create the table and load any data in it from the snowplow_intermediary.sessions_in_progress that
-- does NOT have a corresponding entry in the snowplow_intermediary.sessions_new.

DROP TABLE IF EXISTS snowplow_intermediary.sessions_to_load;
CREATE TABLE snowplow_intermediary.sessions_to_load
  DISTKEY (domain_userid)
  SORTKEY (domain_userid, domain_sessionidx, session_start_tstamp)
  AS (
    SELECT
      o.domain_userid,
      o.domain_sessionidx,
      o.etl_tstamp,
      o.session_start_tstamp,
      o.session_end_tstamp,
      o.event_count,
      o.time_engaged_with_minutes,
      o.geo_country,
      o.geo_country_code_2_characters,
      o.geo_country_code_3_characters,
      o.geo_region,
      o.geo_city,
      o.geo_zipcode,
      o.geo_latitude,
      o.geo_longitude,
      o.landing_page_host,
      o.landing_page_path,
      o.exit_page_host,
      o.exit_page_path,
      o.mkt_source,
      o.mkt_medium,
      o.mkt_term,
      o.mkt_content,
      o.mkt_campaign,
      o.refr_source,
      o.refr_medium,
      o.refr_term,
      o.refr_urlhost,
      o.refr_urlpath,
      o.br_name,
      o.br_family,
      o.br_version,
      o.br_type,
      o.br_renderengine,
      o.br_lang,
      o.br_features_director,
      o.br_features_flash,
      o.br_features_gears,
      o.br_features_java,
      o.br_features_pdf,
      o.br_features_quicktime,
      o.br_features_realplayer,
      o.br_features_silverlight,
      o.br_features_windowsmedia,
      o.br_cookies,
      o.os_name,
      o.os_family,
      o.os_manufacturer,
      o.os_timezone,
      o.dvce_type,
      o.dvce_ismobile,
      o.dvce_screenwidth,
      o.dvce_screenheight
    FROM snowplow_intermediary.sessions_in_progress o
    LEFT JOIN snowplow_intermediary.sessions_new n
    ON o.domain_userid = n.domain_userid AND o.domain_sessionidx = n.domain_sessionidx
    WHERE n.domain_userid IS NULL AND n.domain_sessionidx IS NULL -- Restrict to sessions that have no corresponding entry in the new table 
);
