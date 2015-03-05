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

-- Second, add data from the new table, being careful to exclude any results that have corresponding entries in the old table.

INSERT INTO snowplow_intermediary.sessions_to_load (
  SELECT
    n.blended_user_id,
    n.inferred_user_id,
    n.domain_userid,
    n.domain_sessionidx,
    n.etl_tstamp,
    n.session_start_tstamp,
    n.session_end_tstamp,
    n.event_count,
    n.time_engaged_with_minutes,
    n.geo_country,
    n.geo_country_code_2_characters,
    n.geo_country_code_3_characters,
    n.geo_region,
    n.geo_city,
    n.geo_zipcode,
    n.geo_latitude,
    n.geo_longitude,
    n.landing_page_host,
    n.landing_page_path,
    n.exit_page_host,
    n.exit_page_path,
    n.mkt_source,
    n.mkt_medium,
    n.mkt_term,
    n.mkt_content,
    n.mkt_campaign,
    n.refr_source,
    n.refr_medium,
    n.refr_term,
    n.refr_urlhost,
    n.refr_urlpath,
    n.br_name,
    n.br_family,
    n.br_version,
    n.br_type,
    n.br_renderengine,
    n.br_lang,
    n.br_features_director,
    n.br_features_flash,
    n.br_features_gears,
    n.br_features_java,
    n.br_features_pdf,
    n.br_features_quicktime,
    n.br_features_realplayer,
    n.br_features_silverlight,
    n.br_features_windowsmedia,
    n.br_cookies,
    n.os_name,
    n.os_family,
    n.os_manufacturer,
    n.os_timezone,
    n.dvce_type,
    n.dvce_ismobile,
    n.dvce_screenwidth,
    n.dvce_screenheight
  FROM snowplow_intermediary.sessions_new n
  LEFT JOIN snowplow_intermediary.sessions_in_progress o
  ON o.domain_userid = n.domain_userid AND o.domain_sessionidx = n.domain_sessionidx
  WHERE o.domain_userid IS NULL AND o.domain_sessionidx IS NULL -- Restrict to new sessions that have no corresponding entry in the in_progress table 
);
