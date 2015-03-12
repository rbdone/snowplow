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

-- The standard model identifies sessions using only first party cookies and session domain indexes.

DROP TABLE IF EXISTS snowplow_pivots.sessions;
CREATE TABLE snowplow_pivots.sessions
  DISTKEY (domain_userid)
  SORTKEY (domain_userid, domain_sessionidx, session_start_tstamp)
  AS (
    SELECT 
      blended_user_id,
      inferred_user_id,
      domain_userid,
      domain_sessionidx,
      etl_tstamp,
      session_start_tstamp,
      session_end_tstamp,
      event_count,
      time_engaged_with_minutes,
      geo_country,
      geo_country_code_2_characters,
      geo_country_code_3_characters,
      geo_region,
      geo_city,
      geo_zipcode,
      geo_latitude,
      geo_longitude,
      landing_page_host,
      landing_page_path,
      exit_page_host,
      exit_page_path,
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
      min_tstamp AS processing_run_min_collector_tstamp, -- Included for debugging
      max_tstamp AS processing_run_max_collector_tstamp  -- Included for debugging
    FROM snowplow_intermediary.sessions_to_load
  );

