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

-- The standard model identifies sessions using only first party cookies and session domain indexes,
-- but contains placeholders for identity stitching.

-- Events belonging to the same session can arrive at different times and could end up in different batches.
-- Rows in the sessions_new table therefore have to be merged with those in the pivot table.

-- Move the consolidated sessions to the pivot table (but first delete its contents).

BEGIN;
  DELETE FROM snowplow_pivots.sessions;

  INSERT INTO snowplow_pivots.sessions (
    SELECT
      b.blended_user_id,
      b.inferred_user_id,
      b.domain_userid,
      b.domain_sessionidx,
      b.session_start_tstamp,
      b.session_end_tstamp,
      b.dvce_min_tstamp,
      b.dvce_max_tstamp,
      b.max_etl_tstamp,
      b.event_count,
      b.time_engaged_with_minutes,
      f.geo_country,
      f.geo_country_code_2_characters,
      f.geo_country_code_3_characters,
      f.geo_region,
      f.geo_city,
      f.geo_zipcode,
      f.geo_latitude,
      f.geo_longitude,
      f.landing_page_host,
      f.landing_page_path,
      l.exit_page_host,
      l.exit_page_path,
      f.mkt_source,
      f.mkt_medium,
      f.mkt_term,
      f.mkt_content,
      f.mkt_campaign,
      f.refr_source,
      f.refr_medium,
      f.refr_term,
      f.refr_urlhost,
      f.refr_urlpath,
      f.br_name,
      f.br_family,
      f.br_version,
      f.br_type,
      f.br_renderengine,
      f.br_lang,
      f.br_features_director,
      f.br_features_flash,
      f.br_features_gears,
      f.br_features_java,
      f.br_features_pdf,
      f.br_features_quicktime,
      f.br_features_realplayer,
      f.br_features_silverlight,
      f.br_features_windowsmedia,
      f.br_cookies,
      f.os_name,
      f.os_family,
      f.os_manufacturer,
      f.os_timezone,
      f.dvce_type,
      f.dvce_ismobile,
      f.dvce_screenwidth,
      f.dvce_screenheight
    FROM      snowplow_intermediary.sessions_to_load_basic AS b
    LEFT JOIN snowplow_intermediary.sessions_to_load_first AS f ON b.domain_userid = f.domain_userid AND b.domain_sessionidx = f.domain_sessionidx
    LEFT JOIN snowplow_intermediary.sessions_to_load_last  AS l ON b.domain_userid = l.domain_userid AND b.domain_sessionidx = l.domain_sessionidx
  );
COMMIT;