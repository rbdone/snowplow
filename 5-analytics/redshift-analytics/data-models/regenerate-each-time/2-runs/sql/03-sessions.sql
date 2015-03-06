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

-- Sessions basic table contains a line per individual session
-- The standard model identifies sessions using only first party cookies and session domain indexes

DROP TABLE IF EXISTS snowplow_intermediary.sessions_basic;
CREATE TABLE snowplow_intermediary.sessions_basic
  DISTKEY (domain_userid) -- Optimized to join on other session_intermediary.session_X tables
  SORTKEY (domain_userid, domain_sessionidx) -- Optimized to join on other session_intermediary.session_X tables
  AS (
    SELECT
      blended_user_id, -- One row per domain_userid, so no problem with GROUP BY
      inferred_user_id, -- At most one per domain_userid, so no problem with GROUP BY
      domain_userid,
      domain_sessionidx,
      MAX(etl_tstamp) AS etl_tstamp, -- Included for debugging
      MIN(collector_tstamp) AS session_start_tstamp,
      MAX(collector_tstamp) AS session_end_tstamp,
      COUNT(*) AS event_count,
      COUNT(DISTINCT(FLOOR(EXTRACT (EPOCH FROM collector_tstamp)/30)))/2::FLOAT AS time_engaged_with_minutes
    FROM
      snowplow_intermediary.events_enriched_final
    GROUP BY 1,2,3,4
  );



-- Now create a table that assigns a geography to session

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
        FROM snowplow_intermediary.events_enriched_final
      ) AS a
      GROUP BY 1,2,3,4,5,6,7,8
    ) AS v
    LEFT JOIN reference_data.country_codes AS g
    ON v.geo_country = g.two_letter_iso_code
  );


-- Now create a table that assigns a landing page to each session

DROP TABLE IF EXISTS snowplow_intermediary.sessions_landing_page;
CREATE TABLE snowplow_intermediary.sessions_landing_page 
  DISTKEY (domain_userid) -- Optimized to join on other session_intermediary.session_X tables
  SORTKEY (domain_userid, domain_sessionidx) -- Optimized to join on other session_intermediary.session_X tables
  AS (
    SELECT -- 2. Dedupe records (just in case there are two events with the same dvce_tstamp for a particular session)
      domain_userid,
      domain_sessionidx,
      page_urlhost, 
      page_urlpath 
    FROM (
      SELECT -- 1. Take first value for landing page from each session
        domain_userid,
        domain_sessionidx,
        FIRST_VALUE(page_urlhost) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlhost,
        FIRST_VALUE(page_urlpath) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlpath
      FROM snowplow_intermediary.events_enriched_final
    ) AS a
    GROUP BY 1,2,3,4
  );


-- Now create a table that assigns an exist page to each session

DROP TABLE IF EXISTS snowplow_intermediary.sessions_exit_page;
CREATE TABLE snowplow_intermediary.sessions_exit_page 
  DISTKEY (domain_userid) -- Optimized to join on other session_intermediary.session_X tables
  SORTKEY (domain_userid, domain_sessionidx) -- Optimized to join on other session_intermediary.session_X tables
  AS (
    SELECT -- 2. Dedupe records (just in case there are two events with the same dvce_tstamp for a particular session)
      domain_userid,
      domain_sessionidx,
      page_urlhost, 
      page_urlpath 
    FROM (
      SELECT -- 1. Take first value for exit page from each session
        domain_userid,
        domain_sessionidx,
        LAST_VALUE(page_urlhost) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlhost,
        LAST_VALUE(page_urlpath) OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS page_urlpath
      FROM snowplow_intermediary.events_enriched_final
    ) AS a
    GROUP BY 1,2,3,4
  );


-- Now create a table that assigns campaign / referer data to each session

DROP TABLE IF EXISTS snowplow_intermediary.sessions_source;
CREATE TABLE snowplow_intermediary.sessions_source 
  DISTKEY (domain_userid) -- Optimized to join on other session_intermediary.session_X tables
  SORTKEY (domain_userid, domain_sessionidx) -- Optimized to join on other session_intermediary.session_X tables
  AS (
    SELECT
      *
    FROM (
      SELECT
        domain_userid,
        domain_sessionidx,
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
        RANK() OVER (PARTITION BY domain_userid, domain_sessionidx
          ORDER BY dvce_tstamp, mkt_source, mkt_medium, mkt_term, mkt_content, mkt_campaign, refr_source, refr_medium, refr_term, refr_urlhost, refr_urlpath) AS rank
      FROM
        snowplow_intermediary.events_enriched_final
      WHERE refr_medium != 'internal' -- Not an internal referer
        AND (
          NOT(refr_medium IS NULL OR refr_medium = '') OR
          NOT (
            (mkt_campaign IS NULL AND mkt_content IS NULL AND mkt_medium IS NULL AND mkt_source IS NULL AND mkt_term IS NULL) OR
            (mkt_campaign = '' AND mkt_content = '' AND mkt_medium = '' AND mkt_source = '' AND mkt_term = '')
          )
        )
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
    ) AS t
    WHERE rank = 1 -- Pull only the first referer for each visit
  );


-- Now create a table that technology info per session

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
      FROM snowplow_intermediary.events_enriched_final
      ) AS a
    WHERE rank = 1
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
  );

-- Finally consolidate all the individual sessions tables into a single table in the snowplow_pivots schema

DROP TABLE IF EXISTS snowplow_intermediary.sessions_new;
CREATE TABLE snowplow_intermediary.sessions_new
  DISTKEY (domain_userid) -- Optimized to join on other session_intermediary.session_X tables
  SORTKEY (domain_userid, domain_sessionidx, session_start_tstamp) -- Optimized to join on other session_intermediary.session_X tables
  AS (
    SELECT
      b.blended_user_id, -- Equal to domain_userid if there is no identity stitching
      b.inferred_user_id, -- NULL if there is no identity stitching
      b.domain_userid,
      b.domain_sessionidx,
      b.etl_tstamp,
      b.session_start_tstamp,
      b.session_end_tstamp,
      b.event_count,
      b.time_engaged_with_minutes,
      g.geo_country,
      g.geo_country_code_2_characters,
      g.geo_country_code_3_characters,
      g.geo_region,
      g.geo_city,
      g.geo_zipcode,
      g.geo_latitude,
      g.geo_longitude,
      l.page_urlhost AS landing_page_host,
      l.page_urlpath AS landing_page_path,
      e.page_urlhost AS exit_page_host,
      e.page_urlpath AS exit_page_path,
      s.mkt_source,
      s.mkt_medium,
      s.mkt_term,
      s.mkt_content,
      s.mkt_campaign,
      s.refr_source,
      s.refr_medium,
      s.refr_term,
      s.refr_urlhost,
      s.refr_urlpath,
      t.br_name,
      t.br_family,
      t.br_version,
      t.br_type,
      t.br_renderengine,
      t.br_lang,
      t.br_features_director,
      t.br_features_flash,
      t.br_features_gears,
      t.br_features_java,
      t.br_features_pdf,
      t.br_features_quicktime,
      t.br_features_realplayer,
      t.br_features_silverlight,
      t.br_features_windowsmedia,
      t.br_cookies,
      t.os_name,
      t.os_family,
      t.os_manufacturer,
      t.os_timezone,
      t.dvce_type,
      t.dvce_ismobile,
      t.dvce_screenwidth,
      t.dvce_screenheight
    FROM      snowplow_intermediary.sessions_basic           AS b
    LEFT JOIN snowplow_intermediary.sessions_geo             AS g ON b.domain_userid = g.domain_userid AND b.domain_sessionidx = g.domain_sessionidx
    LEFT JOIN snowplow_intermediary.sessions_landing_page    AS l ON b.domain_userid = l.domain_userid AND b.domain_sessionidx = l.domain_sessionidx
    LEFT JOIN snowplow_intermediary.sessions_exit_page       AS e ON b.domain_userid = e.domain_userid AND b.domain_sessionidx = e.domain_sessionidx
    LEFT JOIN snowplow_intermediary.sessions_source          AS s ON b.domain_userid = s.domain_userid AND b.domain_sessionidx = s.domain_sessionidx
    LEFT JOIN snowplow_intermediary.sessions_technology      AS t ON b.domain_userid = t.domain_userid AND b.domain_sessionidx = t.domain_sessionidx
  );

DROP TABLE IF EXISTS snowplow_intermediary.sessions_to_load;
CREATE TABLE snowplow_intermediary.sessions_to_load
  DISTKEY (domain_userid)
  SORTKEY (domain_userid, domain_sessionidx, session_start_tstamp)
  AS (
    SELECT
      s.*,
      t.min_tstamp,
      t.max_tstamp
    FROM snowplow_intermediary.sessions_new s
    JOIN (
      SELECT
        MIN(session_end_tstamp) AS min_tstamp,
        MAX(session_end_tstamp) AS max_tstamp
      FROM snowplow_intermediary.sessions_new
    ) t
    ON 1
  );

DROP TABLE IF EXISTS snowplow_pivots.sessions;
CREATE TABLE snowplow_pivots.sessions
  DISTKEY (domain_userid) -- Optimized to join on other session_intermediary.session_X tables
  SORTKEY (domain_userid, domain_sessionidx, session_start_tstamp) -- Optimized to join on other session_intermediary.session_X tables
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
