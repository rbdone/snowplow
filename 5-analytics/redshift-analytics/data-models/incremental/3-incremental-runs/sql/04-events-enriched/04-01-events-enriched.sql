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

-- Enrich events with user_id, easy to extend to include unstructured events as well

DROP TABLE IF EXISTS snowplow_intermediary.events_enriched;
CREATE TABLE snowplow_intermediary.events_enriched
  DISTKEY (blended_user_id) -- this is domain_userid when user_id is NULL
  SORTKEY (blended_user_id, collector_tstamp)
  AS (
    SELECT
      COALESCE(u.user_id, e.domain_userid) AS blended_user_id,
      u.user_id AS inferred_user_id,
      e.*
    FROM snowplow_landing.events e
    LEFT JOIN snowplow_intermediary.cookie_id_to_user_id_map u ON u.domain_userid = e.domain_userid
    WHERE domain_userid IS NOT NULL -- Do not aggregate NULL
      AND domain_sessionidx IS NOT NULL -- Do not aggregate NULL
      AND etl_tstamp IN (SELECT etl_tstamp FROM snowplow_intermediary.distinct_etl_tstamps) -- Prevent processing data added after this batch started
      AND collector_tstamp > '2000-01-01' -- Make sure collector_tstamp has a reasonable value, can otherwise cause SQL errors
  );
