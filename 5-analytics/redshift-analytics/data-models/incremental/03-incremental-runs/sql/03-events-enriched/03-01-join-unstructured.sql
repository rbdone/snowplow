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

-- Enrich events with unstructured events and inferred_user_id. First, join with unstructured events and
-- save the table with a different distkey to make successive joins faster.

-- This version does NOT join any unstructured events (a placeholder), but the WHERE clause does remove
-- events that should not be aggregated.

DROP TABLE IF EXISTS snowplow_intermediary.events_enriched;
CREATE TABLE snowplow_intermediary.events_enriched
  DISTKEY (domain_userid) -- Optimized to join cookie_id_to_user_id_map
  SORTKEY (domain_userid, domain_sessionidx, dvce_tstamp) -- Optimized to join cookie_id_to_user_id_map
AS (
  SELECT
    e.*
  FROM
    snowplow_landing.events e
  WHERE e.domain_userid IS NOT NULL -- Do not aggregate NULL
    AND e.domain_sessionidx IS NOT NULL -- Do not aggregate NULL
    AND e.domain_userid <> '' -- Do not aggregate missing domain_userids
    AND e.etl_tstamp IN (SELECT etl_tstamp FROM snowplow_intermediary.distinct_etl_tstamps) -- Prevent processing data added after this batch started
    AND e.collector_tstamp > '2000-01-01' -- Make sure collector_tstamp has a reasonable value, can otherwise cause SQL errors
);
