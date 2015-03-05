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

-- DISTINCT etl_tstamp is used in several queries to exclude rows in snowplow_landing.events that were added
-- after the current processing run started. A list of etl_tstamps is stored in therefore a separate table.

DROP TABLE IF EXISTS snowplow_intermediary.distinct_etl_tstamps;
CREATE TABLE snowplow_intermediary.distinct_etl_tstamps
  DISTKEY (etl_tstamp)
  SORTKEY (etl_tstamp)
  AS (
    SELECT DISTINCT(etl_tstamp)
    FROM snowplow_landing.events
  );
