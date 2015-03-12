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

-- The visitors_basic table has one row per visitor (in this batch) and contains basic information that
-- can be derived from a single table scan. The standard model identifies visitors using only a first party cookie,
-- but the blended_user_id can also be used with identity stitching.

-- Events belonging to the same session can arrive at different times and could end up in different batches.
-- Rows in the sessions_new table therefore have to be merged with those in the pivot table.

-- First, move the current sessions pivot table to sessions_old (this also serves as a backup).

BEGIN;
  DROP TABLE IF EXISTS snowplow_intermediary.sessions_old;
  CREATE TABLE snowplow_intermediary.sessions_old
    DISTKEY (domain_userid) -- Optimized to join on other snowplow_intermediary.session_X tables
    SORTKEY (domain_userid, domain_sessionidx) -- Optimized to join on other snowplow_intermediary.session_X tables
  AS (
    SELECT
      *
    FROM snowplow_pivots.sessions
  );
  DELETE FROM snowplow_pivots.sessions;
COMMIT;
