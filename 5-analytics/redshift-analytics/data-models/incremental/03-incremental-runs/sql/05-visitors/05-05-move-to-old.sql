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

-- Events belonging to the same visitor can arrive at different times and could end up in different batches.
-- Rows in the visitors_new table therefore have to be merged with those in the pivot table.

-- First, move the current visitors pivot table to visitors_old (this also serves as a backup).

BEGIN;
  DROP TABLE IF EXISTS snowplow_intermediary.visitors_old;
  CREATE TABLE snowplow_intermediary.visitors_old
    DISTKEY (blended_user_id) -- Optimized to join on other snowplow_intermediary.visitors_X tables
    SORTKEY (blended_user_id) -- Optimized to join on other snowplow_intermediary.visitors_X tables
  AS (
    SELECT
      *
    FROM snowplow_pivots.visitors
  );
  DELETE FROM snowplow_pivots.visitors;
COMMIT;
