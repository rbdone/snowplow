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

-- There is no in_progress table for visitors, because a visitor can always come back.
-- The visitors_new table will therefore have to be merged into the visitors pivot table.

-- First, move the current visitors table to visitors_old.

BEGIN;
DROP TABLE IF EXISTS snowplow_intermediary.visitors_old;
CREATE TABLE snowplow_intermediary.visitors_old
  DISTKEY (domain_userid) -- Optimized to join on other session_intermediary.visitors_X tables
  SORTKEY (domain_userid, first_touch_tstamp) -- Optimized to join on other session_intermediary.visitors_X tables
  AS (
    SELECT
      *
    FROM snowplow_pivots.visitors 
  );

DELETE FROM snowplow_pivots.visitors;
COMMIT;
