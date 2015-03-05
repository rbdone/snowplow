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

-- The sessions_to_load_complete table contains one line per session (in the current batch).
-- It has 2 extra columns: MIN and MAX(session_end_tstamp), which are used to compute which sessions we can move
-- to snowplow_pivots.sessions and which need to wait for the next run to complete, because they may still be active.

-- The standard model identifies sessions using only first party cookies and session domain indexes.

DROP TABLE IF EXISTS snowplow_intermediary.sessions_to_load_complete;
CREATE TABLE snowplow_intermediary.sessions_to_load_complete
  DISTKEY (domain_userid)
  SORTKEY (domain_userid, domain_sessionidx, session_start_tstamp)
  AS (
    SELECT
      s.*,
      t.min_tstamp,
      t.max_tstamp
    FROM snowplow_intermediary.sessions_to_load s
    JOIN (
      SELECT
        MIN(session_end_tstamp) AS min_tstamp,
        MAX(session_end_tstamp) AS max_tstamp
      FROM snowplow_intermediary.sessions_to_load
    ) t
    ON 1
  );
