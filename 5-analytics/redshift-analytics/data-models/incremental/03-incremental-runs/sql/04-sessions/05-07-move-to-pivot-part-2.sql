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

-- We removed the in_progress table for sessions, because we can never guarantee all events related to a particular
-- session have reached the collector (some arrive weeks after the session finished - this is often related to offline
-- mobile usage). The sessions_new table will therefore have to be merged into the sessions pivot table.

-- Next, load all entires from sessions_new that do NOT have corresponding entries in sessions_old.

INSERT INTO snowplow_pivots.sessions (
  SELECT
    n.*
  FROM snowplow_intermediary.sessions_new n
  LEFT JOIN snowplow_intermediary.sessions_old o
  ON o.domain_userid = n.domain_userid AND o.domain_sessionidx = n.domain_sessionidx
  WHERE o.domain_userid IS NULL AND o.domain_sessionidx IS NULL -- Restrict to sessions that have no corresponding entry in the old table 
);