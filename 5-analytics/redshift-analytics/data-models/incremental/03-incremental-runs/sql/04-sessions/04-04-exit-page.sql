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

-- The sessions_exit_page table contains one line per session (in this batch) and assigns an exit page to each session.
-- The standard model identifies sessions using only first party cookies and session domain indexes.

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
