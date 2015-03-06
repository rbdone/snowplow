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

-- Move the remaining rows to in_progress.

BEGIN;
  DROP TABLE IF EXISTS snowplow_intermediary.page_views_in_progress;
  CREATE TABLE snowplow_intermediary.page_views_in_progress 
    DISTKEY (domain_userid)
    SORTKEY (domain_userid, domain_sessionidx)
    AS (
      SELECT -- All but min and max_tstamp
        blended_user_id,
        inferred_user_id,
        domain_userid,
        domain_sessionidx,
        page_urlhost,
        page_urlpath,
        first_touch_tstamp,
        last_touch_tstamp,
        event_count,
        page_view_count,
        page_ping_count,
        time_engaged_with_minutes
      FROM snowplow_intermediary.page_views_to_load_complete
    );
  DELETE FROM snowplow_intermediary.page_views_to_load_complete;
COMMIT;
