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

-- Add two colums with MIN and MAX(last_touch_tstamp), which are used to compute which rows can be moved to the pivots table.

DROP TABLE IF EXISTS snowplow_intermediary.page_views_to_load_complete;
CREATE TABLE snowplow_intermediary.page_views_to_load_complete
  DISTKEY (domain_userid)
  SORTKEY (domain_userid, domain_sessionidx)
  AS (
    SELECT
      p.*,
      t.min_tstamp,
      t.max_tstamp
    FROM snowplow_intermediary.page_views_to_load p 
    LEFT JOIN (
      SELECT
        MIN(last_touch_tstamp) AS min_tstamp,
        MAX(last_touch_tstamp) AS max_tstamp
      FROM snowplow_intermediary.page_views_to_load 
    ) t ON 1
  );
