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

-- The page_views_to_load table has one line per page view (in the current batch). It combines page views from page_views_new
-- with those in page_views_in_progress. The latter contains page views that have previously been recorded but might not
-- have completed, i.e. the last event was recorded within 60 minutes of the previous data data run occurring.

-- Second, add any data from the snowplow_intermediary.page_views_in_progress that
-- does NOT have a corresponding entry in the snowplow_intermediary.page_views_new.

INSERT INTO snowplow_intermediary.page_views_to_load
(
  SELECT
    o.*
  FROM snowplow_intermediary.page_views_in_progress o
  LEFT JOIN snowplow_intermediary.page_views_new n
  ON n.domain_userid = o.domain_userid
    AND n.domain_sessionidx = o.domain_sessionidx
    AND n.page_urlhost = o.page_urlhost
    AND n.page_urlpath = o.page_urlpath
  WHERE n.domain_userid IS NULL -- The WHERE condition ensures only rows with no match are copied
    AND n.domain_sessionidx IS NULL
    AND n.page_urlhost IS NULL
    AND n.page_urlpath IS NULL
);
