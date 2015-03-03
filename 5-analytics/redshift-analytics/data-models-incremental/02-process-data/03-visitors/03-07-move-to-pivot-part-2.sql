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

-- Next, load all entires from visitors_new that do NOT have corresponding entries in visitors_old.

INSERT INTO snowplow_pivots.visitors (
  SELECT
    n.*
  FROM snowplow_intermediary.visitors_new n
  LEFT JOIN snowplow_intermediary.visitors_old o ON n.domain_userid = o.domain_userid
  WHERE o.domain_userid IS NULL -- Only copy over rows for visitors that do not feature in the old dataset
);

