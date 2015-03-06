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

-- VACUUM relevant tables (those that don't get deleted with each batch)
-- Do this at the end is because an error gets thrown when another user does VACUUM FULL

-- Part 2

VACUUM snowplow_pivots.sessions;
VACUUM snowplow_intermediary.sessions_in_progress;

ANALYZE snowplow_pivots.sessions;
ANALYZE snowplow_intermediary.sessions_in_progress;

-- Part 3

VACUUM snowplow_pivots.visitors;

ANALYZE snowplow_pivots.visitors;

-- Part 4

VACUUM snowplow_pivots.page_views;
VACUUM snowplow_intermediary.page_views_in_progress;

ANALYZE snowplow_pivots.page_views;
ANALYZE snowplow_intermediary.page_views_in_progress;

-- Part 5

VACUUM snowplow_pivots.structured_events;

ANALYZE snowplow_pivots.structured_events;

-- Part 6

VACUUM atomic.events;
VACUUM snowplow_landing.events;

ANALYZE atomic.events;
ANALYZE snowplow_landing.events;
