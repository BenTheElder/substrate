-- Copyright 2026 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- +goose Up
-- One row per Actor, keyed by Actor UID because an Actor has at most one
-- Worker. Kept separate from workers so Worker reads, writes, and watch events
-- do not grow with occupancy. The primary key finds an Actor's Worker;
-- worker_name lists a Worker's Actors.
CREATE TABLE worker_assignments (
    actor_uid    text PRIMARY KEY,
    worker_name  text NOT NULL
        REFERENCES workers(name) ON DELETE CASCADE,
    proto        bytea NOT NULL
);

CREATE INDEX worker_assignments_worker_idx
    ON worker_assignments (worker_name);
