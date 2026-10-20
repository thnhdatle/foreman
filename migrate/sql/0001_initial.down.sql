-- SPDX-License-Identifier: Apache-2.0
-- Reverts 0001_initial: removes every Foreman object from {{schema}}.

DROP FUNCTION IF EXISTS {{schema}}.foreman_insert_job(text, jsonb, jsonb);
DROP TABLE IF EXISTS {{schema}}.foreman_periodic_ticks;
DROP TABLE IF EXISTS {{schema}}.foreman_queues;
DROP TABLE IF EXISTS {{schema}}.foreman_leader;
DROP TABLE IF EXISTS {{schema}}.foreman_jobs;
DROP FUNCTION IF EXISTS {{schema}}.foreman_notify_insert();
DROP FUNCTION IF EXISTS {{schema}}.foreman_canonical_json(jsonb);
DROP FUNCTION IF EXISTS {{schema}}.foreman_state_in_mask(bit, {{schema}}.foreman_state);
DROP TYPE IF EXISTS {{schema}}.foreman_state;
