-- SPDX-License-Identifier: Apache-2.0
-- Foreman schema v1. {{schema}} is replaced by the quoted schema name and the
-- channel placeholders by the NOTIFY channel names of that schema.

CREATE TYPE {{schema}}.foreman_state AS ENUM (
  'available', 'scheduled', 'running', 'retryable', 'completed', 'cancelled', 'discarded'
);

CREATE TABLE {{schema}}.foreman_jobs (
  id                  bigserial PRIMARY KEY,
  kind                text NOT NULL,
  queue               text NOT NULL DEFAULT 'default',
  args                jsonb NOT NULL DEFAULT '{}',
  state               {{schema}}.foreman_state NOT NULL DEFAULT 'available',
  priority            smallint NOT NULL DEFAULT 2,
  attempt             int NOT NULL DEFAULT 0,
  max_attempts        int NOT NULL DEFAULT 25,
  scheduled_at        timestamptz NOT NULL,
  attempted_at        timestamptz,
  finalized_at        timestamptz,
  created_at          timestamptz NOT NULL,
  heartbeat_at        timestamptz,
  cancel_requested_at timestamptz,
  attempted_by        text[] NOT NULL DEFAULT '{}',
  errors              jsonb[] NOT NULL DEFAULT '{}',
  tags                text[] NOT NULL DEFAULT '{}',
  metadata            jsonb NOT NULL DEFAULT '{}',
  unique_key          bytea,
  unique_states       bit(8),
  concurrency_key     text,
  timeout_ms          int,
  CONSTRAINT foreman_jobs_kind_length CHECK (char_length(kind) BETWEEN 1 AND 255),
  CONSTRAINT foreman_jobs_queue_length CHECK (char_length(queue) BETWEEN 1 AND 128),
  CONSTRAINT foreman_jobs_priority_range CHECK (priority BETWEEN 1 AND 4),
  CONSTRAINT foreman_jobs_max_attempts_positive CHECK (max_attempts > 0),
  CONSTRAINT foreman_jobs_concurrency_key_length CHECK (char_length(concurrency_key) BETWEEN 1 AND 255),
  CONSTRAINT foreman_jobs_finalized_at CHECK (
    (state IN ('completed', 'cancelled', 'discarded')) = (finalized_at IS NOT NULL)
  )
) WITH (
  fillfactor = 90,
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 1000,
  autovacuum_analyze_scale_factor = 0.02,
  autovacuum_vacuum_cost_limit = 2000
);

-- unique_states holds one bit per state in enum order; bit 0 is the leftmost.
CREATE FUNCTION {{schema}}.foreman_state_in_mask(mask bit(8), st {{schema}}.foreman_state)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT get_bit(mask, CASE st
    WHEN 'available' THEN 0 WHEN 'scheduled' THEN 1 WHEN 'running' THEN 2 WHEN 'retryable' THEN 3
    WHEN 'completed' THEN 4 WHEN 'cancelled' THEN 5 WHEN 'discarded' THEN 6 END) = 1
$$;

-- Fetch: next jobs of a queue in priority, scheduled time, id order.
CREATE INDEX foreman_jobs_fetch_idx ON {{schema}}.foreman_jobs (queue, priority, scheduled_at, id)
  WHERE state = 'available';
-- Fairness: loose index scan over keys with available jobs.
CREATE INDEX foreman_jobs_fairness_idx ON {{schema}}.foreman_jobs (queue, concurrency_key, priority, scheduled_at, id)
  WHERE state = 'available';
-- Per-key limits: running jobs per key.
CREATE INDEX foreman_jobs_running_key_idx ON {{schema}}.foreman_jobs (queue, concurrency_key)
  WHERE state = 'running';
-- Scheduler: scheduled and retryable jobs by due time.
CREATE INDEX foreman_jobs_scheduler_idx ON {{schema}}.foreman_jobs (scheduled_at)
  WHERE state IN ('scheduled', 'retryable');
-- Unique jobs: one row per key among the states selected by the job.
CREATE UNIQUE INDEX foreman_jobs_unique_idx ON {{schema}}.foreman_jobs (unique_key)
  WHERE unique_key IS NOT NULL AND {{schema}}.foreman_state_in_mask(unique_states, state);
-- Cleaner: finalized jobs by age.
CREATE INDEX foreman_jobs_cleaner_idx ON {{schema}}.foreman_jobs (finalized_at)
  WHERE state IN ('completed', 'cancelled', 'discarded');
-- Rescuer: running jobs by last heartbeat.
CREATE INDEX foreman_jobs_rescue_idx ON {{schema}}.foreman_jobs (heartbeat_at)
  WHERE state = 'running';

CREATE TABLE {{schema}}.foreman_leader (
  name       text PRIMARY KEY,
  leader_id  text NOT NULL,
  elected_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL
);

CREATE TABLE {{schema}}.foreman_queues (
  name       text PRIMARY KEY,
  paused_at  timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE {{schema}}.foreman_periodic_ticks (
  name       text NOT NULL,
  tick_at    timestamptz NOT NULL,
  job_id     bigint,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (name, tick_at)
);

-- One NOTIFY per queue per statement, for jobs inserted as available.
CREATE FUNCTION {{schema}}.foreman_notify_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify('{{insert_channel}}', q.queue)
  FROM (SELECT DISTINCT queue FROM foreman_new_jobs WHERE state = 'available') q;
  RETURN NULL;
END
$$;

CREATE TRIGGER foreman_jobs_notify_insert
  AFTER INSERT ON {{schema}}.foreman_jobs
  REFERENCING NEW TABLE AS foreman_new_jobs
  FOR EACH STATEMENT EXECUTE FUNCTION {{schema}}.foreman_notify_insert();

-- Canonical text form of a JSON value used for unique keys:
-- no whitespace, object keys sorted by UTF-8 byte order, strings escaped as
-- to_json(text) does, integral numbers as exact integers; any non-integral
-- number raises SQLSTATE 22023.
CREATE FUNCTION {{schema}}.foreman_canonical_json(v jsonb)
RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE
  n numeric;
BEGIN
  CASE jsonb_typeof(v)
  WHEN 'object' THEN
    RETURN '{' || coalesce((
      SELECT string_agg(to_json(e.key)::text || ':' || {{schema}}.foreman_canonical_json(e.value), ','
                        ORDER BY e.key COLLATE "C")
      FROM jsonb_each(v) AS e
    ), '') || '}';
  WHEN 'array' THEN
    RETURN '[' || coalesce((
      SELECT string_agg({{schema}}.foreman_canonical_json(a.value), ',' ORDER BY a.ordinality)
      FROM jsonb_array_elements(v) WITH ORDINALITY AS a
    ), '') || ']';
  WHEN 'string' THEN
    RETURN to_json(v #>> '{}')::text;
  WHEN 'number' THEN
    n := (v #>> '{}')::numeric;
    IF n <> trunc(n) THEN
      RAISE EXCEPTION USING ERRCODE = '22023',
        MESSAGE = 'foreman: unique args not canonical: non-integral number ' || (v #>> '{}');
    END IF;
    RETURN trunc(n)::text;
  ELSE
    RETURN v::text;
  END CASE;
END
$$;

-- Cross-language insert function (see docs/guides/insert-from-other-languages.md).
-- opts keys (all optional): queue,
-- priority, scheduled_at, max_attempts, tags, metadata, concurrency_key,
-- timeout_ms, unique {by_args, by_arg_fields, by_period_seconds, by_queue,
-- by_state}. Returns the id of the inserted job, or of the existing job when
-- a unique job already exists. Uses now() because it has no client clock.
CREATE FUNCTION {{schema}}.foreman_insert_job(kind text, args jsonb, opts jsonb DEFAULT '{}')
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
  v_now         timestamptz := now();
  v_opts        jsonb := coalesce(opts, '{}');
  v_queue       text := coalesce(v_opts ->> 'queue', 'default');
  v_scheduled   timestamptz := coalesce((v_opts ->> 'scheduled_at')::timestamptz, v_now);
  v_unique      jsonb := v_opts -> 'unique';
  v_selected    jsonb;
  v_identity    jsonb;
  v_key         bytea;
  v_mask        bit(8);
  v_states      text[];
  v_period      bigint;
  v_id          bigint;
BEGIN
  IF kind IS NULL OR args IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'foreman: kind and args are required';
  END IF;
  IF v_unique IS NOT NULL AND jsonb_typeof(v_unique) = 'object' THEN
    IF coalesce((v_unique ->> 'by_args')::boolean, false) AND v_unique ? 'by_arg_fields' THEN
      RAISE EXCEPTION USING ERRCODE = '22023',
        MESSAGE = 'foreman: invalid unique options: by_args and by_arg_fields are mutually exclusive';
    END IF;
    IF coalesce((v_unique ->> 'by_args')::boolean, false) THEN
      v_selected := args;
    ELSIF v_unique ? 'by_arg_fields' THEN
      SELECT coalesce(jsonb_object_agg(f, args -> f), '{}') INTO v_selected
      FROM jsonb_array_elements_text(v_unique -> 'by_arg_fields') AS f
      WHERE args ? f;
    ELSE
      v_selected := '{}';
    END IF;
    v_identity := jsonb_build_object('kind', kind, 'args', v_selected);
    IF coalesce((v_unique ->> 'by_queue')::boolean, false) THEN
      v_identity := v_identity || jsonb_build_object('queue', v_queue);
    END IF;
    v_period := (v_unique ->> 'by_period_seconds')::bigint;
    IF v_period IS NOT NULL AND v_period > 0 THEN
      v_identity := v_identity || jsonb_build_object(
        'period', (floor(extract(epoch FROM v_now) / v_period) * v_period)::bigint);
    END IF;
    v_key := sha256(convert_to({{schema}}.foreman_canonical_json(v_identity), 'UTF8'));
    IF v_unique ? 'by_state' THEN
      SELECT array_agg(s) INTO v_states FROM jsonb_array_elements_text(v_unique -> 'by_state') AS s;
    ELSE
      v_states := ARRAY['available', 'scheduled', 'running', 'retryable', 'completed'];
    END IF;
    IF EXISTS (SELECT 1 FROM unnest(v_states) AS s WHERE s NOT IN
        ('available', 'scheduled', 'running', 'retryable', 'completed', 'cancelled', 'discarded')) THEN
      RAISE EXCEPTION USING ERRCODE = '22023',
        MESSAGE = 'foreman: invalid unique options: by_state contains an unknown state';
    END IF;
    IF NOT v_states @> ARRAY['available', 'scheduled', 'running', 'retryable'] THEN
      RAISE EXCEPTION USING ERRCODE = '22023',
        MESSAGE = 'foreman: invalid unique options: by_state must include available, scheduled, running and retryable';
    END IF;
    v_mask := (
      (CASE WHEN 'available' = ANY(v_states) THEN '1' ELSE '0' END) ||
      (CASE WHEN 'scheduled' = ANY(v_states) THEN '1' ELSE '0' END) ||
      (CASE WHEN 'running'   = ANY(v_states) THEN '1' ELSE '0' END) ||
      (CASE WHEN 'retryable' = ANY(v_states) THEN '1' ELSE '0' END) ||
      (CASE WHEN 'completed' = ANY(v_states) THEN '1' ELSE '0' END) ||
      (CASE WHEN 'cancelled' = ANY(v_states) THEN '1' ELSE '0' END) ||
      (CASE WHEN 'discarded' = ANY(v_states) THEN '1' ELSE '0' END) || '0')::bit(8);
  END IF;

  INSERT INTO {{schema}}.foreman_jobs AS j (
    kind, queue, args, state, priority, max_attempts, scheduled_at, created_at,
    tags, metadata, unique_key, unique_states, concurrency_key, timeout_ms
  ) VALUES (
    kind, v_queue, args,
    CASE WHEN v_scheduled > v_now THEN 'scheduled' ELSE 'available' END::{{schema}}.foreman_state,
    coalesce((v_opts ->> 'priority')::smallint, 2),
    coalesce((v_opts ->> 'max_attempts')::int, 25),
    v_scheduled, v_now,
    coalesce(ARRAY(SELECT jsonb_array_elements_text(v_opts -> 'tags')), '{}'),
    coalesce(v_opts -> 'metadata', '{}'),
    v_key, v_mask,
    nullif(v_opts ->> 'concurrency_key', ''),
    (v_opts ->> 'timeout_ms')::int
  )
  ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND {{schema}}.foreman_state_in_mask(unique_states, state)
  DO UPDATE SET kind = EXCLUDED.kind
  RETURNING j.id INTO v_id;
  RETURN v_id;
END
$$;
