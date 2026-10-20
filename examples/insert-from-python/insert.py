# SPDX-License-Identifier: Apache-2.0
"""Enqueue a Foreman job from Python with plain psycopg and foreman_insert_job."""

import json
import os
import sys

import psycopg

url = os.environ.get(
    "FOREMAN_DATABASE_URL", "postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable"
)
schema = sys.argv[1] if len(sys.argv) > 1 else "public"

with psycopg.connect(url) as conn:
    opts = {"queue": "default", "priority": 1, "tags": ["python"], "unique": {"by_args": True}}
    args = {"to": "ada@example.com", "template": "welcome"}
    first = conn.execute(
        f'SELECT "{schema}".foreman_insert_job(%s, %s::jsonb, %s::jsonb)',
        ("email", json.dumps(args), json.dumps(opts)),
    ).fetchone()[0]
    again = conn.execute(
        f'SELECT "{schema}".foreman_insert_job(%s, %s::jsonb, %s::jsonb)',
        ("email", json.dumps(args), json.dumps(opts)),
    ).fetchone()[0]
    print(f"python inserted job {first}; unique duplicate returned job {again}")
