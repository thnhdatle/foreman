// SPDX-License-Identifier: Apache-2.0

package migrate

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/foreman/internal/adapter/pgstore"
)

//go:embed sql/*.sql
var files embed.FS

// DB is satisfied by *pgxpool.Pool and *pgx.Conn.
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Migration is one embedded schema version.
type Migration struct {
	Version int
	Name    string
	up      string
	down    string
}

type options struct {
	schema string
	target int
}

// Option configures a migration run.
type Option func(*options)

// WithSchema installs Foreman into schema instead of public.
func WithSchema(schema string) Option { return func(o *options) { o.schema = schema } }

// WithTargetVersion stops Down at version v instead of removing everything.
func WithTargetVersion(v int) Option { return func(o *options) { o.target = v } }

// ErrUnknownVersion reports a database migrated by a newer Foreman.
var ErrUnknownVersion = errors.New("migrate: database has a version this binary does not know")

// Migrations returns the embedded migrations in version order.
func Migrations() ([]Migration, error) {
	entries, err := fs.Glob(files, "sql/*.up.sql")
	if err != nil {
		return nil, fmt.Errorf("migrate: list migrations: %w", err)
	}
	out := make([]Migration, 0, len(entries))
	for _, up := range entries {
		base := strings.TrimSuffix(strings.TrimPrefix(up, "sql/"), ".up.sql")
		num, name, ok := strings.Cut(base, "_")
		if !ok {
			return nil, fmt.Errorf("migrate: bad migration file name %q", up)
		}
		v, err := strconv.Atoi(num)
		if err != nil {
			return nil, fmt.Errorf("migrate: bad migration version in %q: %w", up, err)
		}
		upSQL, err := files.ReadFile(up)
		if err != nil {
			return nil, fmt.Errorf("migrate: read %s: %w", up, err)
		}
		downSQL, err := files.ReadFile("sql/" + base + ".down.sql")
		if err != nil {
			return nil, fmt.Errorf("migrate: read down migration of %s: %w", up, err)
		}
		out = append(out, Migration{Version: v, Name: name, up: string(upSQL), down: string(downSQL)})
	}
	slices.SortFunc(out, func(a, b Migration) int { return a.Version - b.Version })
	return out, nil
}

func resolve(opts []Option) (options, pgstore.Names, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	names, err := pgstore.NewNames(o.schema)
	return o, names, err //nolint:wrapcheck // NewNames errors are already prefixed
}

func render(sql string, n pgstore.Names) string {
	return strings.NewReplacer(
		"{{schema}}", n.Quoted,
		"{{insert_channel}}", n.InsertChannel,
		"{{control_channel}}", n.ControlChannel,
	).Replace(sql)
}

// SQL returns the statements Up (up=true) or Down would run on an empty
// (or fully migrated) database, for users who apply migrations with their
// own tools. The migrations table is included.
func SQL(up bool, opts ...Option) (string, error) {
	_, names, err := resolve(opts)
	if err != nil {
		return "", err
	}
	ms, err := Migrations()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	if up {
		b.WriteString(render(bootstrapSQL, names))
		for _, m := range ms {
			fmt.Fprintf(&b, "\n-- migration %04d_%s\n%s", m.Version, m.Name, render(m.up, names))
			fmt.Fprintf(&b, "INSERT INTO %s.foreman_migrations (version, name) VALUES (%d, '%s');\n",
				names.Quoted, m.Version, m.Name)
		}
		return b.String(), nil
	}
	for i := len(ms) - 1; i >= 0; i-- {
		m := ms[i]
		fmt.Fprintf(&b, "\n-- revert %04d_%s\n%s", m.Version, m.Name, render(m.down, names))
	}
	fmt.Fprintf(&b, "DROP TABLE IF EXISTS %s.foreman_migrations;\n", names.Quoted)
	return b.String(), nil
}

const bootstrapSQL = `CREATE SCHEMA IF NOT EXISTS {{schema}};
CREATE TABLE IF NOT EXISTS {{schema}}.foreman_migrations (
  version    int PRIMARY KEY,
  name       text NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
);
`

// Up applies every pending migration, each in its own transaction. Runs
// are serialised across processes by an advisory lock, so concurrent
// clients starting at once are safe; Up on an up-to-date schema is a no-op.
func Up(ctx context.Context, db DB, opts ...Option) error {
	_, names, err := resolve(opts)
	if err != nil {
		return err
	}
	ms, err := Migrations()
	if err != nil {
		return err
	}
	for _, m := range ms {
		err := inTx(ctx, db, names, func(tx pgx.Tx, current int) error {
			if current >= m.Version {
				return nil
			}
			if _, err := tx.Exec(ctx, render(m.up, names)); err != nil {
				return fmt.Errorf("apply %04d_%s: %w", m.Version, m.Name, err)
			}
			_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.foreman_migrations (version, name) VALUES ($1, $2)`,
				names.Quoted), m.Version, m.Name)
			return err //nolint:wrapcheck // wrapped by inTx
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Down reverts migrations newest first down to the target version (0 by
// default, which removes every Foreman object but keeps the schema).
func Down(ctx context.Context, db DB, opts ...Option) error {
	o, names, err := resolve(opts)
	if err != nil {
		return err
	}
	ms, err := Migrations()
	if err != nil {
		return err
	}
	for i := len(ms) - 1; i >= 0; i-- {
		m := ms[i]
		if m.Version <= o.target {
			break
		}
		err := inTx(ctx, db, names, func(tx pgx.Tx, current int) error {
			if current < m.Version {
				return nil
			}
			if _, err := tx.Exec(ctx, render(m.down, names)); err != nil {
				return fmt.Errorf("revert %04d_%s: %w", m.Version, m.Name, err)
			}
			_, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.foreman_migrations WHERE version = $1`, names.Quoted), m.Version)
			return err //nolint:wrapcheck // wrapped by inTx
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Version returns the current schema version (0 when not installed).
func Version(ctx context.Context, db DB, opts ...Option) (int, error) {
	_, names, err := resolve(opts)
	if err != nil {
		return 0, err
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("migrate: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return currentVersion(ctx, tx, names)
}

// Latest returns the newest version embedded in this binary.
func Latest() int {
	ms, err := Migrations()
	if err != nil || len(ms) == 0 {
		return 0
	}
	return ms[len(ms)-1].Version
}

func currentVersion(ctx context.Context, tx pgx.Tx, names pgstore.Names) (int, error) {
	var exists bool
	err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, names.Quoted+".foreman_migrations").Scan(&exists)
	if err != nil {
		return 0, fmt.Errorf("migrate: read version: %w", err)
	}
	if !exists {
		return 0, nil
	}
	var v int
	q := fmt.Sprintf(`SELECT coalesce(max(version), 0) FROM %s.foreman_migrations`, names.Quoted)
	err = tx.QueryRow(ctx, q).Scan(&v)
	if err != nil {
		return 0, fmt.Errorf("migrate: read version: %w", err)
	}
	if v > Latest() {
		return v, fmt.Errorf("%w: %d > %d", ErrUnknownVersion, v, Latest())
	}
	return v, nil
}

func inTx(ctx context.Context, db DB, names pgstore.Names, fn func(tx pgx.Tx, current int) error) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	const lock = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`
	if _, lerr := tx.Exec(ctx, lock, "foreman_migrate:"+names.Schema); lerr != nil {
		return fmt.Errorf("migrate: lock: %w", lerr)
	}
	if _, berr := tx.Exec(ctx, render(bootstrapSQL, names)); berr != nil {
		return fmt.Errorf("migrate: bootstrap: %w", berr)
	}
	current, err := currentVersion(ctx, tx, names)
	if err != nil {
		return err
	}
	if err := fn(tx, current); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migrate: commit: %w", err)
	}
	return nil
}
