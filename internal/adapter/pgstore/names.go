// SPDX-License-Identifier: Apache-2.0

package pgstore

import (
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
)

// DefaultSchema is the schema used when none is configured.
const DefaultSchema = "public"

var schemaRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// Names are the schema-dependent identifiers of one Foreman installation.
type Names struct {
	// Schema is the unquoted schema name.
	Schema string
	// Quoted is the schema as a quoted SQL identifier.
	Quoted string
	// InsertChannel receives the queue name when available jobs are inserted.
	InsertChannel string
	// ControlChannel receives cancel, pause and resume commands as JSON.
	ControlChannel string
	// LeaderChannel is notified when a leader resigns.
	LeaderChannel string
	// LockPrefix scopes advisory lock keys to the schema ("" for public).
	LockPrefix string
}

// NewNames validates schema and derives the identifiers. Channels keep the
// documented names (foreman_insert, foreman_control) in the public schema and are
// prefixed with the schema name otherwise, so installations sharing a
// database do not wake each other.
func NewNames(schema string) (Names, error) {
	if schema == "" {
		schema = DefaultSchema
	}
	if !schemaRe.MatchString(schema) {
		return Names{}, fmt.Errorf("pgstore: invalid schema name %q", schema)
	}
	prefix, lockPrefix := "", ""
	if schema != DefaultSchema {
		prefix, lockPrefix = schema+"_", schema+":"
	}
	return Names{
		Schema:         schema,
		Quoted:         pgx.Identifier{schema}.Sanitize(),
		InsertChannel:  prefix + "foreman_insert",
		ControlChannel: prefix + "foreman_control",
		LeaderChannel:  prefix + "foreman_leadership",
		LockPrefix:     lockPrefix,
	}, nil
}
