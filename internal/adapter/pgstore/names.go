// SPDX-License-Identifier: Apache-2.0

package pgstore

import (
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
)

const DefaultSchema = "public"

var schemaRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

type Names struct {
	Schema         string
	Quoted         string
	InsertChannel  string
	ControlChannel string
	LeaderChannel  string
	LockPrefix     string
}

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
