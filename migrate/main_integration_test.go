// SPDX-License-Identifier: Apache-2.0

//go:build integration

package migrate_test

import (
	"os"
	"testing"

	"github.com/thnhdatle/foreman/internal/testutil/pgtest"
)

var db *pgtest.Database

func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &db)) }
