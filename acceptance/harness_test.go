// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// criteriaDoc mirrors acceptance/criteria.yaml; thresholds are read at runtime.
type criteriaDoc struct {
	Criteria []struct {
		ID   string                    `yaml:"id"`
		Pass map[string]map[string]any `yaml:"pass"`
	} `yaml:"criteria"`
}

// accEnv returns "mac" or "ci" (set by the runner; CI=1 selects ci).
func accEnv() string {
	if v := os.Getenv("FOREMAN_ACC_ENV"); v != "" {
		return v
	}
	if os.Getenv("CI") != "" {
		return "ci"
	}
	return "mac"
}

func repoDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(file))
}

// thresholds holds the pass map of one criterion for the current environment.
type thresholds map[string]any

func loadThresholds(tb testing.TB, id string) thresholds {
	tb.Helper()
	t := tb
	data, err := os.ReadFile(filepath.Join(repoDir(), "acceptance", "criteria.yaml"))
	require.NoError(t, err)
	var doc criteriaDoc
	require.NoError(t, yaml.Unmarshal(data, &doc))
	for _, c := range doc.Criteria {
		if c.ID == id {
			p, ok := c.Pass[accEnv()]
			require.Truef(t, ok, "%s has no pass.%s thresholds", id, accEnv())
			return p
		}
	}
	t.Fatalf("criterion %s not found", id)
	return nil
}

func (th thresholds) Int(tb testing.TB, key string) int {
	tb.Helper()
	t := tb
	v, ok := th[key]
	require.Truef(t, ok, "threshold %q missing", key)
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	default:
		t.Fatalf("threshold %q is %T, want number", key, v)
		return 0
	}
}

func (th thresholds) Float(tb testing.TB, key string) float64 {
	tb.Helper()
	t := tb
	v, ok := th[key]
	require.Truef(t, ok, "threshold %q missing", key)
	switch n := v.(type) {
	case int:
		return float64(n)
	case float64:
		return n
	default:
		t.Fatalf("threshold %q is %T, want number", key, v)
		return 0
	}
}

func (th thresholds) Bool(tb testing.TB, key string) bool {
	tb.Helper()
	t := tb
	v, ok := th[key].(bool)
	require.Truef(t, ok, "threshold %q missing or not a bool", key)
	return v
}

// result is written to acceptance/logs/<ID>.json for the runner.
type result struct {
	ID       string         `json:"id"`
	Measured map[string]any `json:"measured"`
	Pass     bool           `json:"pass"`
	Details  any            `json:"details,omitempty"`
}

var resultMu sync.Mutex

// writeResult records the measurements of a criterion. It is called exactly
// once per run, before the final assertions, so a failing run still reports.
func writeResult(tb testing.TB, id string, measured map[string]any, pass bool, details any) {
	tb.Helper()
	t := tb
	resultMu.Lock()
	defer resultMu.Unlock()
	dir := os.Getenv("FOREMAN_ACC_LOG_DIR")
	if dir == "" {
		dir = filepath.Join(repoDir(), "acceptance", "logs")
	}
	require.NoError(t, os.MkdirAll(dir, 0o750))
	data, err := json.MarshalIndent(result{ID: id, Measured: measured, Pass: pass, Details: details}, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, id+".json"), data, 0o600))
	t.Logf("%s measured: %s", id, fmt.Sprint(measured))
}
