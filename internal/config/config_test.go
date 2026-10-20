// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/thnhdatle/foreman/internal/config"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

// The example file parses and documents every field.
func TestExampleConfigIsCompleteAndValid(t *testing.T) {
	t.Parallel()
	path := "../../deploy/config.example.yaml"
	cfg, err := config.Load(path, env(nil))
	require.NoError(t, err)
	assert.Equal(t, config.Default(), cfg, "the example states the defaults")
	require.NoError(t, cfg.Validate(false))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, yaml.Unmarshal(data, &doc))
	for _, key := range yamlKeys(reflect.TypeOf(config.Config{}), "") {
		assert.True(t, hasKey(doc, key), "config.example.yaml misses %s", key)
	}
}

func yamlKeys(t reflect.Type, prefix string) []string {
	var keys []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := strings.Split(f.Tag.Get("yaml"), ",")[0]
		if tag == "-" || tag == "" {
			continue
		}
		if f.Type.Kind() == reflect.Struct && f.Type != reflect.TypeOf(config.Duration(0)) {
			keys = append(keys, yamlKeys(f.Type, prefix+tag+".")...)
			continue
		}
		keys = append(keys, prefix+tag)
	}
	return keys
}

func hasKey(doc map[string]any, key string) bool {
	parts := strings.Split(key, ".")
	cur := doc
	for i, p := range parts {
		v, ok := cur[p]
		if !ok {
			return false
		}
		if i < len(parts)-1 {
			cur, ok = v.(map[string]any)
			if !ok {
				return false
			}
		}
	}
	return true
}

func TestLoad_Precedence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	file := filepath.Join(dir, "c.yaml")
	require.NoError(t, os.WriteFile(file, []byte("schema: from_file\noutput: json\ntimeout: 2d\nlog:\n  level: info\n"), 0o600))
	cfg, err := config.Load("", env(map[string]string{
		"FOREMAN_CONFIG": file, "FOREMAN_OUTPUT": "text", "FOREMAN_DATABASE_URL": "postgres://u:p@h/db",
		"FOREMAN_LOG_FORMAT": "json",
	}))
	require.NoError(t, err)
	assert.Equal(t, "from_file", cfg.Schema)
	assert.Equal(t, "text", cfg.Output, "env beats file")
	assert.Equal(t, config.Duration(48*time.Hour), cfg.Timeout)
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
	assert.Equal(t, "postgres://u:p@h/db", cfg.DatabaseURL)
	require.NoError(t, cfg.Validate(true))

	_, err = config.Load(filepath.Join(dir, "missing.yaml"), env(nil))
	require.Error(t, err)
	require.NoError(t, os.WriteFile(file, []byte("unknown_key: 1\n"), 0o600))
	_, err = config.Load(file, env(nil))
	require.ErrorContains(t, err, "unknown_key")
	require.NoError(t, os.WriteFile(file, []byte("database_url: postgres://x\n"), 0o600))
	_, err = config.Load(file, env(nil))
	require.Error(t, err, "secrets are not read from the file")
	_, err = config.Load("", env(map[string]string{"FOREMAN_TIMEOUT": "soon"}))
	require.Error(t, err)
	require.NoError(t, os.WriteFile(file, []byte(""), 0o600))
	_, err = config.Load(file, env(nil))
	require.NoError(t, err, "an empty file is fine")
}

func TestValidate_AllErrorsAtOnce(t *testing.T) {
	t.Parallel()
	cfg := config.Config{Output: "xml", Log: config.Log{Level: "loud", Format: "csv"}}
	err := cfg.Validate(true)
	require.ErrorIs(t, err, config.ErrInvalid)
	for _, m := range []string{"database URL is required", "schema", "output", "timeout", "log.level", "log.format"} {
		assert.Contains(t, err.Error(), m)
	}
	bad := config.Default()
	bad.DatabaseURL = "postgres://h/%zz"
	require.ErrorContains(t, bad.Validate(true), "not a URL")
}

func TestResolveSecretsAndRedact(t *testing.T) {
	t.Parallel()
	f := filepath.Join(t.TempDir(), "url")
	require.NoError(t, os.WriteFile(f, []byte("postgres://app:s3cret@db:5432/app?sslmode=disable\n"), 0o600))
	cfg := config.Default()
	cfg.DatabaseURLFile = f
	require.NoError(t, cfg.ResolveSecrets())
	assert.Equal(t, "postgres://app:s3cret@db:5432/app?sslmode=disable", cfg.DatabaseURL)
	out, err := cfg.Redacted()
	require.NoError(t, err)
	assert.NotContains(t, out, "s3cret")
	assert.Contains(t, out, "database_url: postgres://app:redacted@db:5432/app?sslmode=disable")
	assert.Contains(t, out, "timeout: 30s")

	cfg.DatabaseURLFile = filepath.Join(t.TempDir(), "missing")
	require.NoError(t, cfg.ResolveSecrets(), "an explicit URL wins over the file")
	cfg.DatabaseURL = ""
	require.Error(t, cfg.ResolveSecrets())

	assert.Equal(t, "postgres://u@h/db?password=redacted", config.RedactURL("postgres://u@h/db?password=x"))
	assert.Equal(t, "postgres://u@h/db", config.RedactURL("postgres://u@h/db"))
	assert.Empty(t, config.RedactURL(""))
	assert.Equal(t, "redacted", config.RedactURL("postgres://h/%zz"))
}

func TestRedactURLKeywordValue(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"host=db user=app password=s3cret dbname=x": "host=db user=app password=redacted dbname=x",
		"host=db password = s3cret":                 "host=db password = redacted",
		`host=db password='s3 cr\'et' dbname=x`:     "host=db password=redacted dbname=x",
		"PASSWORD=s3cret host=db":                   "PASSWORD=redacted host=db",
		"host=db user=app dbname=x":                 "host=db user=app dbname=x",
		"host=db password=":                         "host=db password=redacted",
	}
	for in, want := range cases {
		got := config.RedactURL(in)
		assert.Equal(t, want, got, in)
		assert.NotContains(t, got, "s3", in)
	}
}

func TestParseDuration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want time.Duration
	}{
		{"30s", 30 * time.Second},
		{"14d", 14 * 24 * time.Hour},
		{"1d12h", 36 * time.Hour},
		{"\t5m\n", 5 * time.Minute},
		{"0d", 0},
	}
	for _, tc := range tests {
		got, err := config.ParseDuration(tc.in)
		require.NoError(t, err, tc.in)
		assert.Equal(t, tc.want, got, tc.in)
	}
	for _, bad := range []string{"", "d", "xd", "-1d", "1dx", "1d-5m", "abc", "107000d", "106751d24h"} {
		_, err := config.ParseDuration(bad)
		require.Error(t, err, bad)
	}
}
