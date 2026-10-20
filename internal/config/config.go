// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// EnvPrefix prefixes every environment variable.
const EnvPrefix = "FOREMAN_"

// ErrInvalid wraps every validation problem; the CLI exits with code 2.
var ErrInvalid = errors.New("config: invalid")

// Config is the configuration of the foreman CLI. The database URL is a
// secret: it comes from FOREMAN_DATABASE_URL, --database-url, or the file
// named by database_url_file, never from the config file itself.
type Config struct {
	// DatabaseURL is the Postgres connection string (flag or env only).
	DatabaseURL string `yaml:"-"`
	// DatabaseURLFile names a file holding the connection string.
	DatabaseURLFile string `yaml:"database_url_file"`
	// Schema holding the Foreman tables.
	Schema string `yaml:"schema"`
	// Output is text or json.
	Output string `yaml:"output"`
	// Timeout bounds each command.
	Timeout Duration `yaml:"timeout"`
	Log     Log      `yaml:"log"`
}

// Log configures CLI logging.
type Log struct {
	// Level is debug, info, warn or error.
	Level string `yaml:"level"`
	// Format is json or text.
	Format string `yaml:"format"`
}

const formatText = "text"

// Default returns the defaults.
func Default() Config {
	return Config{
		Schema: "public", Output: formatText, Timeout: Duration(30 * time.Second),
		Log: Log{Level: "warn", Format: formatText},
	}
}

// Load builds the configuration: defaults, then the YAML file (path from
// --config or FOREMAN_CONFIG), then environment variables. Flags are applied
// by the caller afterwards with Apply. Validation is a separate step.
func Load(path string, getenv func(string) string) (Config, error) {
	cfg := Default()
	if path == "" {
		path = getenv(EnvPrefix + "CONFIG")
	}
	if path != "" {
		data, err := os.ReadFile(path) //nolint:gosec // G304: the operator chooses the config file
		if err != nil {
			return cfg, fmt.Errorf("config: read %s: %w", path, err)
		}
		dec := yaml.NewDecoder(strings.NewReader(string(data)))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
			return cfg, fmt.Errorf("config: parse %s: %w", path, err)
		}
	}
	for env, dst := range map[string]*string{
		"DATABASE_URL":      &cfg.DatabaseURL,
		"DATABASE_URL_FILE": &cfg.DatabaseURLFile,
		"SCHEMA":            &cfg.Schema,
		"OUTPUT":            &cfg.Output,
		"LOG_LEVEL":         &cfg.Log.Level,
		"LOG_FORMAT":        &cfg.Log.Format,
	} {
		if v := getenv(EnvPrefix + env); v != "" {
			*dst = v
		}
	}
	if v := getenv(EnvPrefix + "TIMEOUT"); v != "" {
		d, err := ParseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf("config: %sTIMEOUT: %w", EnvPrefix, err)
		}
		cfg.Timeout = Duration(d)
	}
	return cfg, nil
}

// ResolveSecrets reads DatabaseURLFile when DatabaseURL is empty.
func (c *Config) ResolveSecrets() error {
	if c.DatabaseURL != "" || c.DatabaseURLFile == "" {
		return nil
	}
	data, err := os.ReadFile(c.DatabaseURLFile)
	if err != nil {
		return fmt.Errorf("config: read database_url_file: %w", err)
	}
	c.DatabaseURL = strings.TrimSpace(string(data))
	return nil
}

// Validate reports every problem at once.
func (c Config) Validate(needDatabase bool) error {
	var errs []error
	if needDatabase && c.DatabaseURL == "" {
		errs = append(errs, errors.New(
			"database URL is required (--database-url, FOREMAN_DATABASE_URL or database_url_file)"))
	}
	if c.DatabaseURL != "" {
		if _, err := url.Parse(c.DatabaseURL); err != nil {
			errs = append(errs, fmt.Errorf("database URL is not a URL: %w", err))
		}
	}
	if c.Schema == "" {
		errs = append(errs, errors.New("schema must not be empty"))
	}
	if c.Output != formatText && c.Output != "json" {
		errs = append(errs, fmt.Errorf("output %q must be text or json", c.Output))
	}
	if c.Timeout <= 0 {
		errs = append(errs, errors.New("timeout must be positive"))
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("log.level %q must be debug, info, warn or error", c.Log.Level))
	}
	if c.Log.Format != formatText && c.Log.Format != "json" {
		errs = append(errs, fmt.Errorf("log.format %q must be text or json", c.Log.Format))
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrInvalid, errors.Join(errs...))
}

// Redacted returns the configuration as YAML with the password of the
// database URL replaced by "redacted".
func (c Config) Redacted() (string, error) {
	type printable struct {
		DatabaseURL string `yaml:"database_url,omitempty"`
		Config      `yaml:",inline"`
	}
	data, err := yaml.Marshal(printable{DatabaseURL: RedactURL(c.DatabaseURL), Config: c})
	if err != nil {
		return "", fmt.Errorf("config: print: %w", err)
	}
	return string(data), nil
}

// kvPassword matches the password setting of a keyword/value connection
// string: an unquoted value runs to the next space, a quoted one to the
// closing quote, with backslash escapes.
var kvPassword = regexp.MustCompile(`(?i)(\bpassword\s*=\s*)('(?:[^'\\]|\\.)*'|\S*)`)

// RedactURL hides the password of a connection string, given either as a
// postgres:// URL or in keyword/value form ("host=db password=secret").
func RedactURL(raw string) string {
	if raw == "" {
		return ""
	}
	if !strings.HasPrefix(raw, "postgres://") && !strings.HasPrefix(raw, "postgresql://") {
		return kvPassword.ReplaceAllString(raw, "${1}redacted")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "redacted"
	}
	if _, ok := u.User.Password(); ok {
		u.User = url.UserPassword(u.User.Username(), "redacted")
	}
	q := u.Query()
	if q.Has("password") {
		q.Set("password", "redacted")
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// Duration is a time.Duration written as a Go duration string with an
// optional d (24h) suffix, for example "30s" or "14d".
type Duration time.Duration

// UnmarshalYAML parses a duration string.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	v, err := ParseDuration(n.Value)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// Std returns the time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// MarshalYAML writes the duration string.
func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

// ParseDuration parses Go duration strings plus whole days with a d suffix
// ("14d"), optionally followed by a Go duration ("1d12h").
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	i := strings.IndexByte(s, 'd')
	if i <= 0 {
		d, err := time.ParseDuration(s)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", s, err)
		}
		return d, nil
	}
	const day = 24 * time.Hour
	days, err := strconv.ParseInt(s[:i], 10, 64)
	if err != nil || days < 0 || days > int64(math.MaxInt64/day) {
		return 0, fmt.Errorf("invalid duration %q", s)
	}
	rest := time.Duration(0)
	if tail := s[i+1:]; tail != "" {
		rest, err = time.ParseDuration(tail)
		if err != nil || rest < 0 {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
	}
	total := time.Duration(days) * day
	if rest > math.MaxInt64-total {
		return 0, fmt.Errorf("invalid duration %q: too large", s)
	}
	return total + rest, nil
}
